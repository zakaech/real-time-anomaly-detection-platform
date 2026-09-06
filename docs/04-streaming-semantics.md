# 04 — Sémantique de streaming

Ce document traite les points d'ingénierie que le projet ne doit pas contourner : temps d'événement,
watermarking, checkpointing, sémantique de livraison, backpressure.

## 1. Temps d'événement contre temps de traitement

Un système de streaming manipule deux temps qu'il ne faut jamais confondre :

- le **temps d'événement** : quand le fait s'est produit dans le monde réel (le capteur a mesuré 82 °C) ;
- le **temps de traitement** : quand notre code s'en occupe.

Ces deux temps divergent en permanence, et pas seulement en cas d'incident : une passerelle qui bufferise
30 secondes de mesures pendant une coupure Wi-Fi, un GC de 2 secondes, un redémarrage de job qui rattrape
10 minutes de retard. Si l'on fenêtre sur le temps de traitement, alors :

- un rattrapage après incident écrase 10 minutes de mesures dans une seule fenêtre, ce qui fabrique des
  agrégats absurdes et donc de fausses anomalies ;
- **rejouer les mêmes données ne redonne pas les mêmes résultats**, ce qui rend le système intestable et non
  auditable.

C'est ce dernier point qui tranche : le fenêtrage se fait **exclusivement sur `event_time`**, ce qui rend le
traitement **déterministe et rejouable**. Le temps de traitement ne sert qu'à mesurer la latence et à décider
quand arrêter d'attendre.

## 2. Fenêtrage et watermark

**Réglages retenus** (valeurs de conception initiales, à réviser après mesure du retard réel — voir §2.4) :

| Paramètre | Valeur | Raison |
|---|---|---|
| Type de fenêtre | glissante | on veut réévaluer souvent sans réduire l'historique observé |
| Durée | 60 s | assez long pour que moyenne, écart-type et pente soient stables à 1 Hz (≈ 60 points) |
| Glissement | 10 s | une réévaluation toutes les 10 s ; borne supérieure de la granularité de détection |
| Watermark | 30 s | doit dépasser le retard usuel sans immobiliser inutilement l'état |

```python
(df
  .withWatermark("event_time", "30 seconds")
  .groupBy(window(col("event_time"), "60 seconds", "10 seconds"), col("machine_id"))
  .agg(...))
```

### 2.1 Ce que fait réellement un watermark

Le watermark est une **promesse révisable** faite par le moteur : « je considère qu'aucun événement antérieur à
`max(event_time observé) − 30 s` n'arrivera plus ». Sur cette base, Spark peut :

- **émettre** les fenêtres dont la fin est passée sous le watermark ;
- **libérer l'état** de ces fenêtres, sans quoi la mémoire croîtrait indéfiniment.

Un watermark n'est donc pas une tolérance au retard : c'est le mécanisme qui rend l'état **borné**. Sans lui,
une agrégation fenêtrée en streaming est une fuite mémoire qui finit par un OOM — le seul débat porte sur la
date de l'incident.

### 2.2 L'arbitrage

| Watermark trop court | Watermark trop long |
|---|---|
| Les événements en retard sont écartés | L'état grossit proportionnellement |
| Fenêtres incomplètes → agrégats biaisés | Latence d'émission accrue en mode `append` |
| **Faux négatifs** : une anomalie manquée | Détection plus tardive |

Il n'existe pas de bonne valeur dans l'absolu. La méthode est : mesurer la distribution réelle de
`processing_time − event_time` sur `telemetry.late` et prendre le quantile 99, puis vérifier que l'état reste
borné. Les 30 s de départ sont une hypothèse de conception, pas un résultat.

### 2.3 Données en retard : ne jamais les perdre en silence

Spark, dans une agrégation fenêtrée, **écarte silencieusement** les événements en retard. Le compteur existe
(`numRowsDroppedByWatermark` dans `StreamingQueryProgress`) mais l'événement lui-même est perdu. C'est
inacceptable : on ne saurait pas si l'on manque 0,1 % ou 40 % des données.

Contrairement à Flink, **Structured Streaming n'a pas de mécanisme de sortie latérale**. On l'émule par un
branchement explicite **avant** l'agrégation :

```python
enriched = parsed.withColumn(
    "lateness_ms",
    (unix_millis(current_timestamp()) - unix_millis(col("event_time")))
)
on_time = enriched.filter(col("lateness_ms") <= WATERMARK_MS)
too_late = enriched.filter(col("lateness_ms") > WATERMARK_MS)   # → telemetry.late
```

`too_late` est publié sur `telemetry.late` avec son retard mesuré. Cela donne trois choses : une visibilité
réelle sur la santé de la collecte, la distribution empirique nécessaire pour régler le watermark, et une file
rejouable en traitement différé si un épisode d'anomalie s'y trouvait.

Précision d'honnêteté technique : ce filtre utilise le temps de traitement courant, donc il approxime le
watermark de Spark plutôt qu'il ne le lit — le watermark réel dépend du `max(event_time)` observé, pas de
l'horloge. Les deux coïncident en régime établi et divergent pendant un rattrapage. C'est une approximation
acceptable pour de l'observabilité ; ce n'en serait pas une pour une décision métier.

### 2.4 Mode de sortie : `update` plutôt que `append`

C'est une décision structurante (D-04).

| | `append` | `update` |
|---|---|---|
| Émission | une fois, quand le watermark dépasse la fin de fenêtre | à chaque micro-batch où la fenêtre change |
| Latence | fenêtre + watermark ≈ 90 s | intervalle de déclenchement ≈ 10 s |
| Résultat | définitif | révisable |
| Risque | détection tardive | alerte émise puis révisée |

Je retiens **`update`**. Le raisonnement : avec `append`, une anomalie détectée à 14:23 n'est signalée qu'à
14:24:30 au mieux — pour un système vendu comme « temps réel », c'est difficile à défendre. Le défaut d'`update`
(ré-émissions successives d'une même fenêtre) serait rédhibitoire s'il produisait des alertes en double ; or
`alert_id` étant déterministe sur `(machine_id, window_start, model)`, une ré-émission **met à jour** la même
ligne au lieu d'en créer une seconde.

Autrement dit : c'est l'idempotence qui rend le mode `update` utilisable. Les deux décisions se tiennent l'une
l'autre.

Garde-fou associé : on n'émet une alerte que si `sample_count >= 30` sur les 60 attendus. Sans cela, une
fenêtre à peine entamée serait scorée sur trois points et générerait du bruit.

## 3. Checkpointing et reprise après panne

### 3.1 Contenu et emplacement

Un répertoire de checkpoint par requête, sur un volume Docker persistant
(`/checkpoints/anomaly-scoring/`), qui contient :

| Sous-répertoire | Rôle |
|---|---|
| `offsets/` | offsets Kafka **planifiés** pour chaque batch, écrits **avant** exécution (write-ahead log) |
| `commits/` | batches **terminés** avec succès |
| `state/` | état des agrégations fenêtrées, versionné par batch |
| `sources/` `metadata/` | identifiant de la requête, métadonnées des sources |

### 3.2 Le déroulé d'une reprise

1. Au démarrage, Spark lit le dernier fichier de `offsets/` et le dernier de `commits/`.
2. Si un batch est **planifié mais non commité**, il est **rejoué intégralement** : mêmes offsets d'entrée,
   même état restauré.
3. Le rejeu produit à nouveau les écritures Kafka du batch interrompu → **doublons en sortie**.

C'est le mécanisme central : le checkpoint garantit qu'**aucune donnée n'est perdue**, au prix de doublons
possibles. Voir §4.

### 3.3 Ce qui invalide un checkpoint

Point crucial en exploitation. **Compatible** (redémarrage sans perte) :

- changer `maxOffsetsPerTrigger`, l'intervalle de déclenchement, les ressources allouées ;
- changer les projections non-état (colonnes ajoutées, renommées en sortie) ;
- **changer l'artefact du modèle** — c'est ce qui rend le déploiement d'un nouveau modèle peu coûteux.

**Incompatible** (checkpoint à supprimer, donc rejeu et doublons) :

- modifier les clés de `groupBy`, la durée de fenêtre ou le glissement ;
- modifier la colonne de watermark ou ajouter/supprimer un opérateur à état ;
- réordonner les opérateurs à état (leur identité est positionnelle) ;
- changer de version majeure de Spark.

Le fait que **changer de modèle ne casse pas le checkpoint** mérite d'être souligné : la logique de scoring est
encapsulée dans une `pandas_udf`, qui est du code utilisateur et non un opérateur à état. On redémarre le job
avec `MODEL_VERSION=1.4.0`, il reprend aux offsets exacts. La contrepartie est qu'une fenêtre à cheval sur le
redémarrage peut être scorée par deux versions différentes — d'où la présence de `model.version` dans chaque
événement scoré.

Deux règles absolues : **un checkpoint par requête** (deux requêtes partageant un checkpoint corrompent leur
état mutuel), et **jamais deux instances du même job sur le même checkpoint** (écritures concurrentes du WAL).

### 3.4 State store

`spark.sql.streaming.stateStore.providerClass = RocksDBStateStoreProvider`.

Le fournisseur par défaut (`HDFSBackedStateStoreProvider`) conserve l'état **dans le tas de la JVM**. À
20 machines × fenêtres glissantes, cela tient ; le problème est que la croissance de l'état devient une
pression GC puis un OOM, sans palier intermédiaire. RocksDB place l'état hors tas, sur disque, avec le cache en
mémoire : la dégradation devient progressive et diagnosticable. Pour un système censé tourner en continu, une
dégradation lisible vaut mieux qu'une falaise.

## 4. Sémantique de livraison : la réponse

**Le système est at-least-once de bout en bout. Il n'est pas exactly-once. Il est effectively-once au niveau
de l'affichage et de la persistance des alertes.**

### 4.1 Où naissent les doublons

| # | Frontière | Cause | Neutralisé par |
|---|---|---|---|
| 1 | simulateur → `telemetry.raw` | crash du producteur après envoi, avant mémorisation | `event_id` (déduplication hors ligne) |
| 2 | Spark → sortie | rejeu d'un batch non commité | `alert_id` déterministe |
| 3 | Spark → `telemetry.scored` / `alerts` | **le sink Kafka de Spark n'est pas transactionnel** | idem |
| 4 | `foreachBatch` | la fonction peut être appelée plus d'une fois pour le même `batchId` | `batchId` + écritures idempotentes |
| 5 | `alerts` → alert-service | offset commité après le commit DB | `ON CONFLICT` sur la clé primaire |
| 6 | rééquilibrage du groupe de consommateurs | messages non acquittés redistribués | idem |

### 4.2 Pourquoi pas exactly-once

Trois obstacles, dont le premier est dirimant :

1. **Le sink Kafka de Structured Streaming n'ouvre pas de transaction Kafka.** Il ne peut donc pas lier
   atomiquement « j'écris la sortie » et « je commite l'offset d'entrée ». Ce n'est pas un réglage manquant,
   c'est une limite du connecteur.
2. Même avec des transactions Kafka, l'écriture dans PostgreSQL et le commit d'offset Kafka sont **deux
   systèmes distincts**. Les rendre atomiques exigerait un commit en deux phases ou un pattern *outbox*.
3. La sortie transite par plusieurs frontières de processus ; la garantie serait à reconstruire à chacune.

**Ce qu'il faudrait pour l'obtenir vraiment** : Flink avec un sink 2PC (`TwoPhaseCommitSinkFunction`), ou
Kafka Streams en mode `exactly_once_v2` si tout restait dans Kafka. Les deux sortent de la stack imposée, et le
second ne résoudrait de toute façon pas l'écriture PostgreSQL.

### 4.3 Ce qu'on fait à la place

On assume l'at-least-once et on **rend les effets idempotents** :

- `alert_id = uuid5(NAMESPACE, f"{machine_id}|{window_start}|{model_name}|{model_version}")` — un rejeu produit
  bit pour bit le même identifiant ;
- la persistance est un upsert atomique :

```sql
INSERT INTO alert (id, machine_id, severity, anomaly_score, ...)
VALUES (:id, :machine_id, :severity, :anomaly_score, ...)
ON CONFLICT (id) DO UPDATE
   SET anomaly_score = EXCLUDED.anomaly_score,
       severity      = EXCLUDED.severity,
       updated_at    = now()
 WHERE alert.status = 'NEW';
```

La clause `WHERE alert.status = 'NEW'` est le détail qui compte : un message rejoué **ne doit pas ressusciter**
une alerte qu'un opérateur a déjà acquittée. Sans elle, un incident Spark ferait réapparaître des alertes
traitées dans la file de l'opérateur — un bug bien plus visible que le doublon qu'on cherchait à éviter.

- l'événement SSE n'est émis que si l'upsert a effectivement inséré une ligne (`RETURNING (xmax = 0)`), donc un
  doublon ne produit pas de notification.

**Formulation défendable en entretien** : « livraison at-least-once, traitement effectively-once par
idempotence au point de convergence ». La différence avec l'exactly-once est réelle et je la situe précisément :
un doublon est bien traité deux fois, mais son second traitement est un no-op observable.

### 4.4 Un cas non couvert, assumé

Si le modèle est redéployé avec une nouvelle version et que les mêmes fenêtres sont rejouées, `alert_id` change (la version fait partie de la clé) et **deux
alertes coexistent** pour la même fenêtre physique. C'est voulu : ce sont deux verdicts distincts. Le dashboard
les regroupe visuellement par `(machine_id, window_start)`.

## 5. Backpressure

### 5.1 Première ligne : Kafka est le tampon

Le producteur n'est jamais ralenti par un consommateur lent. Un pic se traduit par une **croissance du lag**,
c'est-à-dire par de la latence, pas par de la perte. C'est la raison d'être d'un log persistant, et cela borne
le problème : tant que la rétention n'est pas dépassée, un retard est rattrapable.

Le vrai risque n'est donc pas la perte immédiate, c'est le **retard cumulé qui dépasse les 7 jours de rétention
de `telemetry.raw`** — à ce moment, `failOnDataLoss=true` arrête le job plutôt que de sauter des données en
silence.

### 5.2 Dans Spark : limitation de débit, pas backpressure adaptatif

Une précision qui a son importance : **`spark.streaming.backpressure.enabled` ne s'applique pas à Structured
Streaming.** C'est un réglage de l'ancien DStream. Le confondre est une erreur fréquente.

Le mécanisme réel est `maxOffsetsPerTrigger`, une limite fixe du nombre de messages par micro-batch :

```
maxOffsetsPerTrigger = débit_soutenable_mesuré (msg/s) × intervalle_trigger (s) × 0,8
```

Le facteur 0,8 laisse de la marge pour absorber la variance sans dépasser l'intervalle. Le débit soutenable
**doit être mesuré**, pas supposé : il se lit dans `processedRowsPerSecond` sous charge stable.

L'effet est contre-intuitif mais essentiel : plafonner le batch **ne fait pas rattraper le retard plus vite**,
il garantit que la durée de batch reste **prévisible**. Sans plafond, un job qui redémarre après une heure
d'arrêt tente d'avaler une heure de données en un batch, épuise la mémoire, échoue, redémarre, et retente la
même chose — une boucle d'échec dont on ne sort pas sans intervention. Avec plafond, il rattrape par paliers
réguliers.

Déclenchement : `Trigger.ProcessingTime("10 seconds")`. Si un batch dépasse 10 s, le suivant démarre
immédiatement après — les déclenchements ne s'empilent pas, ils glissent. Le signal d'alerte est donc
`batchDuration > triggerInterval` **de façon soutenue**.

### 5.3 Les signaux à surveiller

| Signal | Source | Seuil d'inquiétude |
|---|---|---|
| `inputRowsPerSecond` vs `processedRowsPerSecond` | `StreamingQueryProgress` | entrée > traitement de façon soutenue |
| `batchDuration` | idem | > intervalle de déclenchement |
| `stateOperators[0].numRowsTotal` | idem | croissance monotone → watermark inefficace |
| lag par partition | `kafka-consumer-groups` / JMX | croissance monotone |
| `numRowsDroppedByWatermark` | `StreamingQueryProgress` | > 0 de façon répétée |

Ces valeurs sont journalisées à chaque batch par un `StreamingQueryListener` dédié, en JSON structuré. Aucune
n'est inventée dans la documentation : ce sont des sorties d'exécution.

### 5.4 Que faire quand ça sature

Dans l'ordre :

1. **Augmenter le parallélisme** : partitions Kafka **et** exécuteurs Spark ensemble — augmenter les seuls
   exécuteurs ne sert à rien, le parallélisme de lecture est plafonné par le nombre de partitions.
2. **Élargir l'intervalle de déclenchement** : moins de batches, chacun mieux amorti (moins de frais fixes par
   batch), au prix de latence.
3. **Réduire le coût des features** avant de toucher au modèle : c'est presque toujours l'agrégation, pas le
   `predict`, qui domine.
4. **Dégrader sélectivement, en dernier recours** : `telemetry.scored` alimente l'affichage et peut être
   échantillonné (1 fenêtre sur N) ; **`alerts` n'est jamais dégradé**. C'est la raison pour laquelle les deux
   sorties sont sur des topics séparés avec des exigences différentes — la séparation n'est pas esthétique,
   elle existe pour rendre cette dégradation possible.

### 5.5 En aval

**alert-service** : `max.poll.records=200` borne le lot traité par appel ; si PostgreSQL ralentit, le lag
s'accumule dans Kafka — ce qui est le bon endroit pour accumuler. Le point de vigilance est
`max.poll.interval.ms` : si le traitement d'un lot dépasse 5 minutes, le broker considère le consommateur mort
et rééquilibre, ce qui provoque un rejeu et donc des doublons. La règle est donc :
`max.poll.records × temps_par_message < max.poll.interval.ms`, avec une marge large.

**SSE vers le dashboard** : un flot d'alertes rendrait le navigateur inutilisable. Le flux est **agrégé côté
serveur** : au plus un événement par machine et par seconde, les mises à jour intermédiaires étant fusionnées.
Le dashboard affiche des alertes destinées à un humain ; au-delà de quelques-unes par seconde, la limite n'est
plus technique mais cognitive.
