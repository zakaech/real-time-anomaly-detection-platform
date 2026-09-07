# 10 — Pipeline Spark Structured Streaming (Phase 3)

Ce document décrit ce qui a été implémenté, **ce qui a été mesuré**, et ce qui ne l'a pas été.

> **Sur les chiffres.** Toutes les valeurs de ce document proviennent d'exécutions réelles, relues depuis les
> topics par `stream_processor.tools.inspect_stream`. Aucune n'est estimée. Ce sont des mesures **locales**,
> sur une seule machine de développement, avec `local[2]` et un unique broker : elles caractérisent ce
> pipeline dans ces conditions et **ne constituent pas un benchmark industriel**. Elles ne sont pas
> transposables à un cluster, ni à un débit de production.

## 1. Ce que fait le job

```
telemetry.raw
     │
     ├─ parse + validation ───────┬─► telemetry.dlq   (erreurs de données uniquement, ADR-002)
     │                            └─► telemetry.late  (retard de publication > watermark)
     ▼
temps d'événement + watermark 90 s
     ▼
fenêtres glissantes 60 s / 10 s, groupées par machine
     ▼
52 features (spécification v2.0.0, identiques à l'entraînement)
     ▼
admission : >= 30 échantillons, running_ratio >= 0,9, dernier état == RUNNING
     ▼
scoring (One-Class SVM 2.0.0, chargé une fois par worker, ADR-001)
     ▼
telemetry.scored   ◄── frontière contractuelle
     ▼
machine à états d'alerte (2 fenêtres consécutives ouvrent, 120 s de silence ferment)
     ▼
alerts
```

Trois requêtes, **un checkpoint chacune**. En partager un corromprait les deux états.

`telemetry.scored` est une frontière contractuelle et non un détail interne : la Phase 4 (service Spring) la
consomme sans rien savoir de Spark. C'est ce qui permet de remplacer le moteur de traitement sans toucher au
consommateur.

## 2. Parité des features : l'invariant non négociable

Les mêmes 52 features sont calculées par **trois moteurs** : la référence `telemetry_core` (Python pur), le
chemin pandas de `ml-training`, et Spark ici. Une divergence silencieuse ferait scorer au modèle une
distribution qu'il n'a jamais vue, sans que rien n'échoue.

Le mécanisme n'est pas la discipline, c'est la construction : la traduction Spark est **pilotée par
`FEATURE_SPECS`**, la même structure qui pilote la référence. Ajouter une feature d'un côté sans l'autre est
impossible ; la modifier des deux côtés différemment est détecté par `test_feature_conformance.py`, qui compare
la sortie Spark à la référence sur un historique complet, sur des données fortement manquantes et sur un
capteur figé.

Deux subtilités que la parité a imposées :

- **`min_by` / `max_by` sur le temps d'événement**, jamais `first()` / `last()`. Ces derniers sont
  explicitement non déterministes après un shuffle : le test de conformance aurait échoué par intermittence,
  ce qui est le pire mode de défaillance possible (D-26).
- **`stddev_samp`** (ddof=1) épinglé dans les trois moteurs, et corrélations en suppression par paires,
  renvoyant `null` à variance nulle plutôt qu'une valeur arbitraire.

La comparaison tolère 1e-9 : l'ordre de sommation de Spark dépend du partitionnement, donc l'égalité flottante
exacte n'est pas une propriété que le code peut garantir. Le seuil est documenté, pas choisi après coup pour
faire passer le test.

## 3. Sémantique de livraison — ce qui est garanti et ce qui ne l'est pas

**At-least-once** de bout en bout. **Effectively-once** à la persistance, par `alert_id` déterministe (UUIDv5)
et upsert idempotent.

**Ce n'est pas de l'exactly-once, et ce n'est jamais présenté comme tel.** Le sink Kafka de Spark n'est pas
transactionnel dans cette configuration : au redémarrage, le lot non commité est rejoué et republié. Les
doublons sont donc **attendus**, pas accidentels. Ce document en donne le nombre mesuré après un vrai crash
(section 5.2) au lieu d'affirmer qu'il n'y en a pas.

Trois emplacements absorbent la ré-émission :

1. `telemetry.scored` est un **état de fenêtre**, pas un événement immuable ;
2. la machine à états d'alerte déduplique par `(machine_id, window_start)` — plusieurs mises à jour d'une même
   fenêtre comptent pour **une** fenêtre dans l'hystérésis ;
3. `alert_id` est dérivé de façon déterministe, donc une ré-émission met à jour au lieu de dupliquer.
## 4. Patterns employés, et pourquoi

| Pattern | Où | Ce qu'il résout ici |
|---|---|---|
| **Dead Letter Channel** (EIP) | `split_valid` → `telemetry.dlq` | Un message invalide ne bloque pas le lot et n'est pas perdu. Restreint aux **erreurs de données** : router une panne d'infrastructure ici jetterait des messages valides pendant un incident (ADR-002). |
| **Idempotent Receiver** (EIP) | `alert_id` UUIDv5 + upsert | Rend la ré-émission inoffensive, ce qui est la seule façon honnête de vivre avec de l'at-least-once. |
| **Content Enricher** (EIP) | `derive_features` | La fenêtre brute devient un vecteur de 52 features sans que le producteur en sache rien. |
| **Singleton par processus** | `model.load_model` | Un SVM à plusieurs centaines de vecteurs de support est dépicklé une fois par worker, pas une fois par lot. Verrouillage à double vérification : plusieurs lots Arrow peuvent y arriver simultanément. |
| **Strategy** | `CalibratedAnomalyModel` | Le job ne connaît pas le détecteur qu'il exécute ; changer de modèle ne change pas une ligne du streaming. |
| **State** | `make_alert_state_handler` | L'ouverture et la fermeture d'un épisode sont un automate explicite, pas une accumulation de conditions. |
| **Template Method** | `kafka_writer` | Les trois sorties partagent acquittements, compression et checkpoint ; seules la requête et la destination varient. |

**Ce qui a été délibérément écarté** : Kubernetes, Schema Registry, MLflow, Redis. Chacun ajouterait de
l'infrastructure sans rien démontrer de plus sur le sujet du projet, et déplacerait la complexité vers
l'exploitation au lieu du traitement. `local[2]` est un paramètre, pas une hypothèse : aucune ligne de code ne
suppose un seul exécuteur.

## 5. Résultats mesurés
### 5.1 Conditions

Une seule machine de développement, Docker Desktop sous Windows 11, Spark en `local[2]`, un broker Kafka, un
disque. **Ce ne sont pas des conditions de benchmark** et rien ici n'est présenté comme tel.

Jeu d'entrée, produit par le simulateur en mode rejeu, graine `20260907`, 4 heures d'historique :

| Grandeur | Valeur mesurée |
|---|---|
| Messages publiés dans `telemetry.raw` | **215 997** |
| Machines | 15 |
| Échantillons anormaux | 7 010 |
| Épisodes distincts | 66 |
| Échantillons retardés par le simulateur | 4 321 |

215 997 et non 216 000 : trois échantillons étaient encore dans le tampon de retard à l'arrêt du simulateur,
qui les a comptés et jetés explicitement plutôt que de les publier avec un horodatage faux.

### 5.2 Test de crash et de reprise

**Protocole.** Remise à zéro complète (topics et volume de checkpoints supprimés), rejeu des 4 heures, puis
démarrage du job. À t+100 s, relevé de l'état ; `docker kill --signal=KILL` ; redémarrage sur **le même volume
de checkpoints** ; attente de la fin du rattrapage.

| Étape | Mesure |
|---|---|
| État à t+100 s | `telemetry.scored` = 14 635, `alerts` = 99 |
| Derniers lots avant le crash | validation 8, scoring 6, alerting 5 |
| État de la requête de scoring | 2 231 lignes, 0 ligne écartée par le watermark |
| Signal | `SIGKILL`, code de sortie **137**, `oomkilled: false` |
| Lots au redémarrage | validation **9**, scoring **7**, alerting **6** |
| Code de sortie de la reprise | **0**, **0 erreur** dans les journaux |

Le job **reprend là où il s'était arrêté** : il redémarre aux lots 9 / 7 / 6, pas à zéro. C'est le checkpoint
qui le permet, et c'est la raison pour laquelle son volume doit survivre au conteneur.

**Résultat final, après crash et reprise :**

| Grandeur | Valeur |
|---|---|
| Messages dans `telemetry.scored` | 22 581 |
| **Fenêtres distinctes** | **21 690** |
| Fenêtres scorées | 18 331 |
| Fenêtres anormales | 746 |
| Non scorées : machine à l'arrêt | 3 723 |
| Non scorées : échantillons insuffisants | 527 |
| Messages dans `alerts` | 189 |
| Identifiants d'alerte distincts | 181 |
| **Livraisons dupliquées** | **8** |
| `telemetry.dlq` / `telemetry.late` | 0 / 0 |

**Le chiffre qui compte : 21 690 fenêtres distinctes, exactement le nombre produit par une exécution sans
crash.** Aucune fenêtre n'a été perdue. Le contrôle indépendant le confirme : la mesure d'admission, calculée
en **batch** sur le même `telemetry.raw`, compte elle aussi 21 690 fenêtres — deux chemins de calcul
différents arrivent au même total.

Les **8 doublons** ne sont pas un défaut, ce sont les 8 alertes republiées par le lot non commité au moment du
crash. C'est exactement ce que signifie *at-least-once*, et c'est pourquoi la déduplication repose sur un
`alert_id` déterministe plutôt que sur une promesse d'unicité. Sans le crash, il y en aurait zéro ; les
compter est la seule façon honnête de documenter la sémantique.

`consecutive_windows_at_open` vaut **2 au minimum et 2 au maximum** : l'hystérésis ouvre toujours à la
deuxième fenêtre consécutive, jamais à la première, jamais plus tard.

**`processing_delay_ms` n'est pas exploitable sur ce run** (médiane ≈ 7,4 × 10⁶ ms). Pendant un rejeu
accéléré, `scored_at − window_end` mesure **l'âge de l'historique rejoué**, pas le retard du pipeline.
Confondre les deux donnerait une latence absurde de deux heures. La latence est donc mesurée séparément, en
temps réel (section 5.4).

### 5.3 L'effet réel du filtre `machine_state == RUNNING`

Le plan de la Phase 3 ajoutait cette condition à côté de `running_ratio >= 0,9`, en demandant explicitement de
**mesurer son effet** plutôt que d'affirmer qu'elle améliore les performances. Le topic ne peut pas répondre
seul : les deux conditions se confondent dans un unique motif `MACHINE_NOT_RUNNING`, et `running_ratio` n'est
pas une feature publiée, donc leur recouvrement n'est pas observable en aval.

`stream_processor.tools.measure_admission` applique donc le **même** fenêtrage en batch et compte chaque
condition séparément, ainsi que leur recouvrement.

| Condition | Fenêtres rejetées |
|---|---|
| Fenêtres totales | 21 690 |
| Échantillons insuffisants (< 30) | 90 |
| `running_ratio < 0,9` | 3 534 |
| Dernier état ≠ `RUNNING` | 2 625 |
| — rejetées par **les deux** | 2 512 |
| — **rejetées par le seul filtre d'état** | **113** |
| Admises avec le filtre | 17 953 |
| Admises sans le filtre | 18 066 |

**Conclusion mesurée : le filtre d'état retire 113 fenêtres sur 18 066, soit 0,63 %.** Son effet est
**marginal**, et non l'amélioration substantielle qu'on pourrait supposer : il recouvre à 96 % le filtre de
ratio (2 512 des 2 625 fenêtres qu'il rejette l'étaient déjà).

Les 113 fenêtres qu'il ajoute ont une interprétation précise : la machine a tourné plus de 90 % de la fenêtre
**mais s'est arrêtée avant la fin**. Scorer ces fenêtres reviendrait à juger un régime de fonctionnement à
partir d'agrégats dont la fin décrit un arrêt. Le filtre est donc conservé — parce qu'il est **correct**, non
parce qu'il serait quantitativement important. C'est la formulation que la mesure autorise ; toute autre
serait une extrapolation.
### 5.4 Exécution temps réel — latence, mode `update`, canaux latéraux

Exécution distincte du backfill, et c'est délibéré : les deux mesurent des choses différentes et les mélanger
donnerait une latence absurde. Simulateur en mode temps réel, graine `515151`, 300 s, 15 machines ; détection
de retard **activée** ; quatre messages fabriqués injectés en cours de route pour exercer les canaux latéraux.

| Grandeur | Valeur mesurée |
|---|---|
| Messages dans `telemetry.raw` | 4 503 (4 499 du simulateur + 4 fabriqués) |
| Code de sortie du job / du simulateur | 0 / 0 |
| Messages dans `telemetry.scored` | 3 060 |
| Fenêtres distinctes | 540 |
| **Ratio de mise à jour** | **5,67** |
| Émissions maximales pour une seule fenêtre | 8 |
| Messages dans `alerts` | 16, **16 identifiants distincts, 0 doublon** |
| `consecutive_windows` à l'ouverture | 2 (min et max) |

**Le coût du mode `update`, enfin chiffré.** Le plan exigeait de le mesurer avant toute optimisation : chaque
fenêtre est publiée **5,67 fois en moyenne**, jusqu'à **8 fois**. En backfill le même ratio valait 1,04 — parce
que les données arrivent en masse et qu'une fenêtre se remplit entièrement dans un seul lot. **C'est le régime
temps réel qui révèle le vrai coût**, et c'est celui qui compte.

Les **0 doublon** ici, contre 8 après le crash, confirment que les doublons du test de reprise venaient bien du
rejeu du lot non commité, et non d'un défaut permanent du pipeline.

**Latence.** `processing_delay_ms` mesuré : min **−50 455 ms**, médiane **−14 643 ms**, max **+51 171 ms**.

Les valeurs négatives ne sont pas une anomalie de mesure : en mode `update`, une fenêtre est republiée à chaque
déclenchement qui la modifie, donc **avant** `window_end`. La médiane de **−14,6 s** signifie que la moitié des
émissions parviennent au consommateur **quatorze secondes avant la fermeture de la fenêtre**. En mode `append`
la première publication serait structurellement à `window_end + watermark`, soit **+90 s** au plus tôt.

Ce constat a révélé une **documentation fausse** : le contrat et la docstring décrivaient
`processing_delay_ms` comme une « latence de bout en bout observée ». La mesure le contredit. Les deux ont été
corrigés — le champ dit **quand une fenêtre a été publiée par rapport à sa fermeture**, ce qui est légitimement
négatif tant qu'elle se remplit.

**Débit et durée des lots.**

| Requête | Lots | Durée médiane | Lignes en entrée / lot (min–méd–max) | État max | Écartées par le watermark |
|---|---|---|---|---|---|
| validation | 33 | 10 019 ms | 62 – 298 – 386 | 0 | **0** |
| scoring | 33 | 10 208 ms | 108 – 150 – 188 | 252 | **0** |
| alerting | 33 | 10 058 ms | 45 – 105 – 109 | 15 | **0** |

**Ces durées ne mesurent pas une capacité de traitement.** Elles se groupent toutes autour de 10 s,
c'est-à-dire exactement l'intervalle de déclenchement configuré : à 15 messages par seconde, le pipeline est
**limité par le déclencheur, pas par le calcul**. En déduire un débit maximal serait une erreur de lecture. Le
seul chiffre de débit réellement observé vient du rattrapage d'arriéré (section 5.2), où 215 997 messages ont
été absorbés, et même celui-là reste sous le plafond configuré de 20 000 offsets par déclenchement.

**`numRowsDroppedByWatermark` vaut 0 sur les 99 lots des trois requêtes.** Le watermark de 90 s n'a écarté
aucune ligne, ce qui est cohérent avec le pire cas configuré du simulateur (≈ 84 s) — la valeur est suffisante
**pour cet environnement**, et c'est tout ce que cette mesure permet d'affirmer.

L'état de la requête d'alerte plafonne à **15 lignes**, une par machine : l'automate ne fuit pas.

Enfin, la requête de validation lit **8 946 lignes pour 4 473 messages**, soit exactement le double. C'est la
conséquence directe de son union de deux branches issues de la même source : Spark parcourt la source une fois
par branche. C'est le prix du choix « une requête, deux destinations via une colonne `topic` » ; il est mesuré
et assumé, pas ignoré.

### 5.5 Canaux latéraux, vérifiés de bout en bout

Le simulateur ne produit que des messages valides : aucune exécution normale ne remplit `telemetry.dlq`. Quatre
messages ont donc été **fabriqués** et injectés dans `telemetry.raw` pendant l'exécution.

| Message injecté | Destination attendue | Résultat mesuré |
|---|---|---|
| JSON tronqué | `telemetry.dlq` | `MALFORMED_JSON` × 1 |
| `machine_id` absent | `telemetry.dlq` | `SCHEMA_VALIDATION_FAILED` × 1 |
| `schema_version` = 9.0 | `telemetry.dlq` | `UNSUPPORTED_SCHEMA_VERSION` × 1 |
| Publié 300 s après son temps d'événement | `telemetry.late` | 1 message, retard **300,0 s** exactement |

**Les 3 enveloppes de rebut sont rejouables** : `with_recoverable_payload = 3`. Les octets d'origine ressortent
intacts du topic, ce qui est la seule propriété qui rend la file de rebut utile — et c'est précisément ce que
le découpage MIME de `base64` avait cassé (section 6.8).

Le message en retard porte un retard mesuré de **300,0 s**, identique à celui construit : la mesure de retard
est bien `ingest_time − event_time`, et non une comparaison à l'horloge locale.

### 5.6 Le défaut que seule l'exécution temps réel pouvait révéler

**872 émissions sur 1 348 ont été jugées anormales, soit 64,7 %.** En backfill, le même pipeline avec les mêmes
règles en trouvait 4 %. Un tel écart n'est pas un hasard statistique ; il fallait l'expliquer avant de publier
quoi que ce soit.

Hypothèse : en mode `update`, une fenêtre est scorée **pendant qu'elle se remplit**, alors que le modèle a été
entraîné sur des fenêtres **complètes** de 60 échantillons. Le seuil d'admission n'exige que 30 échantillons.
Vérification, en regroupant les émissions scorées par nombre d'échantillons :

| Échantillons dans la fenêtre au moment de l'émission | Émissions | Jugées anormales |
|---|---|---|
| 30 – 39 | 383 | **383 (100,0 %)** |
| 40 – 49 | 351 | 335 (95,4 %) |
| 50 – 59 | 322 | 151 (46,9 %) |
| **60 et plus (fenêtre complète)** | 292 | **3 (1,0 %)** |

L'hypothèse est confirmée sans ambiguïté. **Une fenêtre incomplète est classée anormale dans pratiquement
100 % des cas ; une fenêtre complète dans 1,0 %** — cohérent avec les 4 % du backfill et avec le budget
d'alertes calibré en Phase 2. En ne gardant que la dernière émission de chaque fenêtre, le taux tombe à 22,2 %
(86 sur 387).

Ce ne sont pas des anomalies : ce sont des fenêtres **hors distribution**. Une moyenne, un écart-type et une
pente calculés sur 30 secondes ne ressemblent pas à ceux calculés sur 60 secondes, et le modèle n'a jamais vu
les premiers.

**Conséquence directe et visible** : 16 alertes pour 15 machines, **toutes `CRITICAL`**, en cinq minutes. C'est
une tempête d'alertes — exactement ce que l'hystérésis devait empêcher, et qu'elle ne peut pas empêcher
puisque les fenêtres partielles successives sont réellement consécutives et réellement au-dessus du seuil.

**Pourquoi le backfill ne l'a pas montré.** Les données y arrivent par blocs de plusieurs milliers de messages :
une fenêtre passe de vide à complète à l'intérieur d'un même lot, et n'est presque jamais publiée dans un état
partiel. Le ratio de mise à jour de 1,04 le disait déjà. **Un test de charge en rejeu ne remplace pas une
exécution temps réel**, et c'est la leçon la plus importante de cette phase.

Ce défaut n'est **pas corrigé dans cette phase** : le corriger demande d'arbitrer entre plusieurs règles
défendables, ce qui relève d'une décision et non d'un détail d'implémentation. Il est ouvert en **D-37**, avec
les options et leur coût en latence. Il est documenté ici avec ses chiffres plutôt que corrigé à la hâte et
sans mesure.
## 6. Incidents rencontrés, et ce qu'ils ont coûté

Cette section est écrite pour la soutenance : chaque entrée est un problème réel, avec son symptôme, sa cause
et le correctif. Aucun n'a été contourné en silence.

### 6.1 Kafka KRaft refusait de démarrer

**Symptôme.** Boucle de redémarrage, `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`.

**Cause.** En mode combiné (broker + contrôleur dans le même processus), Kafka dérive le point d'accès annoncé
du contrôleur depuis `listeners`. Une adresse d'écoute universelle est valide pour écouter, jamais pour être
annoncée.

**Correctif.** Forme à hôte vide, `INTERNAL://:9092` : écoute partout, annonce le nom du conteneur.

### 6.2 PySpark 3.5.3 échouait sur Java 21

**Symptôme.** `sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long,int) not available`, dès qu'Arrow est
sollicité — donc sur tout le chemin de scoring.

**Premier diagnostic : faux.** J'ai d'abord attribué l'échec à numpy 2. La trace mentionnait Arrow, et
l'hypothèse tenait jusqu'à ce que la **même** image soit testée avec deux JVM. Spark 3.5 n'ouvre pas les
modules JDK internes dont Arrow a besoin ; Java 17 les expose encore.

**Correctif.** `python:3.11-slim-bookworm` + `openjdk-17-jre-headless` (D-33). La CI installe explicitement
Temurin 17, sinon elle validerait sur une JVM que l'image n'utilise pas.

### 6.3 Le driver ne traitait aucun lot

**Symptôme.** Trois requêtes déclarées actives, **zéro lot terminé**, 0,4 % de CPU pendant quatre minutes.
Aucune erreur, aucune exception.

**Cause.** Le `StreamingQueryListener` en Python passe par le serveur de rappels Py4J. Avec trois requêtes
concurrentes, le driver passait son temps dans cette passerelle au lieu d'exécuter des lots.

**Correctif.** Instrumentation **par sondage** : `query.recentProgress` retourne les mêmes enregistrements, en
les tirant, sans serveur de rappels. Les lots sont dédupliqués par `(requête, identifiant de lot)`, donc un
sondage lent rapporte chaque lot exactement une fois. Le même pipeline sans écouteur a vidé l'arriéré en douze
lots.

**Leçon.** Le premier réflexe — « Spark est lent » — était faux. La mesure a désigné le coupable ; elle a aussi
été construite en réaction à ce problème, ce qui est l'ordre inverse du bon.

### 6.4 `awaitAnyTermination` prend des secondes, pas des millisecondes

**Symptôme.** Le job tournait mais n'émettait aucune métrique.

**Cause.** J'avais passé `5000` en croyant à des millisecondes. Le sondage attendait donc ~83 minutes entre
deux relevés, soit bien plus que la durée de la démonstration.

**Correctif.** `_POLL_INTERVAL_SECONDS = 5.0`. Une erreur d'unité, sans exception : le job avait l'air sain.

### 6.5 Un checkpoint fantôme faisait passer le job pour inactif

**Symptôme.** Le job démarrait, ne lisait rien, ne produisait rien. Aucune erreur, aucun stage.

**Cause.** `docker compose down -v` n'a **pas** supprimé le volume de checkpoints, parce que des conteneurs
lancés hors compose (`docker run`) le référençaient encore. Le job reprenait donc au bout du topic et n'avait
effectivement plus rien à lire.

**Correctif.** Suppression explicite du volume (`docker volume rm -f`) dans la procédure de remise à zéro.
C'est aussi une leçon sur les checkpoints : ils **survivent** à ce qu'on croit avoir nettoyé, et c'est
précisément leur rôle.

### 6.6 L'image contenait du code périmé

**Symptôme.** Un réglage venait d'être ajouté et n'existait pas à l'exécution ; tout l'historique rejoué partait
dans `telemetry.late`.

**Cause.** `pip install -e` dans le Dockerfile pointe vers les sources **copiées au build**. Sans montage du
dépôt, un conteneur exécute le code de la dernière construction.

**Correctif.** Reconstruire après chaque modification de source, ou monter les sources pour un aller-retour
rapide. Le même piège s'est reproduit plus tard avec un outil ajouté après la construction — les deux fois,
le symptôme ressemblait à un bug de logique.

### 6.7 Arrow ne renvoie pas des nanosecondes

**Symptôme.** Le test de conformance des features joignait **zéro ligne**.

**Cause.** `.toPandas()` avec Arrow renvoie `datetime64[us]`, pas `[ns]`. Diviser par 1e9 donnait des valeurs
1000 fois trop petites, donc aucune clé de jointure ne correspondait.

**Correctif.** Une conversion indépendante de l'unité. Le test échouait *bruyamment*, ce qui est exactement ce
qu'on lui demande.

### 6.8 `base64` de Spark découpe en MIME

**Symptôme.** Des enveloppes de rebut relues depuis le topic étaient refusées par un décodeur strict.

**Cause.** La fonction `base64` de Spark émet des lignes de 76 caractères séparées par des retours au-delà de
cette longueur. C'est du base64 valide, refusé par `validate=True`.

**Correctif.** Suppression des blancs avant décodage. Trouvé en **relisant le topic**, pas en le supposant.

### 6.9 Le timeout d'état tuait la requête au redémarrage

**Symptôme.** Après un crash volontaire, le conteneur redémarré sortait en code 1 :

```
pyspark.errors.exceptions.base.PySparkValueError: [INVALID_TIMEOUT_TIMESTAMP]
Timeout timestamp (1788796360000) cannot be earlier than the current watermark (1788796700000).
```

`telemetry.scored` restait figé, le checkpoint de scoring bloqué à 139 978 offsets sur 216 000.

**Cause.** La machine à états d'alerte arme son timeout sur le temps d'événement, à partir de la fenêtre la
plus récente vue pour cette machine. Au redémarrage, le watermark est **restauré depuis le checkpoint** alors
que le lot rejoué porte des fenêtres antérieures : le timeout calculé se retrouve derrière le watermark, et
Spark refuse — en tuant la requête.

**Correctif.** Le timeout est borné par le watermark courant (`state.getCurrentWatermarkMs()`).

**Le point important.** Ce défaut est **inatteignable sur le chemin nominal**. Aucune exécution normale ne le
déclenche : il fallait un crash, un redémarrage et un rejeu pour le produire. Il a été trouvé par le test de
reprise, et une régression le couvre désormais — avec un faux `GroupState` qui lève la même exception que
Spark, sinon le test passerait sans rien vérifier.

### 6.10 Un `.env` vide empêchait le service de démarrer

**Symptôme.** `pydantic_core.ValidationError: run_seconds — Input should be a valid number, unable to parse
string as a number [input_value='']`.

**Cause.** Docker Compose développe une variable non définie en **chaîne vide** : `FOO: ${FOO:-}` produit
`FOO=""`, pas une variable absente. Lue comme une valeur, elle n'est pas un flottant. Le `.env.example`
versionné laisse justement les variables optionnelles à blanc — donc la commande `docker compose up`
documentée dans le README ne pouvait pas démarrer le service.

**Portée.** Le même défaut touchait le simulateur (`SIMULATOR_SEED=`) et la fabrique de configuration partagée.

**Correctif.** `env_ignore_empty=True` dans les quatre configurations : une variable vide vaut « non définie ».
Deux tests de régression vérifient que l'optionnel retombe sur son défaut **et** qu'un champ requis laissé
vide échoue toujours — la règle ne doit pas transformer une adresse de broker vide en défaut silencieux.
### 6.11 La durée des lots était journalisée à `null`

**Symptôme.** `batch_duration_ms` valait `null` sur **tous** les lots de toutes les exécutions.

**Cause.** La forme dictionnaire d'un enregistrement de progression Spark ne contient pas de clé
`batchDuration`. La durée est dans la carte `durationMs`, ventilée par phase, dont `triggerExecution` est le
déclenchement complet.

**Correctif.** Repli sur `durationMs["triggerExecution"]`, et trois tests qui figent le comportement, y
compris le cas d'un lot vide où Spark n'émet aucune phase.

**Ce que ça a coûté.** La seule métrique disant si un lot dépasse son intervalle de déclenchement était absente
de toutes les mesures faites jusque-là. Il a fallu **refaire une exécution complète** pour l'obtenir. Une
métrique qui vaut silencieusement `null` est pire qu'une métrique absente : sa ligne existe dans les journaux
et donne l'illusion qu'elle est surveillée.

### 6.12 Un script d'injection qui annonçait un succès qu'il n'avait pas obtenu

**Symptôme.** Une exécution temps réel complète s'est terminée avec `telemetry.dlq` et `telemetry.late` vides,
alors qu'elle devait injecter quatre messages fabriqués. Le script avait pourtant affiché
« injected 4 crafted messages ».

**Cause.** Deux erreurs superposées. `docker cp /tmp/payloads.txt` sous Windows résout `/tmp` en `C:	mp`, un
chemin qui n'existe pas ; et la ligne de confirmation était un `echo` **inconditionnel**, exécuté que la copie
ait réussi ou non.

**Correctif.** Transmission par l'entrée standard plutôt que par copie, et vérification du nombre de messages
réellement présents dans le topic avant d'annoncer quoi que ce soit.

**La leçon est la plus importante des douze**, et elle n'est pas technique : un script qui affirme avoir fait
quelque chose sans le vérifier produit exactement le genre de résultat qu'il ne faut jamais rapporter. Le
défaut a été trouvé parce que les compteurs de topics contredisaient le message de succès — c'est-à-dire parce
que la mesure ne faisait pas confiance au script.

## 7. Limites, et ce qui n'a pas été mesuré

Cette section existe pour éviter de laisser croire à plus que ce qui a été fait.

- **Les mesures sont locales.** Une machine, `local[2]`, un broker, un disque. Elles ne disent rien du
  comportement sur un cluster, ni sous une charge de production, ni avec plusieurs exécuteurs concurrents sur
  le même topic. Aucune extrapolation n'en est tirée.
- **Le débit mesuré en rejeu n'est pas une latence.** Pendant un backfill accéléré, `scored_at − window_end`
  mesure l'âge de l'historique rejoué, pas le retard du pipeline. C'est pourquoi la latence est rapportée
  depuis une exécution **temps réel** séparée, et pourquoi les deux ne sont pas mélangées.
- **Le watermark de 90 s est calibré sur le simulateur**, dont la distribution de retard est connue parce
  qu'elle est générée. Sur des données réelles, cette valeur doit être recalibrée depuis la distribution
  mesurée, en s'appuyant sur `numRowsDroppedByWatermark` (ADR-003).
- **La détection d'anomalies elle-même n'est pas réévaluée ici.** Les performances du modèle sont celles de la
  Phase 2, mesurées hors ligne sur un jeu étiqueté ; la Phase 3 vérifie que le **même** calcul est appliqué en
  flux, pas que le modèle est bon. Le constat honnête de la Phase 2 reste : la référence 3-sigma battait
  Isolation Forest à tous les points de fonctionnement, et les pics courts de type OVERHEAT sont dilués dans
  des fenêtres de 60 s.
- **La reprise n'est testée que sur un `SIGKILL` du conteneur.** Une perte de broker, une corruption de
  checkpoint ou une panne disque ne sont pas couvertes.
- **Aucune mesure de montée en charge n'a été faite.** Le seul chiffre de débit provient d'un rattrapage
  d'arriéré, qui est un régime favorable : le job lit à son rythme, sans producteur concurrent.

## 8. Ce que la Phase 4 consommera

`telemetry.scored` et `alerts`, sans rien savoir de Spark. Les deux contrats sont figés dans
`contracts/json-schema/` et vérifiés par des tests de conformance des deux côtés.

La sémantique que le service Spring devra respecter est **at-least-once** : l'upsert sur `alert_id` n'est pas
une optimisation, c'est ce qui rend la chaîne correcte.
