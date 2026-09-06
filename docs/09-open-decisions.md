# 09 — Décisions ouvertes

Chaque décision ci-dessous a **plusieurs options défendables**. Ma recommandation est indiquée, mais l'arbitrage
te revient : ce sont exactement les points sur lesquels un jury te demandera « pourquoi ce choix ? ».

Statut : `ACCEPTÉ` = arbitré et appliqué ; `PROPOSÉ` = en attente de ta validation.

---

### D-01 — Domaine métier · `ACCEPTÉ`

**Options** : (A) télémétrie industrielle — (B) fraude bancaire.

**Recommandation : A.** Spark Structured Streaming est un moteur micro-batch : sa latence en secondes est
cohérente avec la supervision industrielle et incohérente avec l'autorisation de paiement, qui est synchrone et
sub-seconde. Choisir B créerait une faille d'architecture que le premier examinateur trouverait. Détail complet
dans `docs/00-domain-choice.md`.

**Si tu choisis B** : ~20 % du code change (simulateur, features, libellés métier). Toute l'ingénierie de
streaming reste identique.

---

### D-02 — Format de sérialisation · `ACCEPTÉ`

**Options** : (A) JSON + JSON Schema + `schema_version` — (B) Avro + Schema Registry — (C) Protobuf.

**Recommandation : A.** Le facteur décisif est technique et vérifiable : Spark ne lit pas nativement le format
de fil Confluent (octet magique + identifiant de schéma), ce qui impose ABRiS ou un découpage manuel des
octets. C'est de l'infrastructure qui ne démontre rien sur le sujet du projet. Le coût de A est que la
compatibilité repose sur la CI et non sur un serveur ; c'est explicité et outillé (`docs/02-data-contracts.md`).

**Argument pour B** : « j'ai mis en place un Schema Registry » se dit bien en entretien et démontre une maîtrise
de la gouvernance de schémas. Si tu veux ce point, il faut budgéter la friction PySpark.

---

### D-03 — Emplacement de la vérité terrain · `ACCEPTÉ`

**Options** : (A) topic dédié `telemetry.labels` — (B) bloc `ground_truth` dans le message brut.

**Recommandation : A.** Deux raisons : la fuite de données devient **impossible par construction** plutôt que
prévenue par vigilance ; et un flux d'étiquettes séparé, tardif et partiel est le modèle **fidèle** de ce qui
se passe en production. Le même topic accueille ensuite le retour opérateur, ce qui referme la boucle sans
composant supplémentaire.

**Argument pour B** : plus simple, pas de jointure hors ligne. Mais un seul oubli de projection contamine
l'entraînement, et le bug est silencieux — les métriques deviennent excellentes, ce qui n'alerte personne.

---

### D-04 — Mode de sortie Spark · `ACCEPTÉ`

**Options** : (A) `update` + identifiants déterministes — (B) `append`.

**Recommandation : A.** `append` impose une latence de fenêtre + watermark, soit ~90 s, difficile à défendre
pour un système annoncé temps réel. Le défaut d'`update` (ré-émission d'une même fenêtre) est neutralisé par
l'`alert_id` déterministe : une ré-émission met à jour au lieu de dupliquer.

**Argument pour B** : sémantique plus simple, un résultat définitif par fenêtre, aucun risque d'alerte
« révisée ». Si tu préfères la simplicité de raisonnement à la latence, c'est un choix tenable — il faut alors
assumer les 90 s dans la démonstration.

---

### D-05 — Écriture multi-sorties · `PROPOSÉ`

**Options** : (A) un `foreachBatch` écrivant les trois sorties — (B) trois `writeStream` indépendants.

**Recommandation : A.** B relit la source **trois fois** (trois consommateurs Kafka, trois checkpoints, trois
fois le calcul des features) : le travail le plus coûteux serait fait en triple. A lit une fois, met en cache le
DataFrame du batch et écrit trois fois.

**Coût de A** : `foreachBatch` ne garantit pas l'unicité d'appel pour un `batchId` donné ; l'idempotence est à
notre charge — ce qui est déjà le cas par ailleurs. On perd aussi les sinks natifs et leurs métriques.

---

### D-06 — Source du référentiel machines · `PROPOSÉ`

**Options** : (A) lecture JDBC de PostgreSQL au démarrage du job, diffusée en broadcast — (B) topic compacté
`machines.reference` — (C) fichier statique dans l'image.

**Recommandation : A.** PostgreSQL est déjà la source de vérité du référentiel ; le lire évite un second
endroit où la même donnée existe. 20 machines tiennent largement en broadcast.

**Limite** : un changement de référentiel n'est pris en compte qu'au redémarrage du job. Acceptable — un parc
machines ne change pas plusieurs fois par jour. **B** serait le choix propre si le référentiel devenait
dynamique, au prix d'un topic compacté et d'une jointure stream-static à rafraîchissement.

---

### D-07 — Normalisation du score · `PROPOSÉ`

**Options** : (A) modèle global + normalisation robuste des features par machine — (B) un modèle et un seuil par
machine — (C) modèle global sans normalisation.

**Recommandation : A.** C garantit que les machines structurellement plus chaudes alertent en permanence. B
traite parfaitement l'hétérogénéité mais exige un historique par machine, ne fonctionne pas pour une machine
neuve, et transforme l'exploitation en gestion de 20 modèles. A capture l'essentiel du bénéfice de B avec la
simplicité de C.

**Point de vigilance** : démarrage à froid d'une machine inconnue. Traité par un profil de repli explicite,
signalé dans l'événement scoré (`profile_fallback: true`) pour que ces scores restent identifiables.

---

### D-08 — Transport temps réel vers le dashboard · `ACCEPTÉ`

**Options** : (A) SSE — (B) WebSocket + STOMP.

**Recommandation : A.** Le besoin est strictement unidirectionnel, et `EventSource` gère nativement la
reconnexion avec `Last-Event-ID` — ce qui compte réellement pour un poste d'atelier au réseau instable. Avec
WebSocket, cette logique est à écrire entièrement.

**Argument pour B** : STOMP offre un abonnement par sujet côté client (`/topic/machines/M-014`), plus élégant
que le filtrage par paramètre de requête, et c'est une compétence attendue dans un contexte Spring. Si tu veux
montrer ce point, B est défendable — la reconnexion devient ta responsabilité.

---

### D-09 — Contrôle de concurrence sur l'acquittement · `PROPOSÉ`

**Options** : (A) `expectedVersion` dans le corps de la requête — (B) `ETag` + `If-Match` — (C) dernier écrit
gagne.

**Recommandation : A.** C est un bug silencieux : deux opérateurs agissent, un seul est pris en compte, aucun ne
le sait. B est plus conforme à HTTP mais impose la gestion d'en-têtes côté Angular pour un bénéfice identique
dans une API interne.

**Argument pour B** : c'est la forme canonique. Si tu vises une API destinée à des tiers, B est le bon choix.

---

### D-10 — State store Spark · `PROPOSÉ`

**Options** : (A) RocksDB — (B) `HDFSBackedStateStoreProvider` (défaut).

**Recommandation : A.** Le défaut garde l'état dans le tas de la JVM : la croissance de l'état devient une
pression GC puis un OOM, sans palier. RocksDB place l'état hors tas, la dégradation devient progressive et
diagnosticable. À notre volume, B tiendrait — mais démontrer qu'on a anticipé le mode de panne vaut mieux que
de découvrir l'OOM en démonstration.

---

### D-11 — Déploiement Spark · `PROPOSÉ`

**Options** : (A) `local[*]` dans un conteneur — (B) master + workers dans Compose.

**Recommandation : A pour la v1, B en option.** A garantit qu'un `docker compose up` fonctionne sur n'importe
quelle machine de développement. B démontre la distribution réelle mais alourdit sensiblement l'empreinte
mémoire.

**Compromis retenu** : le code est écrit sans aucune hypothèse de mono-exécuteur (pas d'état Python global
partagé, singleton par exécuteur), et un `docker-compose.cluster.yml` permet de basculer **sans modifier une
ligne**. Le fait que ce basculement soit possible sans changement de code est en soi la démonstration.

---

### D-12 — Hystérésis des alertes · `PROPOSÉ`

**Options** : (A) 2 fenêtres consécutives au-dessus du seuil — (B) alerte immédiate — (C) N configurable, réglé
par la mesure.

**Recommandation : C avec 2 comme valeur initiale.** B produit des rafales sur du bruit ponctuel. Chaque
fenêtre supplémentaire coûte 10 s de latence de détection et supprime des faux positifs — c'est exactement le
type d'arbitrage qui doit être **mesuré** puis documenté dans `docs/benchmarks/`, pas décidé à l'avance.

---

## Décisions déjà tranchées et non ouvertes

Elles découlent de contraintes techniques vérifiables, pas de préférences :

| Décision | Raison |
|---|---|
| Fenêtrage sur `event_time` | seul moyen d'obtenir un traitement déterministe et rejouable |
| At-least-once + idempotence | le sink Kafka de Spark n'est pas transactionnel — ce n'est pas un réglage |
| `alert_id` déterministe (UUIDv5) | condition de l'idempotence, dont dépendent D-04 et D-05 |
| Upsert natif plutôt que `save()` JPA | `save()` fait `SELECT` puis `INSERT`, donc une course entre les deux |
| `TIMESTAMPTZ` partout, `Instant` en Java | `TIMESTAMP` sans fuseau est ambigu par définition |
| Flyway, `ddl-auto=validate` | le schéma est un livrable relu, pas un effet de bord de l'ORM |
| Publication de `features` avec l'alerte | condition de l'explicabilité et du rejeu hors ligne |
| `auto.create.topics.enable=false` | un topic auto-créé arrive avec une seule partition, irréversiblement |

---

## Décisions de l'étape 1

### D-13 — Modélisation des messages en Python · `ACCEPTÉ`

**Retenu : dataclasses figées + codecs explicites.** Écartés : Pydantic v2, attrs.

Le contrat de référence est le JSON Schema ; le code Python s'y conforme au lieu de le redéfinir. L'argument
décisif est vérifiable : le contrat impose `2026-09-06T14:23:07.412Z`, or ni `json.dumps` ni la sérialisation
par défaut de Pydantic ne produisent cette forme — Pydantic écrit `+00:00` et une précision variable. Un codec
explicite rend le format du fil visible dans le code plutôt que dépendant des réglages d'une librairie.

Bénéfice secondaire : `schemas` et `codec` n'ont aucune dépendance tierce, ce qui compte puisque tout
`telemetry_core` est expédié aux exécuteurs Spark.

Pydantic reste utilisé pour la **configuration** (`config.py`), où le besoin est inverse : coercition depuis
l'environnement et validation au démarrage.

### D-14 — Image Kafka · `ACCEPTÉ`

**Retenu : `apache/kafka`.** D-02 ayant écarté le Schema Registry, aucun composant Confluent n'entre dans le
projet et l'image amont est cohérente. Conséquence pratique confirmée à l'usage : les chemins de scripts sont
`/opt/kafka/bin`, ce qui conditionne le healthcheck et le script de création de topics.

### D-15 — Version Kafka · `ACCEPTÉ`

**Retenu : 3.9.0 en KRaft**, tag exact, jamais `latest`.

### D-16 — Dépendances Python · `ACCEPTÉ`

**Retenu : pip + `pyproject.toml`, doctrine « une bibliothèque déclare des intervalles, une application
épingle ».** `telemetry-core` déclare des bornes ; les composants déployables auront chacun un lock.

### D-17 — Console Kafka · `ACCEPTÉ`

**Retenu : profil Compose `tools`.** La pile par défaut reste minimale.

### D-18 — Champ `source_event_count` retiré du contrat · `ACCEPTÉ`

Il dupliquait `sample_count` dans `ScoredEvent`. Une redondance dans un contrat finit toujours par diverger :
deux champs censés dire la même chose, deux producteurs, et un jour deux valeurs différentes sans moyen de
savoir laquelle fait foi. Retiré avant tout usage, donc sans coût de migration.
