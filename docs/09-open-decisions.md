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

---

## Décisions de la Phase 2

### D-25 — Extension du jeu de features en v2.0.0 · `ACCEPTÉ`

52 features au lieu de 36. Les deux familles manquantes du cahier des charges — écart à la moyenne mobile et
corrélations inter-capteurs — l'imposaient. `FEATURE_SET_VERSION` passe en `2.0.0` et l'artefact refuse de se
charger si la version ne correspond pas : un modèle entraîné sur la v1 scorerait des colonnes qui ne veulent
plus dire la même chose, sans que rien n'échoue bruyamment.

### D-26 — `min_by`/`max_by` plutôt que `first`/`last` · `ACCEPTÉ`

Défaut trouvé dans la spécification de la Phase 1. `slope_per_second` était définie via « premier » et
« dernier », dont la traduction Spark naturelle utilise `first()`/`last()` — **explicitement non
déterministes** : leur résultat dépend de l'ordre des lignes, non garanti après un shuffle. Le test de
conformance de la Phase 3 aurait échoué par intermittence.

La sémantique est redéfinie en `min_by(valeur, event_time)` et `max_by(valeur, event_time)`, déterministes tant
que `(machine_id, event_time)` est unique.

### D-27 — Entraînement sur données contaminées · `ACCEPTÉ`

Corrige une consigne écrite en Phase 0. Filtrer l'entraînement par les labels est une fuite : la production ne
sait pas ce qui est propre. La variante filtrée est mesurée en secondaire pour chiffrer l'écart.

### D-28 — Prévalence abaissée à 0,4 épisode/heure · `ACCEPTÉ`

La précision dépend directement de la prévalence. À 1,5 épisode/heure la précision affichée aurait été
flatteuse et non transposable. Le rapport indique la prévalence réellement mesurée à côté de chaque chiffre de
précision.

### D-29 — Normalisation par machine dans le Pipeline · `ACCEPTÉ`

`PerMachineNormalizer` est le premier étage du Pipeline sérialisé, ce qui impose que l'entrée porte
`machine_id`. C'est le prix à payer pour que la Phase 3 n'ait aucune logique ML à réimplémenter.

### D-30 — Elliptic Envelope en quatrième modèle exploratoire · `ACCEPTÉ`

Distingué des trois obligatoires dans le rapport. Il est le seul candidat à ne pas pouvoir prendre le jeu de
features complet, pour une raison mesurée en Phase 2 : `range = max − min` rend la covariance de rang
déficient, et scikit-learn **ne refuse pas** — il avertit et continue avec un conditionnement de l'ordre de
10¹⁶, au bord de la précision float64. C'est pire qu'un échec, puisque le modèle se charge et score comme si de
rien n'était. Les colonnes `*_range` sont donc exclues délibérément.

### D-31 — Fenêtre anormale si `anomaly_fraction >= 0.25` · `ACCEPTÉ`

« Tout recouvrement » qualifierait d'anormale une fenêtre de 60 s contenant un pic de 2 s, dont les agrégats ne
bougent quasiment pas. La sensibilité à ce seuil est rapportée.

### D-32 — Imputation médiane avec indicateurs de manquant · `ACCEPTÉ`

Un capteur en panne produit des features nulles ; l'indicateur transforme cette absence en colonne explicite,
donc en signal exploitable, au lieu que l'imputation l'efface.

### D-33 — Image `python:3.11-slim-bookworm` + OpenJDK 17 · `ACCEPTÉ`

Contrainte mesurée, pas une préférence. Sur une base plus récente (Java 21), PySpark 3.5.3 échoue dès qu'Arrow
est sollicité — donc à chaque `applyInPandas` et à chaque `pandas_udf`, c'est-à-dire sur tout le chemin de
scoring : `sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long,int) not available`. Spark 3.5 n'ouvre pas
les modules JDK internes dont Arrow a besoin ; Java 17 les expose encore. Le premier diagnostic — une
incompatibilité numpy 2 — était faux, et l'a été jusqu'à ce que la même image soit testée avec les deux JVM.

La CI installe explicitement Temurin 17 pour la même raison, sinon elle validerait sur une JVM que l'image
n'utilise pas.

### D-34 — Spark 3.5.3 · `ACCEPTÉ`

Dernière version compatible Python 3.11 et Java 17 au moment de la Phase 3. Spark 4 impose Java 17+ et modifie
le connecteur Kafka ; l'intérêt pédagogique est nul et le risque de régression réel.

### D-35 — `ml-training` installé dans l'image du `stream-processor` · `ACCEPTÉ`

**Options** : (A) installer `ml-training` — (B) réimplémenter le chargement du modèle.

**A.** Le pickle référence `CalibratedAnomalyModel` et `PerMachineNormalizer` : sans le module d'origine
importable, `joblib.load` lève `ModuleNotFoundError`. B signifierait dupliquer la normalisation par machine et
la calibration ECDF dans un second code — exactement le genre de duplication qui diverge en silence et que le
triangle de conformité des features existe pour empêcher.

Le coût assumé : l'image du job de streaming dépend du composant d'entraînement. C'est une dépendance de
sérialisation, pas d'architecture — elle disparaîtrait avec un format d'export neutre (ONNX, PMML), au prix
d'une conversion à valider.

### D-36 — Watermark 90 s + mode de sortie `update` · `ACCEPTÉ`

Le watermark passe de 30 s (conception Phase 0) à 90 s. Le simulateur met une machine en tampon 20–75 s lors
d'une coupure réseau et ajoute jusqu'à 9 s de gigue : le pire cas configuré est d'environ 84 s. Un watermark de
30 s aurait écarté précisément les données générées pour exercer ce mécanisme. **La valeur est propre à
l'environnement simulé** et doit être recalibrée par la mesure sur des données industrielles réelles.

Le mode `update` a un coût réel — une même fenêtre est publiée plusieurs fois — que la Phase 3 s'était engagée
à mesurer avant toute optimisation. Le chiffre mesuré est dans `docs/10-phase-3-streaming.md` ; il n'a pas été
estimé.

Détail complet : `docs/adr/ADR-003-watermark-and-output-mode.md`.

### D-37 — Ne scorer qu'une fenêtre mature · `ACCEPTÉ`

**Défaut mesuré en Phase 3, puis corrigé.** En mode `update`, une fenêtre est publiée pendant qu'elle se
remplit, et le seuil d'admission n'exigeait que 30 échantillons alors que le modèle a été entraîné sur des
fenêtres complètes de 60.

**Options** : (A) continuer à scorer les fenêtres intermédiaires — (B) ne scorer qu'une fenêtre suffisamment
mature — (C) changer de modèle ou réentraîner sur des fenêtres partielles.

| Critère | A | **B** | C |
|---|---|---|---|
| Latence | premier score ~30 s avant `window_end` | **+9 s après `window_end`** (mesuré) | inchangée |
| Cohérence Phase 2 | rompue | **préservée** | nouveau modèle à revalider |
| Complexité Spark | nulle | **faible** : 2 agrégats internes | nulle |
| Volume Kafka | 5,67 émissions/fenêtre | **identique** | identique |
| Watermark | sans rapport | **orthogonal** | sans rapport |
| Données tardives | rescore, toujours partiel | **complète la fenêtre, qui devient scorable** | inchangé |
| Alertes | tempête | **1 alerte / 15 machines** (mesuré) | dépend du réentraînement |

**Retenu : B.** Seule option corrigeant la cause — donner au modèle une entrée qu'il n'a jamais vue — sans
toucher au modèle, au seuil ni au jeu de features. C rouvrirait la Phase 2 ; A est le défaut lui-même.

**La règle** : une fenêtre est mature quand sa couverture en temps d'événement s'étend sur toute la fenêtre,
aux **deux** extrémités, à la cadence propre de la machine —
`max(head_gap, tail_gap) ≤ mean_step × 2`. Une fenêtre immature est publiée avec `is_scored = false` et
`skip_reason = WINDOW_NOT_MATURE` : le contrat est inchangé.

**Maturité ≠ watermark**, et c'est le point central. Le watermark est une borne **globale** de retard, calculée
sur le temps d'événement le plus récent de **toutes** les machines, et il sert à évincer l'état. La maturité
est une propriété d'**une** fenêtre d'**une** machine, calculée sur son propre contenu. Attendre que le
watermark dépasse `window_end` coûterait 90 s à chaque fenêtre, mélangerait les machines, et — surtout —
**ne serait pas déterministe au rejeu**, puisque le watermark dépend de l'ordre d'arrivée. La règle retenue est
une fonction pure des agrégats de la fenêtre.

**Résultats mesurés** (même scénario, seul le code change) :

| | Avant | Après |
|---|---|---|
| Émissions anormales | 872 (64,7 %) | **2 (0,6 %)** |
| Alertes / machines | 16 / 15 | **1 / 1** |
| Fenêtres à 30–39 échantillons jugées anormales | **100,0 %** | *plus scorées* |
| Fenêtres complètes jugées anormales | 1,0 % | **0,7 %** |
| Fenêtres distinctes publiées | 540 | **540** |
| Latence du score | — | **+9,1 s** après `window_end` |

Une première version ne contrôlait que la **queue** : elle supprimait 93 % des émissions anormales et laissait
**15 alertes sur 15 machines**, toutes sur le même `window_start` avec 44 échantillons — les fenêtres
d'ouverture, tronquées au **début**. D'où le contrôle symétrique. Détail complet :
`docs/adr/ADR-004-window-maturity.md` et `docs/10-phase-3-streaming.md` § 5.7.

### D-38 — Trois tables plutôt que six · `ACCEPTÉ`

`docs/05` esquissait six tables. Deux d'entre elles — `model_version` et `drift_metric` — **n'ont aucun
producteur** en Phase 4, et `production_line` n'a aucun attribut propre : `LINE-A` est un code, rien d'autre.

**Retenu** : `machine` (avec `line_code` en colonne), `alert`, `alert_acknowledgement`. Une table vide est une
dette qui ressemble à une fonctionnalité.

Écartées aussi : `criticality`, `commissioned_on`, `nominal_ranges`. `fleet.yaml` ne contient que
`machine_id`, `line_id`, `profile`, `firmware_version`. La description du contrat prétend que la sévérité
dépend de la criticité de la machine ; c'est faux — `severity_for()` n'utilise que le score et le seuil.

Détail : `docs/adr/ADR-007-persistence-model.md`.

### D-39 — Auto-provisionnement d'une machine inconnue · `ACCEPTÉ`

**Options** : (A) clé étrangère stricte, alerte rejetée en DLQ — (B) créer la machine à la première alerte —
(C) clé étrangère nullable.

**Retenu : B.** A jette une **détection réelle** parce qu'un seed est périmé : c'est le mauvais mode de panne.
L'alerte porte `machine_id` et `line_id`, ce qui suffit à créer la ligne. `ON CONFLICT (code) DO NOTHING`,
parce que trois threads consommateurs peuvent rencontrer la même machine neuve au même instant.

### D-40 — Un seul topic de rebut · `ACCEPTÉ`

**Options** : (A) réutiliser `telemetry.dlq` — (B) un `alerts.dlq` dédié.

**Retenu : A.** L'enveloppe `DlqEnvelope` (ADR-002) porte déjà `source_topic` : elle a été conçue pour être
partagée. Filtrer sur `source_topic = 'alerts'` distingue les deux producteurs, et il n'y a qu'un endroit à
surveiller. Le contrat étant défini en Python, l'implémentation Java est vérifiée contre **le même JSON
Schema** — deux implémentations ne restent alignées que si un tiers les arbitre.

### D-09 — Contrôle de concurrence sur l'acquittement · `ACCEPTÉ`

Arbitré en Phase 4. **`expectedVersion` optionnel** dans le corps de la requête : absent, aucun contrôle ;
présent, un 409 si l'alerte a bougé entre-temps. Le rendre obligatoire alourdirait un tableau de bord qui agit
sur des lignes fraîchement chargées ; le supprimer laisserait l'échec silencieux où deux opérateurs agissent,
un seul compte, et aucun ne l'apprend.

### D-06 — Source du référentiel machines · `ACCEPTÉ`

Arbitré en Phase 4, et différemment de la recommandation initiale (lecture JDBC depuis Spark). Le référentiel
est **alimenté par le seed Flyway et complété par le flux** (D-39). Le job Spark n'a besoin d'aucun
référentiel : il ne lit que `machine_id` et `line_id`, qui voyagent déjà sur chaque message.

### D-41 — Source des courbes de télémétrie · `ACCEPTÉ`

**Options** : (A) pas de courbe capteur, seulement le score et les contributeurs — (B) persister les fenêtres
scorées et exposer une API dédiée — (C) faire porter les 52 features par l'alerte (modifie Spark).

**Retenu : B.** Le constat qui a tranché : `telemetry.scored` **contient réellement** les moyennes capteur par
fenêtre — vérifié sur un message du topic (`temperature_c_mean = 45.97`, `vibration_mm_s_mean = 1.13`, …) —
mais elles n'atteignaient ni PostgreSQL ni l'API. Sans B, toute courbe affichée aurait été **inventée**.

L'extension est **strictement limitée** : 5 moyennes capteur, le score, le drapeau d'anomalie et de quoi
distinguer une fenêtre partielle d'une fenêtre complète. Les 47 autres features ne sont pas stockées — le
dashboard trace cinq lignes, pas une matrice d'entraînement. **Spark n'est pas modifié.**

Volume mesuré : ~5 700 lignes/heure après déduplication (11 342 mises à jour de fenêtre pour 2 h de télémétrie
sur 15 machines). Écriture idempotente sur `(machine_id, window_start)`, purge planifiée.

### D-42 — CORS · `ACCEPTÉ`

Vérifié en direct : `OPTIONS /api/v1/alerts` avec `Origin: http://localhost:4200` renvoie **403**, sans aucun
en-tête `Access-Control-*`. **Retenu : reverse proxy** (nginx en production, `proxy.conf.json` en
développement). Le navigateur ne voit qu'une origine : le problème est **supprimé** au lieu d'être contourné,
et le backend n'est pas modifié.

⚠️ Le proxy SSE exige `proxy_buffering off` ; sans cela nginx retient le flux et le dashboard affiche
« connecté » sans jamais rien recevoir.

### D-43 — Bibliothèque de graphiques · `ACCEPTÉ`

**Chart.js 4 sans wrapper.** Comparaison factuelle des dépendances : ECharts + `ngx-echarts` (60,3 Mo
dépaquetés, peer `@angular/core >= 22`), Chart.js + `ng2-charts` (peer `>= 21` **et** `@angular/cdk`), Plotly
(5,6 Mo), D3 (tout à écrire). **Chart.js est le seul sans aucun peer Angular** (`peerDependencies` vide) : il
ne peut donc pas bloquer une montée de version Angular.

Mesuré : la page de détail est un chunk paresseux de 241,58 kB, hors du bundle initial (263,93 kB / 73,62 kB
transférés).

### D-44 — Gestion d'état · `ACCEPTÉ`

**Services + signals, pas de NgRx.** Trois pages et une seule source de vérité ne justifient pas actions,
reducers, effects et selectors au-dessus d'une `Map`.

### D-45 — `z_score` en snake_case · `ACCEPTÉ`

Vérifié sur la réponse réelle : `topContributors` est en camelCase mais son champ interne est `z_score`. Le
backend réutilise un DTO pour le message Kafka et la réponse REST, et le côté Kafka est en snake_case. **Le
modèle TypeScript le reproduit fidèlement** — le renommer ferait mentir le modèle sur le contrat réel.

### D-46 — `Last-Event-ID` · `ACCEPTÉ`

**Contrainte technique réelle** : `EventSource` **ne permet pas de définir d'en-têtes**. `Last-Event-ID` est
envoyé **par le navigateur**, **automatiquement**, et **uniquement lors d'une reconnexion**.

**Retenu : `EventSource` natif + resynchronisation REST.** Le navigateur couvre le trou court ; la requête REST
couvre le trou long, y compris au-delà des 500 événements rejoués par le backend. Écrire un parseur SSE sur
`fetch` pour contrôler l'en-tête reviendrait à réimplémenter reconnexion et découpage de trames pour un
problème que la base résout déjà.

La reconnexion native est **désactivée** au profit d'un backoff exponentiel applicatif : le navigateur réessaie
à intervalle fixe et martèlerait un backend en panne.

### D-47 — Version Angular · `ACCEPTÉ` — **Angular 21.2.23**, pas 22

La validation était conditionnelle (« si la compatibilité npm/build est confirmée »). Elle **ne l'est pas** :
le CLI Angular 22 exige Node `^22.22.3` et la machine exécute **v22.14.0**. Angular 21.2 exige `^22.12.0` et
construit proprement. C'est la version la plus récente réellement compatible avec la chaîne d'outils vérifiée.

Conséquence : Angular 21 génère des tests **Vitest + jsdom**, pas Karma/Jasmine. La chaîne fournie est utilisée
telle quelle.
