# 02 — Contrat de données

Les schémas font foi dans `contracts/json-schema/`. Ce document explique **pourquoi** ils ont cette forme.
Aucun composant ne redéfinit sa propre vision d'un événement : les trois langages (Python, Java, TypeScript)
dérivent leurs types de ces fichiers, générés ou vérifiés en CI.

## 1. Choix du format de sérialisation

### Décision : JSON UTF-8, avec `schema_version` obligatoire et JSON Schema comme contrat de référence

| Critère | JSON + JSON Schema | Avro + Schema Registry | Protobuf |
|---|---|---|---|
| Taille sur le fil | la plus grosse (~3 à 5× Avro, ordre de grandeur usuel, non mesuré ici) | compacte | la plus compacte |
| Compatibilité vérifiée automatiquement | non, discipline humaine + CI | **oui, par le registre** | non (par convention de numérotation) |
| Lecture d'un message en production | `kafka-console-consumer` suffit | outillage dédié obligatoire | outillage dédié obligatoire |
| Intégration PySpark | native (`from_json`) | **friction réelle, voir ci-dessous** | via protobuf 3.4+ ou lib tierce |
| Intégration Spring Kafka | native (Jackson) | bonne (serde Confluent) | bonne |
| Intégration Angular | native | nulle sans passerelle | nulle sans passerelle |
| Infrastructure ajoutée | aucune | un conteneur Schema Registry | aucune |

Le point qui tranche est le troisième depuis le bas. **Spark ne sait pas lire nativement le format de fil
Confluent** : un message Avro produit via Schema Registry est préfixé d'un octet magique et de l'identifiant de
schéma sur 4 octets, que `from_avro` ne comprend pas. Il faut soit découper les 5 premiers octets à la main et
résoudre le schéma soi-même, soit intégrer une bibliothèque tierce (ABRiS). C'est du code d'infrastructure qui
ne démontre rien sur le sujet du projet et qui devient une source de pannes.

À cela s'ajoute que le dashboard consomme in fine ces structures : un pivot binaire imposerait une conversion
supplémentaire côté Spring uniquement pour l'affichage.

**Le coût assumé** : rien n'empêche mécaniquement un producteur de publier un message incompatible. La
compatibilité repose sur une discipline outillée, pas sur un serveur. Je la rends contraignante par :

- validation du schéma en CI sur des messages d'exemple versionnés (`contracts/examples/`) ;
- un test de non-régression qui rejoue les exemples de la **v1** contre le parseur courant ;
- validation à l'entrée de chaque consommateur, les messages invalides partant en DLQ plutôt qu'en exception.

**Quand je basculerais sur Avro + Schema Registry** : plus d'une équipe productrice sur un même topic, ou un
débit où la bande passante et le stockage deviennent dimensionnants. À l'échelle visée ici (cible : quelques
dizaines de messages par seconde, voir `docs/00-domain-choice.md`), la compacité n'est pas un critère.

## 2. Stratégie d'évolution du schéma

Règle de compatibilité retenue : **BACKWARD**, au sens Confluent — un consommateur écrit pour le schéma N doit
pouvoir lire les messages produits en N-1. Concrètement, la discipline est :

**Autorisé sans nouvelle version majeure (incrément mineur de `schema_version`) :**
- ajouter un champ **optionnel** avec une valeur par défaut explicite ;
- ajouter une valeur à une énumération **si** les consommateurs ont un cas par défaut (`UNKNOWN`) — c'est une
  obligation d'implémentation, pas une option ;
- assouplir une contrainte (élargir un intervalle, augmenter une longueur maximale).

**Interdit sans version majeure :**
- renommer, supprimer ou changer le type d'un champ ;
- rendre obligatoire un champ jusque-là optionnel ;
- changer l'unité d'une grandeur (`temperature_c` en Fahrenheit serait un changement majeur silencieux, le pire
  de tous — c'est précisément pourquoi **l'unité fait partie du nom du champ**).

**Procédure de version majeure** : nouveau topic `telemetry.raw.v2`. Le producteur écrit sur les deux topics
pendant une fenêtre de transition, les consommateurs migrent un à un, puis `v1` est retiré. On ne mélange
jamais deux versions majeures dans un même topic : cela obligerait chaque consommateur à embarquer un routeur
de version, et l'offset ne dirait plus rien sur la sémantique.

**Politique côté consommateurs**, à appliquer partout :
- Spark : `StructType` explicite, jamais `inferSchema`. Un champ ajouté en amont est ignoré, un champ manquant
  devient `null` — donc un déploiement producteur ne casse pas le job.
- Spring : `FAIL_ON_UNKNOWN_PROPERTIES = false` sur l'`ObjectMapper` Kafka, plus Bean Validation sur les champs
  attendus.
- Angular : types TypeScript générés à partir du JSON Schema, jamais écrits à la main.

## 3. Modèle temporel : quatre horodatages distincts

C'est le point le plus souvent confondu, il est donc explicite dans le contrat.

| Champ | Posé par | Signification | Utilisé pour |
|---|---|---|---|
| `event_time` | le capteur (simulateur) | instant réel de la mesure | **fenêtrage, watermark, métier** |
| `ingest_time` | le producteur | instant d'envoi vers Kafka | mesure du retard côté source |
| horodatage Kafka | le broker | instant d'écriture dans le log | rétention, débogage |
| `scored_at` | Spark | instant de traitement | latence de bout en bout, SLA |

Toutes les dates sont en **ISO-8601 UTC avec millisecondes et suffixe `Z`**. Aucun fuseau local n'entre dans le
système : le fuseau est une préoccupation d'affichage, traitée exclusivement dans le dashboard.

## 4. `TelemetryRaw` — événement brut

```json
{
  "schema_version": "1.0",
  "event_id": "5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
  "machine_id": "M-014",
  "line_id": "LINE-A",
  "event_time": "2026-09-06T14:23:07.412Z",
  "ingest_time": "2026-09-06T14:23:07.610Z",
  "machine_state": "RUNNING",
  "firmware_version": "2.3.1",
  "readings": {
    "temperature_c": 72.48,
    "vibration_mm_s": 2.81,
    "pressure_bar": 5.12,
    "power_kw": 12.44,
    "rotation_rpm": 1478.2
  }
}
```

Points de conception :

- **`event_id` (UUIDv4)** est la clé d'idempotence de bout en bout. Sans lui, un doublon est indétectable.
- **`machine_state`** (`RUNNING` / `IDLE` / `STARTING` / `MAINTENANCE` / `STOPPED`) est indispensable : une
  vibration élevée pendant un démarrage est normale, la même en régime établi ne l'est pas. Sans ce champ, on
  fabrique des faux positifs à chaque changement de production. C'est ce qui rend certaines anomalies
  *contextuelles* et pas seulement ponctuelles.
- **Les unités sont dans le nom des champs.** `temperature_c`, jamais `temperature`.
- **`readings` est un objet imbriqué** et non des champs à plat : ajouter un capteur devient un ajout dans un
  objet dont les consommateurs projettent explicitement ce qu'ils connaissent.
- **Un capteur en panne produit `null`, pas `0`.** Zéro est une valeur physique valide ; confondre les deux
  fabrique des anomalies imaginaires. Le taux de valeurs nulles devient d'ailleurs lui-même une feature.
- **Aucune étiquette de vérité terrain dans ce message** — voir la section suivante.

## 5. `TelemetryLabel` — vérité terrain, topic séparé

```json
{
  "schema_version": "1.0",
  "event_id": "5c0a3f8e-6f5b-4a0e-9d1c-8f2b7a1e4c93",
  "machine_id": "M-014",
  "event_time": "2026-09-06T14:23:07.412Z",
  "is_anomaly": true,
  "anomaly_type": "BEARING_WEAR",
  "episode_id": "ep-20260906-M014-003",
  "episode_started_at": "2026-09-06T14:18:00.000Z",
  "source": "SIMULATOR",
  "emitted_at": "2026-09-06T14:23:07.500Z"
}
```

**Pourquoi un topic séparé plutôt qu'un bloc dans le message brut.** Deux raisons, une technique et une
conceptuelle :

1. **Impossibilité structurelle de fuite.** Si l'étiquette voyage dans le même message, il suffit d'un oubli
   dans une projection pour que le modèle apprenne sur la réponse. En la sortant du flux, la fuite devient
   impossible par construction, pas par vigilance.
2. **C'est le comportement réel.** En production, l'étiquette n'existe pas au moment de la mesure : elle arrive
   des jours plus tard, d'un rapport de maintenance ou du verdict d'un opérateur. Un flux d'étiquettes
   **en retard et partiel** est donc le modèle fidèle, et c'est exactement ce que le simulateur imite.

`episode_id` regroupe tous les événements d'une même occurrence d'anomalie. C'est ce qui permet de mesurer la
**latence de détection** (délai entre `episode_started_at` et la première alerte) et de compter les épisodes
détectés plutôt que les points — la métrique qui a du sens pour un exploitant.

Le `alert-service` produit lui aussi sur ce topic avec `source: "OPERATOR"` lorsqu'un opérateur qualifie une
alerte de faux positif. La boucle de feedback est ainsi fermée sans composant supplémentaire.

## 6. `ScoredEvent` — sortie du scoring

```json
{
  "schema_version": "1.0",
  "machine_id": "M-014",
  "line_id": "LINE-A",
  "window_start": "2026-09-06T14:22:10.000Z",
  "window_end": "2026-09-06T14:23:10.000Z",
  "scored_at": "2026-09-06T14:23:41.905Z",
  "sample_count": 58,
  "is_scored": true,
  "skip_reason": null,
  "machine_state": "RUNNING",
  "anomaly_score": 0.9962,
  "raw_score": -0.0731,
  "score_threshold": 0.995,
  "is_anomaly": true,
  "consecutive_windows": 2,
  "top_contributors": [
    { "feature": "vibration_mm_s_stddev", "z_score": 4.21 },
    { "feature": "power_per_rpm_mean", "z_score": 3.08 }
  ],
  "features": {
    "temperature_c_mean": 72.4,
    "vibration_mm_s_stddev": 0.94,
    "power_per_rpm_mean": 0.0084
  },
  "model": {
    "name": "isolation_forest",
    "version": "1.3.0",
    "trained_at": "2026-09-01T08:00:00.000Z",
    "artifact_sha256": "9f2c...a41b"
  },
  "processing_delay_ms": 31905
}
```

Points de conception :

- **La fenêtre remplace `event_id`** comme identité : l'unité scorée n'est plus un point mais un intervalle par
  machine. Le couple `(machine_id, window_start)` est la clé naturelle.
- **`is_scored` / `skip_reason`** : une fenêtre peut légitimement ne pas être scorée (`MACHINE_NOT_RUNNING`,
  `INSUFFICIENT_SAMPLES`, `MODEL_UNAVAILABLE`). On publie quand même l'événement. Un silence et un « tout va
  bien » ne doivent jamais se ressembler — sinon une panne du scoring ressemble à une usine en bonne santé.
- **`features` est publié.** C'est ce qui rend une alerte explicable six mois plus tard et ce qui permet de
  rejouer un cas litigieux hors ligne sans recalculer la fenêtre.
- **`raw_score` et `anomaly_score` coexistent** : le premier est la sortie native du modèle
  (`IsolationForest.score_samples`, non bornée et non comparable entre versions), le second est calibré dans
  `[0,1]` par ECDF (voir `docs/07-ml-methodology.md`). Garder les deux permet de comparer deux versions de
  modèle sans recalculer.
- **`processing_delay_ms` = `scored_at` − `window_end`.** C'est la latence de bout en bout observable, la seule
  qu'on affichera plus tard — mesurée, jamais estimée.

## 7. `Alert` — anomalie qualifiée

```json
{
  "schema_version": "1.0",
  "alert_id": "2c891855-4e2f-51f1-87e0-9103b5091c7c",
  "machine_id": "M-014",
  "line_id": "LINE-A",
  "severity": "HIGH",
  "status": "NEW",
  "anomaly_score": 0.9962,
  "score_threshold": 0.995,
  "detected_at": "2026-09-06T14:23:10.000Z",
  "window_start": "2026-09-06T14:22:10.000Z",
  "window_end": "2026-09-06T14:23:10.000Z",
  "published_at": "2026-09-06T14:23:41.930Z",
  "consecutive_windows": 2,
  "top_contributors": [
    { "feature": "vibration_mm_s_stddev", "z_score": 4.21 }
  ],
  "features": { "temperature_c_mean": 72.4, "vibration_mm_s_stddev": 0.94 },
  "model": { "name": "isolation_forest", "version": "1.3.0" }
}
```

Le point central : **`alert_id` est un UUIDv5 déterministe**, calculé par le `stream-processor` à partir de

```
namespace UUID fixe du projet + "machine_id|window_start|model_name|model_version"
```

Conséquence : si Spark rejoue un micro-batch après incident, il republie **exactement le même identifiant**.
La contrainte de clé primaire côté PostgreSQL absorbe le doublon sans qu'aucun composant n'ait besoin d'une
table de déduplication ni d'un cache. C'est le mécanisme qui transforme un pipeline at-least-once en un
comportement observable **effectively-once** (voir `docs/04-streaming-semantics.md`).

`detected_at` vaut `window_end`, c'est-à-dire un **temps d'événement**, pas l'heure d'écriture. Un opérateur
doit voir quand la machine a déraillé, pas quand notre serveur a fini son calcul.

`status` est présent avec la valeur `NEW` par cohérence de lecture, mais **le producteur ne pilote pas le cycle
de vie** : seul le `alert-service` fait évoluer ce champ après persistance.

## 8. Ce que le contrat ne contient volontairement pas

- **Pas de champ libre `metadata` ouvert** dans les messages : un fourre-tout non typé devient rapidement un
  schéma implicite non versionné.
- **Pas de valeur seuil dupliquée dans plusieurs messages sans sa version de modèle** : un seuil sans son
  modèle n'est pas interprétable.
- **Pas de texte destiné à l'affichage** (libellés, messages traduits). La présentation est une responsabilité
  du dashboard ; mettre des libellés dans le flux revient à figer l'IHM dans le contrat de données.
