# ADR-008 — Persistance de la télémétrie pour les courbes du dashboard

- **Statut** : accepté (Phase 5)
- **Décisions liées** : D-41, D-37, ADR-005 (idempotence), ADR-006 (erreurs)

## Contexte

La Phase 5 demandait « des graphiques de télémétrie par machine, avec les anomalies marquées sur la courbe » et
« les valeurs des capteurs » sur le détail d'une alerte. L'audit préalable a montré que **rien ne permettait de
les afficher honnêtement** :

| Source | Contient des valeurs capteur ? | Atteignable par le dashboard ? |
|---|---|---|
| `telemetry.raw` (Kafka, 7 j) | oui, brut | ❌ non persisté, aucun endpoint |
| **`telemetry.scored` (Kafka, 3 j)** | **oui — les 52 features par fenêtre** | ❌ topic non consommé |
| `alert.features` | déclaré au contrat | ❌ **toujours vide** (le schéma d'alerting ne parse pas `features`) |
| PostgreSQL | non | aucune table |

Vérifié sur un message réel du topic :

```
is_scored=True machine=M-003 window=00:19:00..00:20:00 — 52 features
  temperature_c_mean = 45.97   vibration_mm_s_mean = 1.13
  pressure_bar_mean  = 12.20   power_kw_mean       = 6.10
  rotation_rpm_mean  = 1033.56 sample_count        = 60.0
```

**La donnée existait, mais nulle part où le dashboard pouvait la lire.** Toute courbe affichée aurait été
fabriquée.

## Options

| Option | Verdict |
|---|---|
| A — pas de courbe capteur : score + contributeurs uniquement | Honnête mais **ne satisfait pas l'exigence** ; et avec ~3 alertes par machine mesurées, une « courbe » de 3 points n'en est pas une |
| **B — consommer `telemetry.scored`, persister le nécessaire, exposer une API** | **Retenu** |
| C — faire porter les 52 features par l'alerte | **Modifie Spark** (le schéma d'alerting devrait parser une map de 52 clés à travers l'opérateur à état) et ne donne qu'**un point par alerte**, pas une courbe |
| D — relire Kafka à la demande | Ne fonctionne que dans la fenêtre de rétention et prétendrait récupérer une donnée non persistée |

## Décision

`alert-service` consomme `telemetry.scored` et persiste **le strict nécessaire** au dashboard :

- les **5 moyennes capteur** (une par signal physique) ;
- `anomaly_score`, `score_threshold`, `is_anomaly` — pour marquer les anomalies **sur** la courbe ;
- `sample_count`, `is_scored`, `machine_state`, `null_ratio` — pour distinguer une fenêtre partielle d'une
  fenêtre complète et **expliquer un trou** dans la courbe.

**Les 47 autres features ne sont pas stockées.** Le dashboard trace cinq lignes ; conserver une matrice
d'entraînement pour cela reviendrait à stocker une donnée que personne ne lit.

**Spark n'est pas modifié**, conformément à la contrainte.

### Idempotence

Écriture `ON CONFLICT (machine_id, window_start) DO UPDATE`. `telemetry.scored` est publié en **mode
`update`** : une fenêtre arrive plusieurs fois pendant qu'elle se remplit — **5,67 émissions par fenêtre** en
temps réel, 1,04 en backfill, toutes deux mesurées en Phase 3. Sans cette contrainte, le graphique
contiendrait cinq copies de chaque point. La dernière écriture gagne : c'est celle construite sur le plus
d'échantillons.

### Consommateur séparé

Groupe de consommateurs et conteneur d'écoute distincts, en **lots** plutôt qu'un message par transaction. Ce
topic porte environ **deux ordres de grandeur** de trafic de plus que `alerts` — 11 342 mises à jour de fenêtre
contre 25 alertes sur les mêmes deux heures mesurées — et **la file de l'opérateur ne doit jamais attendre
derrière des données de graphique**.

### Écart assumé vis-à-vis d'ADR-006

Une fenêtre impossible à parser est **abandonnée avec une ligne de log**, pas mise au rebut. Une lettre morte
existe pour être **rejouée par un humain** ; personne ne rejoue un point de courbe, et les envoyer dans
`telemetry.dlq` **enterrerait les rejets d'alertes** qui, eux, demandent une action. Les erreurs transitoires
restent réessayées : une panne de base ne perd rien.

### Fenêtres non scorées

Conservées. Elles portent de **vraies mesures**, et les masquer laisserait un trou inexpliqué dans la courbe.
La ligne enregistre `is_scored` et `sample_count` pour que le dashboard montre la différence au lieu de la
nier. Un capteur en panne est stocké **`NULL`, jamais `0`** — un zéro placerait une mesure plausible là où il
n'y en a eu aucune.

### Rétention

Croissance mesurée : **~5 700 lignes/heure**. Purge planifiée par âge (72 h par défaut). Pas de partitionnement
avec `DROP PARTITION` : c'est la bonne réponse à plusieurs millions de lignes et c'est prématuré ici.

## Conséquences

- Le dashboard trace des **courbes réelles** à la résolution de fenêtre que la donnée déclare elle-même.
- Ce qui reste impossible, et qui est dit dans l'interface : **il n'y a pas de trace capteur brute**.
  `telemetry.raw` a 7 jours de rétention et n'est persisté nulle part. La courbe est la **moyenne par fenêtre**
  calculée par le pipeline, et la légende le précise.
- Une valeur manquante casse la ligne (`spanGaps: false`) au lieu d'être comblée.

## Vérification

`TelemetryWindowTest` (9 tests, Testcontainers PostgreSQL) : effondrement des émissions répétées en une ligne,
fenêtres non scorées conservées, capteur manquant stocké `NULL`, ordre chronologique et plage appliqués en SQL,
troncature signalée, longueur de fenêtre lue depuis la donnée, machine inconnue auto-provisionnée, purge par
âge. `TelemetryControllerTest` (6 tests) pour l'API.

Vérifié sur la pile réelle : 15 machines, valeurs capteur réelles, 142 fenêtres anormales, et
`GET /api/v1/machines/M-003/telemetry` renvoyant 305 points scorés dont 12 anomalies au-dessus du seuil.
