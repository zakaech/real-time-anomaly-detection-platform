# Real-Time Anomaly Detection Platform

Plateforme temps réel de détection d'anomalies sur télémétrie de capteurs industriels : ingestion continue,
enrichissement, scoring par modèle non supervisé, alertes persistées et workflow d'acquittement opérateur.

**État : Phase 3 terminée** — infrastructure (Kafka, PostgreSQL, topics), bibliothèque partagée
`telemetry-core`, [`event-simulator`](event-simulator/) qui alimente `telemetry.raw` et `telemetry.labels`,
[`ml-training`](ml-training/) qui entraîne, évalue et sérialise le détecteur, et
[`stream-processor`](stream-processor/) qui score le flux en continu et publie `telemetry.scored` et
`alerts`. Les phases suivantes (service Spring Boot, dashboard Angular) restent à écrire.

## Pile technique

| Couche | Technologie |
|---|---|
| Ingestion | Apache Kafka 3.9 (KRaft, sans ZooKeeper) |
| Traitement de flux | Apache Spark Structured Streaming (PySpark) |
| Machine Learning | Python 3.11, scikit-learn, pandas, NumPy |
| Backend applicatif | Java 17, Spring Boot 3, Spring Kafka, Spring Data JPA |
| Persistance | PostgreSQL 16 |
| Frontend | Angular, TypeScript |
| Orchestration | Docker, Docker Compose |

## Démarrage

```sh
cp .env.example .env
docker compose --env-file .env -f infra/docker-compose.yml up -d
```

Le service `kafka-init` attend que le broker soit réellement prêt (`condition: service_healthy`, pas un simple
`depends_on`), puis crée les six topics et se termine. Vérification :

```sh
docker compose --env-file .env -f infra/docker-compose.yml ps -a
docker compose --env-file .env -f infra/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
```

Console d'exploration Kafka, optionnelle, sur `http://localhost:8085` :

```sh
docker compose --env-file .env -f infra/docker-compose.yml --profile tools up -d
```

## Produire des données

Le simulateur est derrière un profil : `docker compose up` laisse la pile passive, et la génération reste
un acte délibéré.

```sh
# temps réel, un échantillon par machine et par seconde
docker compose --env-file .env -f infra/docker-compose.yml --profile sim up -d event-simulator

# une heure d'historique, reproductible, aussi vite que possible
docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.main \
  --mode replay --duration-seconds 3600 --seed 4242
```

Vérifier ce qui est réellement arrivé dans les topics — l'outil décode avec les contrats et mesure la
distribution de retard dont la Phase 3 aura besoin pour régler son watermark :

```sh
docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.tools.inspect --topic telemetry.raw

docker run --rm --network anomaly-platform_anomaly-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  event-simulator python -m event_simulator.tools.inspect --topic telemetry.labels
```

## Entraîner le détecteur

```sh
docker build -f ml-training/Dockerfile -t ml-training .

# instantané borné de Kafka, identifié par son empreinte
docker run --rm --network anomaly-platform_anomaly-net   -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092   -v "$PWD/data":/workspace/data ml-training   python -m ml_training.cli export --destination ../data/run-01 --source-seed 424242

# entraînement, évaluation, artefact et rapport
docker run --rm -v "$PWD":/workspace -w /workspace/ml-training ml-training   python -m ml_training.cli train --dataset ../data/run-01
```

Les résultats mesurés sont dans [`ml-training/reports/model-report.md`](ml-training/reports/model-report.md),
généré depuis `results.json` par une exécution réelle — aucun chiffre n'y est saisi à la main.

## Scorer le flux en continu

```sh
docker build -f stream-processor/Dockerfile -t stream-processor .

# le job (trois requêtes : validation, scoring, alerting)
docker compose --env-file .env -f infra/docker-compose.yml --profile stream up -d stream-processor

# relire ce que les topics de sortie contiennent réellement
docker compose --env-file .env -f infra/docker-compose.yml --profile stream run --rm   stream-processor python -m stream_processor.tools.inspect_stream
```

`STREAM_LATE_DETECTION_ENABLED=false` pour un backfill : pendant un rejeu accéléré, chaque échantillon porte
par construction un retard de publication important, et tout l'historique partirait dans `telemetry.late`.

Les mesures d'exécution réelle sont dans [`docs/10-phase-3-streaming.md`](docs/10-phase-3-streaming.md), avec
leurs conditions. Ce sont des mesures **locales**, pas un benchmark.

Un défaut mesuré reste **ouvert** : en mode `update`, une fenêtre encore en cours de remplissage est scorée
alors que le modèle a été entraîné sur des fenêtres complètes, ce qui la fait classer anormale dans près de
100 % des cas contre 1,0 % pour une fenêtre complète. Options et coût en latence dans **D-37**
([`docs/09`](docs/09-open-decisions.md)). Il est documenté et chiffré plutôt que corrigé sans nouvelle mesure.

Vérifier la bibliothèque partagée (lint, format, typage strict, tests) :

```sh
docker build -f libs/telemetry-core/Dockerfile -t telemetry-core-dev .
docker run --rm -v "$PWD":/workspace -w /workspace/libs/telemetry-core telemetry-core-dev
```

Un `Makefile` regroupe ces commandes (`make up`, `make check`, `make clean`…) mais n'est pas obligatoire :
`make` n'est pas installé partout sous Windows, et chaque recette est une commande unique copiable telle quelle.

## Documentation de conception

| Document | Contenu |
|---|---|
| [00 — Choix du domaine](docs/00-domain-choice.md) | pourquoi la télémétrie industrielle plutôt que la fraude bancaire |
| [01 — Architecture](docs/01-architecture.md) | schéma des composants, diagramme de séquence, ADR-001 sur le lieu du scoring |
| [02 — Contrat de données](docs/02-data-contracts.md) | schémas des messages, choix du format, stratégie d'évolution |
| [03 — Topologie Kafka](docs/03-kafka-topology.md) | topics, partitionnement, clés, rétention, DLQ |
| [04 — Sémantique de streaming](docs/04-streaming-semantics.md) | temps d'événement, watermark, checkpointing, livraison, backpressure |
| [05 — Modèle de données](docs/05-data-model.md) | schéma PostgreSQL, index, idempotence de l'écriture |
| [06 — API REST](docs/06-rest-api.md) | endpoints, DTO, codes de statut, flux SSE, gestion d'erreurs |
| [07 — Méthodologie ML](docs/07-ml-methodology.md) | évaluation sans étiquettes, calibration du seuil, dérive |
| [08 — Dépôt et patterns](docs/08-repository-and-patterns.md) | structure du monorepo, patterns nommés et justifiés |
| [09 — Décisions ouvertes](docs/09-open-decisions.md) | arbitrages, avec options et recommandations |
| [10 — Pipeline Spark](docs/10-phase-3-streaming.md) | implémentation, résultats mesurés, incidents rencontrés |

Les décisions structurantes de la Phase 3 ont leur propre ADR :

| ADR | Sujet |
|---|---|
| [ADR-001](docs/adr/ADR-001-model-artifact-distribution.md) | distribution et vérification bloquante de l'artefact |
| [ADR-002](docs/adr/ADR-002-dead-letter-envelope.md) | contrat de l'enveloppe de rebut, et ce qui n'y va pas |
| [ADR-003](docs/adr/ADR-003-watermark-and-output-mode.md) | watermark 90 s et mode de sortie `update` |

Les contrats de messages font foi dans [`contracts/json-schema/`](contracts/json-schema/), et
[`contracts/examples/`](contracts/examples/) contient les messages d'exemple rejoués par les tests.

Chaque composant a son propre README : [`libs/telemetry-core/`](libs/telemetry-core/README.md),
[`event-simulator/`](event-simulator/README.md), [`ml-training/`](ml-training/README.md),
[`stream-processor/`](stream-processor/README.md).

## Sur les chiffres

Aucune métrique de performance n'est publiée tant qu'elle n'a pas été mesurée sur une exécution réelle.
Les valeurs présentes dans la documentation de conception sont des **paramètres de dimensionnement** ou des
**seuils de la littérature**, signalés comme tels. Les résultats mesurés iront dans `docs/benchmarks/`, avec
leur date et leurs conditions d'exécution.

## Configuration

`.env.example` est versionné, `.env` ne l'est jamais. Aucun nom d'hôte, port ou identifiant n'apparaît dans le
code : tout passe par des variables d'environnement, typées et validées au démarrage
([`telemetry_core.config`](libs/telemetry-core/src/telemetry_core/config.py)). Une adresse de broker ou un mot
de passe absent fait échouer le lancement, il n'a pas de valeur par défaut — une valeur par défaut *est* une
valeur en dur, qui reste simplement invisible jusqu'à ce qu'on se trompe de nom de variable.

## Propriétés d'ingénierie traitées explicitement

- Fenêtrage sur temps d'événement, watermarking, et gestion non silencieuse des données en retard
- Checkpointing Spark, reprise après panne, et compatibilité des évolutions de requête
- Sémantique de livraison : **at-least-once**, rendue **effectively-once** par idempotence au point de
  convergence — avec l'emplacement exact de chaque source de doublon
- Évaluation d'un détecteur non supervisé sans vérité terrain, et calibration du seuil par budget d'alertes
- Détection de dérive, en distinguant la dérive qui *est* le signal de celle qui invalide le modèle
- Backpressure : limitation de débit, signaux de saturation, et dégradation sélective
- Évolution du schéma des messages Kafka
