# 08 — Structure du dépôt et patterns de conception

## 1. Monorepo

```
real-time-anomaly-detection-platform/
├── README.md
├── LICENSE
├── .gitignore
├── .editorconfig
├── .env.example                  # toutes les variables, aucune valeur secrète
├── Makefile                      # up, down, seed, train, test, lint
│
├── docs/
│   ├── 00-domain-choice.md
│   ├── 01-architecture.md
│   ├── 02-data-contracts.md
│   ├── 03-kafka-topology.md
│   ├── 04-streaming-semantics.md
│   ├── 05-data-model.md
│   ├── 06-rest-api.md
│   ├── 07-ml-methodology.md
│   ├── 08-repository-and-patterns.md
│   ├── 09-open-decisions.md
│   ├── adr/                      # ADR-001..N, une décision par fichier
│   └── benchmarks/               # RÉSULTATS MESURÉS uniquement, avec date et conditions
│
├── contracts/
│   ├── json-schema/              # source de vérité des messages
│   └── examples/                 # messages d'exemple par version, joués en CI
│
├── libs/
│   └── telemetry-core/           # paquet Python partagé
│       ├── pyproject.toml
│       └── src/telemetry_core/
│           ├── schemas.py         # dataclasses + (dé)sérialisation
│           ├── features.py        # DÉFINITION UNIQUE des features
│           ├── config.py          # chargement de configuration typé
│           └── logging.py         # journalisation JSON structurée
│
├── event-simulator/
│   └── src/event_simulator/
│       ├── machine_profile.py     # paramètres physiques par machine
│       ├── signal_generator.py    # signal nominal + bruit
│       ├── anomaly_injector.py    # injection, RNG indépendant du bruit
│       ├── producer.py            # publication Kafka
│       └── main.py
│
├── ml-training/
│   └── src/ml_training/
│       ├── dataset.py             # extraction depuis telemetry.raw
│       ├── train.py
│       ├── calibrate.py           # ECDF + choix du seuil
│       ├── evaluate.py            # métriques + comparaison aux baselines
│       ├── baselines.py           # 3-sigma, Mahalanobis
│       └── artifact.py            # écriture et validation de l'artefact
│
├── stream-processor/
│   └── src/stream_processor/
│       ├── session.py             # construction de la SparkSession
│       ├── source.py              # lecture Kafka + parsing + DLQ
│       ├── enrichment.py          # broadcast join du référentiel
│       ├── windowing.py           # fenêtrage et agrégation
│       ├── scoring.py             # pandas_udf + chargement du modèle
│       ├── alerting.py            # seuil, hystérésis, sévérité, alert_id
│       ├── sinks.py               # foreachBatch multi-sorties
│       ├── monitoring.py          # StreamingQueryListener
│       └── main.py
│
├── alert-service/                # Maven, Java 17, Spring Boot 3
│   └── src/main/java/com/anomalyplatform/alertservice/
│       ├── domain/               # entités, énumérations, règles, exceptions métier
│       ├── application/          # services applicatifs, ports
│       ├── infrastructure/
│       │   ├── kafka/            # consumer, producer d'étiquettes, DTO d'entrée
│       │   ├── persistence/      # repositories JPA + upsert natif
│       │   └── sse/              # registre d'émetteurs
│       ├── web/                  # contrôleurs, DTO de sortie, mappers, advice
│       └── config/
│
├── dashboard/                    # Angular
│   └── src/app/
│       ├── core/                 # services HTTP/SSE, intercepteurs, modèles générés
│       ├── features/alerts/      # liste, détail, acquittement
│       ├── features/live/        # vue temps réel
│       └── shared/
│
├── infra/
│   ├── docker-compose.yml
│   ├── docker-compose.cluster.yml
│   ├── kafka/create-topics.sh
│   ├── postgres/init/
│   └── observability/
│
└── .github/workflows/ci.yml
```

### Pourquoi un monorepo

Le système est un **produit unique dont les composants évoluent ensemble**. Ajouter un capteur touche
simultanément le contrat, le simulateur, les features, l'entraînement, le traitement, le schéma SQL et
l'affichage. En multi-dépôts, cela devient sept pull requests à coordonner, sans instant où l'ensemble est
cohérent et testable.

Le monorepo permet un **changement atomique** : une PR, une revue, un état cohérent. Il rend aussi possible le
partage de `libs/telemetry-core`, qui est le mécanisme anti-décalage entraînement/service décrit dans
`docs/07-ml-methodology.md` §2.3.

**Coût assumé** : les images Docker doivent avoir la **racine du dépôt** comme contexte de build pour accéder à
`libs/`, ce qui ralentit les builds. On le compense par `.dockerignore` et un ordonnancement des couches. La CI
utilise un filtrage par chemin pour ne rejouer que les jobs concernés.

## 2. Configuration : rien en dur

**Règle : aucun nom d'hôte, port, identifiant ou seuil n'apparaît dans le code.** Tout vient de variables
d'environnement, avec des valeurs par défaut adaptées au développement uniquement lorsqu'elles ne sont pas
sensibles.

`.env.example` est versionné, `.env` ne l'est jamais (`.gitignore`), et un secret réel n'entre jamais dans
l'historique Git — l'historique étant immuable, un secret commité doit être considéré comme compromis même
après suppression.

| Composant | Mécanisme |
|---|---|
| Python | `pydantic-settings` — configuration **typée et validée au démarrage** |
| Spring Boot | `@ConfigurationProperties` + `@Validated` |
| Angular | fichier de configuration lu au démarrage, pas de valeur figée au build |

Le point commun est la **validation au démarrage** : une configuration absente ou incohérente doit faire
échouer le lancement immédiatement, avec un message explicite. Découvrir un port erroné au premier message
traité, en production, coûte infiniment plus cher.

## 3. Journalisation structurée

JSON sur `stdout` dans tous les composants, avec un socle de champs commun :

```json
{
  "timestamp": "2026-09-06T14:23:41.905Z",
  "level": "INFO",
  "service": "stream-processor",
  "version": "0.4.1",
  "event": "alert_published",
  "machine_id": "M-014",
  "alert_id": "0b6d9a6f-...",
  "anomaly_score": 0.9962,
  "trace_id": "3f7a1c9e2b4d6081"
}
```

Trois principes :

1. **Champs structurés, pas de phrases interpolées.** `logger.info("alert published for %s", machine)` est
   non filtrable sans expression régulière fragile ; un champ `machine_id` est requêtable directement.
2. **`trace_id` propagé de bout en bout**, transporté en en-tête Kafka et en MDC Spring, et renvoyé dans chaque
   réponse d'erreur HTTP.
3. **`stdout` uniquement**, jamais de fichier : c'est le contrat des conteneurs, et la collecte est la
   responsabilité de l'orchestrateur.

Python : `structlog`. Java : Logback + `logstash-logback-encoder`. Spark : un `StreamingQueryListener` qui
journalise les métriques de chaque batch dans ce même format.

## 4. Patterns de conception, nommés et justifiés

### 4.1 Patterns d'intégration (Hohpe & Woolf)

| Pattern | Où | Pourquoi |
|---|---|---|
| **Publish-Subscribe** | topics Kafka | plusieurs consommateurs indépendants sur `telemetry.raw` (streaming, rejeu d'entraînement) sans que le producteur les connaisse |
| **Idempotent Receiver** | upsert sur `alert_id` | seul moyen de rendre l'at-least-once inoffensif (`docs/04` §4.3) |
| **Dead Letter Channel** | `telemetry.dlq` | un message illisible ne doit ni bloquer le flux ni disparaître |
| **Content Enricher** | broadcast join du référentiel | la télémétrie ne transporte que l'identifiant machine ; les métadonnées sont ajoutées en chemin |
| **Message Translator** | mappers DTO ↔ entité ↔ message | frontière explicite entre modèles externes et interne |
| **Datatype Channel** | un topic = un type de message | un topic mixte obligerait chaque consommateur à router |
| **Transactional Outbox** | *envisagé, non retenu en v1* | la publication d'étiquette après commit peut se perdre (`docs/06` §2.5) |

### 4.2 Côté Python

| Pattern | Où | Pourquoi |
|---|---|---|
| **Strategy** | `AnomalyDetector` : `IsolationForestDetector`, `MahalanobisDetector`, `ThreeSigmaDetector` | l'évaluation compare des algorithmes derrière une interface unique ; sans cela, chaque comparaison serait un `if` de plus dans le code de scoring |
| **Strategy** | `AnomalyInjector` : un injecteur par type d'anomalie | ajouter un type d'anomalie ne modifie aucun code existant |
| **Factory Method** | `create_detector(config)` | l'algorithme est un paramètre de configuration, pas un `import` figé |
| **Singleton paresseux** | chargement du modèle par exécuteur Spark | sans lui, l'artefact serait désérialisé à **chaque appel** de `pandas_udf` ; avec lui, une fois par processus exécuteur |
| **Template Method** | `StreamingJob` : `read()` → `transform()` → `write()` | fixe le cycle de vie commun (session, checkpoint, arrêt propre) |
| **Builder** | construction des événements | des dataclasses immuables avec valeurs par défaut évitent les messages partiellement remplis |
| **Repository** | `TelemetryDatasetRepository` | l'entraînement ignore si les données viennent de Kafka ou de Parquet |

Le **Singleton paresseux** mérite un mot : c'est un choix contraint par Spark. Un modèle chargé au niveau du
pilote et capturé par la closure serait sérialisé et transmis à chaque tâche. On le charge donc dans une
variable de module, protégée par un verrou, initialisée au premier appel dans chaque processus exécuteur.

### 4.3 Côté Java / Spring Boot

Architecture en couches, **dépendances dirigées vers le domaine** :

```
web ──▶ application ──▶ domain ◀── infrastructure
```

Le `domain` ne dépend de rien : ni de Spring, ni de JPA, ni de Kafka. C'est ce qui permet de tester les règles
de transition d'état sans démarrer de contexte Spring — un test unitaire en millisecondes plutôt qu'un test
d'intégration en secondes.

| Pattern | Où | Pourquoi |
|---|---|---|
| **State** | `AlertStatus` porte ses transitions autorisées | la règle « on ne clôture pas une alerte non acquittée » vit à un seul endroit, pas dispersée dans les contrôleurs |
| **Repository** | Spring Data JPA | abstraction de la persistance, avec `@Query` natif pour l'upsert |
| **DTO + Mapper** | MapStruct, `web/dto` | une entité JPA exposée en JSON couple le contrat HTTP au schéma SQL et expose les relations paresseuses |
| **Facade / Application Service** | `AlertApplicationService` | orchestration transaction + persistance + notification, laissant les contrôleurs sans logique |
| **Observer** | `ApplicationEventPublisher` → `SseEmitterRegistry` | le service de persistance ignore l'existence du SSE ; ajouter un canal (e-mail, webhook) n'y touche pas |
| **Anti-Corruption Layer** | `kafka/dto` distinct du domaine | un changement de contrat Kafka ne se propage pas dans le domaine |
| **Chain of Responsibility** | `@RestControllerAdvice` | traduction centralisée exception → `ProblemDetail` |

L'**Observer** via `ApplicationEventPublisher` est important pour la testabilité : le service publie
`AlertCreatedEvent` et s'arrête là. Un test vérifie que l'événement est publié, sans monter de connexion SSE.
L'écouteur, lui, est `@TransactionalEventListener(phase = AFTER_COMMIT)` — une notification émise avant le
commit annoncerait au dashboard une alerte qui n'existera peut-être jamais.

### 4.4 Côté Angular

| Pattern | Où | Pourquoi |
|---|---|---|
| **Facade** | `AlertFacade` | les composants ne connaissent ni HTTP ni SSE, seulement des observables |
| **Observer** | RxJS | le modèle naturel pour fusionner un flux SSE et des requêtes REST |
| **Smart / Presentational** | conteneurs vs composants d'affichage | les composants de présentation sont testables sans injection de service |
| **Interceptor** | `HttpInterceptor` | corrélation `trace_id` et traitement centralisé des `problem+json` |

## 5. Conventions Git

- **Anglais** pour le code, les noms, les commits et les branches. Le français reste dans `docs/`, destiné
  à la soutenance.
- **Conventional Commits** : `feat(stream-processor): add sliding window feature builder`. Un préfixe de
  composant rend l'historique lisible dans un monorepo.
- Branches : `feat/…`, `fix/…`, `docs/…`, `chore/…`.
- Un commit = un changement cohérent qui laisse le dépôt dans un état fonctionnel.

## 6. Ordre de construction proposé

Chaque étape produit quelque chose d'observable — pas de composant écrit « à l'aveugle » pendant des heures.

| # | Composant | Livrable vérifiable |
|---|---|---|
| 1 | `infra` + `libs/telemetry-core` | Kafka et PostgreSQL démarrent, topics créés, schémas validés en test |
| 2 | `event-simulator` | messages lisibles sur `telemetry.raw` avec `kafka-console-consumer` |
| 3 | `ml-training` | artefact versionné + rapport d'évaluation **mesuré** vs baselines |
| 4 | `stream-processor` | alertes visibles sur `alerts`, checkpoint qui survit à un `docker restart` |
| 5 | `alert-service` | alertes en base, API interrogeable, doublon Kafka sans effet |
| 6 | `dashboard` | alerte affichée en direct, acquittement fonctionnel |
| 7 | mesures + durcissement | `docs/benchmarks/` rempli, test de charge, test de panne |

L'étape 3 avant l'étape 4 est délibérée : sans artefact évalué, le `stream-processor` n'aurait rien à charger,
et on écrirait le scoring en supposant une interface de modèle plutôt qu'en s'appuyant sur celle qui existe.
