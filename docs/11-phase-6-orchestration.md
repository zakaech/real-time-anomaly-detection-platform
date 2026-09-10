# 11 — Phase 6 : orchestration, documentation, lancement

Ce document décrit ce qui a été construit en Phase 6, **ce qui a été réellement
exécuté**, et ce qui ne l'a pas été.

> Les chiffres proviennent d'exécutions réelles sur cette machine. Les mesures
> de plateforme sont **locales** : une machine, un broker, `local[2]`. Ce ne sont
> pas des benchmarks.

## 1. Le point de départ : un audit, pas une refonte

La Phase 6 demandait des Dockerfiles, un compose unique, des healthchecks, un
bootstrap et de la documentation. L'audit a montré que **l'essentiel existait
déjà** :

| Objectif | État trouvé |
|---|---|
| Dockerfile par composant | 6 présents |
| Multi-étages Java / Angular | déjà fait |
| Compose unique | 8 services |
| Healthchecks + `depends_on` | `service_healthy`, `service_completed_successfully` |
| Création des topics | script **déjà idempotent et convergent** |
| Migration de schéma | Flyway V1/V2/V3 |
| CI | 8 jobs |

La Phase 6 n'a donc pas reconstruit ces éléments. Recréer une création de topics
ou un chemin SQL parallèle aurait produit une **seconde source de vérité**, et
une seconde source de vérité diverge. Le travail réel a porté sur six manques
mesurés.

## 2. Le point bloquant : la plateforme ne démarrait pas depuis un clone

L'objectif « lancement en une commande » était **factuellement faux**.

`model.joblib` était exclu par `.gitignore`. Le `stream-processor` monte
`../ml-training/artifacts` en lecture seule et lit
`STREAM_ARTIFACT_DIR=/models/one_class_svm/2.0.0` : sur un clone neuf, le
répertoire ne contenait pas le binaire et le job **refusait de démarrer** — ce
qui est le comportement correct (ADR-001), pas un bug.

Le fait décisif : **l'artefact n'est pas reconstructible depuis le dépôt**. Le
ré-entraîner exige un jeu de 2 591 998 lignes lui-même non versionné.

Décision **D-48 / ADR-009** : versionner le fichier, tel quel.

| Vérification | Résultat |
|---|---|
| Taille | 222 347 octets |
| SHA-256 disque | `0f608323f944e63d30b80cc91418c10b3cfb1d3fc46bac184d3591c03a8d0aa5` |
| SHA-256 du blob git | **identique** |
| SHA-256 dans `metadata.json` | **identique** |
| SHA-256 après clone neuf | **identique** |
| `elliptic_envelope/model.joblib` | **toujours ignoré**, vérifié par `git check-ignore` |

Le modèle n'a été **ni ré-entraîné, ni recalibré, ni modifié**.

`*.joblib binary` a été ajouté à `.gitattributes`. L'heuristique texte/binaire de
git est une supposition, et une seule conversion CRLF sur un checkout Windows
aurait changé l'empreinte — faisant échouer le job **sur la machine du
relecteur, jamais sur la nôtre**. Le clone neuf confirme que la protection tient.

**Effet secondaire mesuré** : trois tests de `stream-processor` qui étaient
`SKIPPED` faute d'artefact s'exécutent désormais réellement, dont celui qui
retourne un seul bit du pickle et vérifie que le job le refuse.

```
avant : 94 passed, 3 skipped
après :  9 passed  (tests/test_model.py seul, 0 skipped)
```

Le message de skip affirmait « the binary is a build output and is not
committed » : devenu faux, il a été corrigé — dans le test **et** dans le
commentaire CI qui répétait la même chose.

## 3. Bootstrap : vérifier, ne rien créer

`scripts/bootstrap.sh` **ne crée rien**. C'est sa propriété centrale.

| Domaine | Ce que le bootstrap fait | Pourquoi pas plus |
|---|---|---|
| Topics Kafka | vérifie présence **et nombre de partitions** | `kafka-init` est déjà convergent |
| Schéma | vérifie `flyway_schema_history` en **lecture seule** | Flyway est la source unique de vérité |
| PostgreSQL | interroge la **readiness d'alert-service** | elle est `DOWN` pendant la migration : plus fort que `pg_isready` |
| Artefact ML | 5 fichiers + SHA-256 | c'est le seul contrôle que ni Kafka ni Flyway ne peuvent faire |

Il n'exécute **aucun SQL modifiant quoi que ce soit**.

### Les chemins d'échec, testés

Un contrôle qui ne passe que quand tout va bien n'est pas un contrôle. Les trois
modes d'échec ont été provoqués :

| Cas provoqué | Résultat |
|---|---|
| `STREAM_ARTIFACT_DIR` inexistant | sortie **1**, message + liste des modèles disponibles |
| Un octet ajouté à `model.joblib` | sortie **1**, empreintes attendue/calculée affichées, cause CRLF expliquée |
| `calibration.json` supprimé | sortie **1**, fichier manquant nommé, renvoi à ADR-001 |

## 4. Trois défauts que seule l'exécution réelle a révélés

Le bootstrap a été écrit, relu, passé à `shellcheck` — puis exécuté contre la
pile réelle. Il a rapporté **cinq échecs sur une plateforme parfaitement saine**.
Les trois causes sont instructives, et aucune n'était visible à la lecture.

### 4.1 Git Bash réécrit les chemins absolus

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh …
```

Git Bash convertit tout argument ressemblant à un chemin Unix absolu avant de le
passer à un `.exe` natif. `docker.exe` recevait donc
`C:/Program Files/Git/opt/kafka/bin/kafka-topics.sh`. La commande ne trouvait
rien, et le script concluait « broker injoignable » — **accusant la plateforme
d'un problème de quoting sur l'hôte**.

Correction : `MSYS_NO_PATHCONV=1` et `MSYS2_ARG_CONV_EXCL='*'`, inertes sous
Linux et en CI.

### 4.2 `docker compose exec` consomme stdin

La boucle sur les topics était un `while read` alimenté par un tube. Le premier
`docker compose exec` du corps a **avalé les lignes restantes** : le script a
vérifié **un** topic au lieu de six, sans rien signaler d'anormal.

C'est exactement la classe de défaut déjà rencontrée en Phase 3 (`docker cp` et
stdin, docs/10 §6.12).

Correction : une boucle `for` — ni sous-shell, ni tube, donc les compteurs
d'échec fonctionnent aussi — plus `</dev/null` sur l'appel, pour que la
régression soit impossible à réintroduire.

### 4.3 Un booléen PostgreSQL ne se lit pas `t` une fois concaténé

```sql
SELECT version || '|' || success   -- rend "1|true"
```

Le `t` / `f` affiché par `psql` est sa **forme d'affichage** d'une colonne
booléenne. Concaténé dans une chaîne, PostgreSQL rend `true` / `false`. Comparer
à `t` déclarait donc **les trois migrations en échec** sur une base parfaitement
migrée.

Correction : `success::text`, comparaison à `true` (et `t` accepté par sécurité).

> Ces trois défauts partagent une propriété : ils produisaient des **faux
> négatifs**. Un outil de vérification qui accuse à tort est plus nuisible qu'un
> outil absent, parce qu'on finit par cesser de le croire.

### 4.4 Le rejeu de démonstration était un backfill sans le dire

Le quatrième défaut n'était pas dans le bootstrap mais dans `demo.sh`, et il
aurait rendu la démonstration **vide** — sans jamais signaler d'erreur.

Mesuré sur la première exécution complète :

| Topic | Messages |
|---|---|
| `telemetry.raw` | 108 359 |
| **`telemetry.late`** | **107 055** (98,8 %) |
| `telemetry.scored` | **0** |
| `alerts` | **0** |

La cause est une conséquence exacte de la sémantique documentée en Phase 3. Le
retard vaut `ingest_time - event_time`, c'est-à-dire **le temps qu'un producteur
a gardé un échantillon avant de le publier**. En rejeu, le simulateur publie
*maintenant* des événements dont le temps d'événement remonte à deux heures : le
retard mesuré atteint 7 200 s, très au-delà du watermark de 90 s.

Et la branche de scoring ne lit que `on_time` :

```python
on_time, _too_late = split_late(accepted, settings)
aggregated = aggregate_windows(prepare_signals(on_time), …)
```

Presque tout le backfill était donc **exclu du scoring**. Aucune fenêtre, aucune
alerte, aucune courbe — et pas la moindre erreur dans les journaux.

Le projet avait déjà écrit la réponse, dans `.env.example` et dans la docstring
de `split_late` : *« Off for a backfill: producer lag is meaningless when the
producer is replaying history. »* `demo.sh` ne l'appliquait simplement pas.

Correction : le script démarre le job avec `STREAM_LATE_DETECTION_ENABLED=false`,
et **dit pourquoi** dans sa sortie plutôt que de le faire discrètement. La
contrepartie est énoncée : pendant la démonstration, un événement réellement en
retard n'est pas routé vers `telemetry.late`. Ce canal est vérifié séparément en
Phase 3, avec un message construit à 300 s de retard et mesuré à 300,0 s
exactement (docs/10 §5.5).

> Ce défaut est le meilleur argument en faveur de l'exigence de test réel. Le
> script passait `shellcheck`, la pile était saine, les neuf premières
> vérifications étaient vertes — et la démonstration n'aurait rien montré.

## 5. Durcissement des images

| Image | Utilisateur | Healthcheck | Étages |
|---|---|---|---|
| `alert-service` | `alert` (999) | **ajouté** (readiness) | 2 |
| `dashboard` | **`dashboard` (100)** | présent | 2 |
| `event-simulator` | **`simulator`** | — | 1 |
| `stream-processor` | **`spark`** | — | 1 |
| `ml-training` | root (assumé) | — | 1 |
| `libs/telemetry-core` | root (assumé) | — | 1 |

Le Dockerfile du dashboard **créait** un utilisateur `dashboard` sans jamais
émettre `USER` : l'utilisateur existait et ne servait à rien.

### Le piège du cache Ivy

L'utilisateur `spark` est créé **avant** le préchargement du cache Ivy, et cet
ordre porte la correction. Spark résout ses jars sous `${user.home}/.ivy2` :
précharger en root les aurait laissés dans `/root`, illisible par l'utilisateur
d'exécution. Le job aurait alors tenté de joindre Maven au démarrage — la
dépendance réseau que ce préchargement existe précisément pour supprimer.

Vérifié dans l'image finale :

```
uid=999(spark) gid=999(spark)   HOME=/home/spark
/home/spark/.ivy2  →  org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.3.jar …
checkpoints writable
```

`/checkpoints` est créé dans l'image avec cette propriété pour que le volume
nommé monté par-dessus en hérite. Sans cela Docker crée le point de montage en
root et le job ne peut plus checkpointer, ce qui **annule la garantie de reprise**
de docs/04.

### Deux images restent en root, et c'est documenté

`ml-training` et `libs/telemetry-core` sont des images d'**outillage**, exécutées
avec le dépôt monté depuis l'hôte, et elles écrivent dans ce montage (artefacts,
rapports, caches ruff/mypy/pytest). Un utilisateur conteneur dont l'uid ne
correspond pas au propriétaire hôte ne peut pas y écrire : abandonner root
casserait l'image dans son travail réel. Aucune des deux n'écoute sur un port.

### Pas de multi-étages Python, et c'est vérifiable

Aucune image Python n'installe de dépendance de compilation. Le seul `apt-get`
du dépôt installe `openjdk-17-jre-headless` et `procps` dans `stream-processor`
— du **runtime**, pas du build. Il n'y a donc rien à jeter entre un étage de
build et un étage d'exécution : un multi-étages ajouterait de la complexité pour
un gain nul. C'est écrit dans les Dockerfiles plutôt que fait pour cocher une
case.

## 6. `.env.example` : deux variables mortes retirées

| Variable | Diagnostic |
|---|---|
| `KAFKA_EXTERNAL_BOOTSTRAP` | référencée **nulle part** |
| `POSTGRES_PORT` | **pire que morte** : l'URL JDBC porte le port en dur, donc la modifier ne faisait rien tout en donnant l'impression du contraire |

`scripts/check_env_example.py` compare les variables déclarées à celles
interpolées par compose, dans les deux sens, et échoue sur un écart. Une
variable morte ne peut plus réapparaître silencieusement.

```
declared in .env.example : 48
used by docker-compose   : 48
OK    every declared variable is used, and every used one is declared.
```

La seconde direction compte autant : `docker compose config` n'échoue que sur
une variable **sans valeur par défaut**. Un `${FOO:-default}` ajouté à compose et
oublié dans le modèle passe la validation et tourne sur son défaut pour toujours.

## 7. OpenAPI généré, et vérifié contre les routes

springdoc 2.6.0, généré depuis les contrôleurs (D-50). Six opérations, ni plus
ni moins.

Le test central ne compare pas le document à une liste écrite à la main — une
liste n'est qu'un second document à oublier. Il le compare à la **table de
routage de Spring** :

> tout chemin documenté est routable, et tout chemin routable est documenté.

Un chemin documenté mais absent enverrait un client vers un 404 ; un chemin réel
non documenté rendrait la documentation silencieusement incomplète. Ni l'un ni
l'autre ne survit au build.

Le bean a dû être qualifié par son nom : l'actuator fournit un **second**
`RequestMappingHandlerMapping`, et injecter par type seul empêchait le contexte
de démarrer.

### La limite de SSE, énoncée

OpenAPI 3.1 sait déclarer `text/event-stream` comme média, et rien de plus. Le
protocole d'événements — `alert.created`, `alert.updated`, `heartbeat`,
`id: event_seq`, `Last-Event-ID`, le heartbeat de 15 s — n'a **aucun vocabulaire**
dans la spécification. Il est décrit **en prose** dans la description de
l'opération, et un test vérifie que cette prose est présente. La modéliser par un
schéma produirait un document qui valide et qui ment.

## 8. Ce qui a réellement été exécuté

### Suites de tests

| Composant | Résultat | Portes |
|---|---|---|
| `libs/telemetry-core` | **166 passed** | ruff, format, mypy strict, pytest |
| `event-simulator` | **140 passed** | ruff, format, mypy strict, pytest |
| `ml-training` | **73 passed** | ruff, format, mypy strict, pytest |
| `stream-processor` | **97 passed, 0 skipped** (dépôt monté) — 94 + 3 sans l'artefact | ruff, format, mypy strict, pytest |
| `alert-service` | **63 passed** dont les **7 nouveaux tests OpenAPI** | `mvn verify`, Testcontainers PostgreSQL |
| `dashboard` | **29 passed** | Prettier, Vitest + jsdom, build de production |
| **Total** | **568** | |

`shellcheck` passe sur les quatre scripts shell du dépôt. `docker compose config`
résout. Les quatre images de compose construisent.

### Test sur clone Git propre

Cloné dans un répertoire temporaire, depuis le commit `434c69e`.

> Note : le chemin du répertoire de travail habituel dépassait `MAX_PATH` sous
> Windows et faisait échouer `git clone` sur un fichier de commit-graph. Le clone
> a été fait dans un chemin court. C'est une contrainte de la plateforme hôte,
> pas du dépôt.

| # | Vérification | Résultat |
|---|---|---|
| 1 | Les 5 fichiers de l'artefact présents | **oui** |
| 2 | SHA-256 conforme à `metadata.json` | **oui**, identique après clone |
| 3 | Le bootstrap fonctionne | **oui** (après les 3 corrections de §4) |
| 5 | Kafka initialisé | **oui** |
| 6 | Topics aux bonnes partitions | **6/6/3/3/1/1**, conformes |
| 7 | PostgreSQL démarre | **oui**, healthy |
| 8 | Flyway applique V1/V2/V3 | **oui**, `success=true` pour les trois |
| 9 | `alert-service` healthy | **oui**, readiness `UP` |

La pile a été construite et démarrée **depuis le clone** : les conteneurs portent
`com.docker.compose.project.working_dir` pointant sur le répertoire temporaire,
et non sur le dépôt de travail.

Le durcissement a été vérifié sur les conteneurs **en marche**, pas seulement
dans les images :

```
anomaly-dashboard      uid=100(dashboard) gid=102(dashboard)
anomaly-alert-service  uid=999(alert)     gid=999(alert)
```

nginx exécute son maître **et** ses workers en tant que `dashboard`, répond `ok`
sur `/healthz`, et l'entrypoint a bien réécrit `assets/config.json` sans droits
root.

## 9. `make demo` exécuté, depuis zéro

Volumes supprimés (`down -v`), puis exécution complète depuis le clone.

> `make` **n'est pas installé** sur cette machine — ce qui est précisément
> pourquoi le README donne désormais l'équivalent direct de chaque cible. La
> recette a été vérifiée avec GNU make dans un conteneur (`make -n demo` rend
> bien `./scripts/demo.sh`), et c'est ce script qui a été exécuté pour de vrai.

```
Plateforme prete en 391s.
  alertes persistees          : 2
  fenetres de telemetrie      : 1917
DEMO EXIT=0
```

| Étape | Mesure |
|---|---|
| Rejeu de 7 200 s simulées | **78 s réels**, graine 424242 |
| Messages produits dans `telemetry.raw` | 108 000 |
| Durée totale jusqu'aux premières alertes | **391 s** |
| `telemetry.late` (avant correctif §4.4) | 107 055 |
| **`telemetry.late` (après)** | **0** |

Le flux a continué de tourner pendant les vérifications suivantes ; les
compteurs augmentent donc d'une mesure à l'autre, ce qui est le comportement
attendu d'une plateforme en marche.

### Les 17 points de vérification

| # | Vérification | Preuve observée |
|---|---|---|
| 1 | 5 fichiers de l'artefact | présents dans le clone |
| 2 | SHA-256 conforme | `0f608323…a8d0aa5`, identique disque / git / `metadata.json` |
| 3 | Bootstrap fonctionne | 20 contrôles, tous `OK`, sortie 0 |
| 4 | `make demo` fonctionne | `DEMO EXIT=0` |
| 5 | Kafka initialisé | `kafka-init` sorti en 0, broker healthy |
| 6 | Topics aux bonnes partitions | 6 / 6 / 3 / 3 / 1 / 1 |
| 7 | PostgreSQL démarre | healthy |
| 8 | Flyway applique V1/V2/V3 | `success=true` pour les trois |
| 9 | `alert-service` healthy | readiness `UP` |
| 10 | Spark charge le modèle | `artifact_verified` avec le SHA-256 exact, 52 features |
| 11 | Le simulateur produit | `simulation_starting`, 15 machines, 108 000 messages |
| 12 | Des alertes sont produites | 12 alertes, **12 identifiants distincts** |
| 13 | La télémétrie est persistée | 11 460 fenêtres, 15 machines |
| 14 | Dashboard accessible | `GET /` → 200, `/healthz` → `ok` |
| 15 | REST fonctionne | `GET /api/v1/alerts` → 200 à travers le proxy nginx |
| 16 | SSE fonctionne | 5 `alert.created` + 1 `heartbeat` en 20 s |
| 17 | Parcours d'acquittement | 200 → **409** → `ACKNOWLEDGED` en base |

### SSE, capturé à travers nginx

```
id:9
event:alert.created
data:{"alertId":"c44288a8-…","eventSeq":9,"machineCode":"M-004",…}

event:heartbeat
data:{}
```

Les curseurs `id:` se suivent (7, 8, 9, 10, 11) : c'est la séquence monotone qui
sert de point de reprise, et non l'UUID de l'alerte. Le heartbeat arrive comme
prévu. La réception incrémentale prouve aussi que `proxy_buffering off` fait son
travail — sans lui, rien ne serait sorti du proxy.

### Acquittement

| Appel | Résultat |
|---|---|
| 1er `POST …/acknowledge` avec `X-Operator` | **200**, `NEW → ACKNOWLEDGED`, acteur enregistré |
| 2e appel sur la même alerte | **409** `invalid-state-transition`, avec `traceId`, `currentStatus`, `currentVersion` |
| `GET` sur un UUID inexistant | **404** `alert-not-found`, avec `traceId` |
| En base | `ACKNOWLEDGED`, 1 ligne d'acquittement |

### Courbe de télémétrie servie au dashboard

`GET /api/v1/machines/M-010/telemetry` : 348 points, `windowSeconds=60`,
**16 anomalies marquées**, **89 fenêtres non scorées** conservées avec leurs
vraies mesures. C'est exactement le contrat d'ADR-008 : un trou reste un trou.

### Arrêt, redémarrage, récupération

`down` sans `-v`, puis `up`.

| | Avant | Après |
|---|---|---|
| Alertes / identifiants distincts | 12 / 12 | **12 / 12** |
| Fenêtres de télémétrie | 11 265 | 11 460 *(le flux continue)* |
| `flyway_schema_history.installed_on` | 15:39:13 | **15:39:13, inchangé** |
| `batch_id` Spark | 13 – 18 | **20 – 25** |

Rien n'est perdu, **rien n'est dupliqué**, Flyway ne rejoue pas, et Spark reprend
au numéro de lot suivant plutôt que de repartir de zéro : le checkpoint a
survécu à l'arrêt. L'artefact est re-vérifié au démarrage, avec la même
empreinte.

## 10. Contexte de build : deux gaspillages mesurés

| Contexte | Avant | Après |
|---|---|---|
| `alert-service` | **74 Mo** (`target/`) | **293 ko** |
| Composants Python (racine) | contenait `data/` (**226 Mo**) | exclu |

`alert-service` construit depuis **son propre contexte**
(`context: ../alert-service`), et Docker lit `.dockerignore` à la racine du
contexte : celui du dépôt ne s'y appliquait donc pas. 74 Mo de sortie de build
étaient envoyés au démon à chaque construction, puis jamais utilisés — le
Dockerfile ne copie que `pom.xml` et `src/`, et compile dans le conteneur
précisément pour que l'artefact ne dépende pas de l'hôte.

## 11. Ce que la Phase 6 n'a pas fait

- **Aucune capture d'écran n'a été produite.** `docs/screenshots/` contient le
  mode d'emploi et rien d'autre ; les liens du README sont cassés tant que les
  images n'existent pas, ce qui se voit et se corrige — contrairement à une
  capture fabriquée.
- **Aucun épinglage par digest.** Les images restent référencées par tags,
  mutables. C'est une limite de reproductibilité, écrite comme telle.
- **Le modèle n'a pas été touché** : ni ré-entraîné, ni recalibré, ni remplacé.
- **Aucun second système de migration** n'a été créé.
- **La CI n'a pas été exécutée** : elle ne tourne qu'après un push. Les portes
  qu'elle contient ont toutes été exécutées localement.
