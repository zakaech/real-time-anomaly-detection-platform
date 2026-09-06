# 03 — Topologie Kafka

## 1. Plan des topics

| Topic | Partitions | Clé | Rétention | Nettoyage | Producteur | Consommateurs |
|---|---|---|---|---|---|---|
| `telemetry.raw` | 6 | `machine_id` | 7 jours | `delete` | event-simulator | stream-processor, ml-training (rejeu) |
| `telemetry.scored` | 6 | `machine_id` | 3 jours | `delete` | stream-processor | dashboard (via alert-service), analyse de dérive |
| `alerts` | 3 | `machine_id` | 30 jours | `delete` | stream-processor | alert-service |
| `telemetry.labels` | 3 | `machine_id` | 90 jours | `delete` | event-simulator, alert-service | ml-training, évaluation |
| `telemetry.dlq` | 1 | `null` | 30 jours | `delete` | stream-processor | inspection humaine, rejeu manuel |
| `telemetry.late` | 1 | `machine_id` | 7 jours | `delete` | stream-processor | observabilité, réglage du watermark |

Tous les topics sont créés explicitement par un script d'initialisation (`infra/kafka/create-topics.sh`).
**`auto.create.topics.enable` est mis à `false`.** Un topic auto-créé prend les valeurs par défaut du broker —
en général une seule partition — et on découvre le problème le jour où l'on cherche à monter en charge, avec un
topic qu'on ne peut plus repartitionner sans casser l'affinité des clés.

## 2. Pourquoi `machine_id` comme clé de partition

Kafka ne garantit l'ordre **qu'à l'intérieur d'une partition**. La question n'est donc pas « comment ordonner
le flux » — c'est impossible et inutile — mais « quel ordre a réellement une signification métier ».

Ici, le seul ordre qui compte est **l'ordre des mesures d'une même machine** : la fenêtre glissante, le calcul
de tendance, et l'hystérésis sur fenêtres consécutives sont tous des raisonnements par machine. L'ordre relatif
entre la machine M-003 et la machine M-014 n'a aucun sens physique : ce sont deux processus indépendants.

En partitionnant sur `machine_id` :

- tous les événements d'une machine atterrissent dans la même partition, donc sont lus dans l'ordre de
  production, ce qui rend le désordre temporel exclusivement dû au réseau et aux buffers côté source — donc
  borné et traitable par le watermark plutôt que par la topologie ;
- le parallélisme reste réel : 20 machines réparties sur 6 partitions permettent 6 consommateurs simultanés ;
- `alerts` étant partitionné sur la même clé, toutes les alertes d'une machine sont traitées séquentiellement
  par le même consommateur Spring, ce qui supprime toute course entre deux alertes de la même machine.

Kafka utilise **murmur2** sur les octets de la clé pour choisir la partition. La répartition de 20 clés sur 6
partitions n'est pas parfaitement équilibrée — c'est un hachage, pas un round-robin — mais l'écart est sans
conséquence à ce volume. Le point important est ailleurs : **si une machine devenait très dominante en débit,
elle créerait une partition chaude**, et le seul remède serait une clé composite `machine_id#bucket`, qui
détruirait l'ordre par machine. Ce serait un mauvais échange ici ; on préfère surveiller le lag par partition.

## 3. Pourquoi 6, 3 et 1 partitions

Le nombre de partitions détermine le **plafond de parallélisme** d'un groupe de consommateurs : au-delà de N
consommateurs pour N partitions, les consommateurs supplémentaires restent inactifs.

- **`telemetry.raw` → 6.** C'est le topic chaud. 6 est divisible par 1, 2, 3 et 6, donc toute taille de groupe
  de consommateurs dans cet intervalle donne une répartition parfaitement équilibrée. Le facteur décisif est
  qu'**on ne peut qu'augmenter le nombre de partitions, jamais le réduire**, et qu'une augmentation modifie le
  résultat de `hash(clé) mod N` : les événements d'une machine changeraient de partition et l'ordre serait
  rompu à la jonction. On prend donc de la marge dès le départ.
- **`alerts` → 3.** Le débit d'alertes est de deux à trois ordres de grandeur inférieur à celui de la
  télémétrie (une alerte représente une fenêtre au-delà du seuil, pas un échantillon). 3 partitions suffisent et
  correspondent au `concurrency: 3` du listener Spring — un thread par partition, sans thread oisif.
- **`telemetry.dlq` → 1.** Un rebut ne doit pas être parallélisé : on veut pouvoir le lire de bout en bout dans
  l'ordre, à la main, quand quelque chose a mal tourné. Une seule partition rend cette lecture triviale.

## 4. Réplication : une dette de développement assumée

En développement, `docker-compose` lance **un seul broker**, donc `replication.factor = 1` et
`min.insync.replicas = 1`. C'est une configuration qui **perd des données** en cas de panne du broker, et il
faut le dire plutôt que de laisser croire à de la haute disponibilité.

La configuration de production correspondante, écrite dans `infra/kafka/topics.prod.yaml` à titre de référence :

```
replication.factor = 3
min.insync.replicas = 2
acks = all                       (côté producteur)
unclean.leader.election.enable = false
```

`acks=all` combiné à `min.insync.replicas=2` signifie : un message n'est acquitté que lorsqu'il est présent sur
au moins deux répliques. C'est ce qui rend la perte d'un broker non destructive. `unclean.leader.election` à
`false` interdit à une réplique en retard de devenir leader — élire un leader en retard, c'est perdre
silencieusement les messages qu'il n'avait pas.

## 5. Configuration des producteurs

**event-simulator (Python, `confluent-kafka`)**

```
acks = all
enable.idempotence = true
max.in.flight.requests.per.connection = 5
retries = 2147483647
compression.type = lz4
linger.ms = 20
batch.size = 65536
```

`enable.idempotence=true` mérite une précision, car c'est un piège classique en entretien : il garantit qu'un
**retour en arrière réseau ne duplique pas** un message dans la partition, grâce au numéro de séquence porté par
le producteur. Il ne garantit **rien** au-delà de la durée de vie du processus producteur : si le simulateur
plante après l'envoi mais avant d'avoir mémorisé qu'il a envoyé, un redémarrage renverra le message avec un
nouveau `producer_id`, et le doublon passera. L'idempotence producteur n'est donc pas de l'exactly-once
de bout en bout — c'est l'`event_id` qui joue ce rôle en aval.

`linger.ms=20` accepte 20 ms de latence supplémentaire en échange d'un vrai lot : sans cela, à 1 Hz par
machine, on enverrait des requêtes réseau minuscules. C'est un arbitrage latence/débit, pas un réglage magique.

**stream-processor (sink Kafka de Spark)** : Spark impose ses propres réglages producteur. On y fixe
`kafka.compression.type=lz4` et `kafka.acks=all`. On ne peut pas y activer de transaction — voir
`docs/04-streaming-semantics.md`.

## 6. Configuration des consommateurs

**stream-processor**

```
startingOffsets = latest        (au premier démarrage seulement)
failOnDataLoss = true
maxOffsetsPerTrigger = <calculé, voir 04>
kafka.group.id = non défini     (Spark gère ses offsets dans le checkpoint)
```

Deux pièges à connaître :

- **`startingOffsets` est ignoré dès qu'un checkpoint existe.** C'est la source la plus fréquente de « pourquoi
  mon job ne relit pas depuis le début ». Pour rejouer, on supprime le checkpoint ou on en pointe un nouveau —
  et alors les sorties seront des doublons, absorbés par l'idempotence de l'`alert_id`.
- **`failOnDataLoss=true`** fait échouer le job si les offsets attendus ont été purgés par la rétention.
  L'alternative silencieuse (`false`) fait sauter un trou de données sans rien dire ; pour un système de
  détection, un trou muet est pire qu'un arrêt.

**alert-service (Spring Kafka)**

```
group.id = alert-service
enable.auto.commit = false
ack-mode = MANUAL_IMMEDIATE      (offset validé après le commit PostgreSQL)
auto.offset.reset = earliest
max.poll.records = 200
max.poll.interval.ms = 300000
concurrency = 3                  (aligné sur les 3 partitions d'alerts)
isolation.level = read_committed
```

L'ordre commit base de données **puis** commit d'offset est délibéré : il produit de l'at-least-once. L'ordre
inverse produirait de l'at-most-once, c'est-à-dire des alertes perdues — inacceptable pour un système dont
l'objet est de ne pas rater d'anomalie. On choisit donc le doublon, et on le neutralise par la clé primaire.

`auto.offset.reset=earliest` : un nouveau groupe de consommateurs doit reprendre l'historique disponible plutôt
que d'ignorer les alertes déjà publiées.

## 7. Politique de rebut (DLQ)

Un message part sur `telemetry.dlq` dans trois cas, et **jamais** pour une autre raison :

1. JSON illisible (`_corrupt_record` non nul après `from_json` en mode `PERMISSIVE`) ;
2. champ obligatoire absent ou hors domaine (`machine_id` inconnu, `event_time` non parsable) ;
3. `schema_version` de version majeure non supportée par ce job.

Le message rebuté est enveloppé sans être modifié :

```json
{
  "dlq_reason": "SCHEMA_VALIDATION_FAILED",
  "dlq_detail": "readings.temperature_c = 812.4 hors domaine [-50, 300]",
  "source_topic": "telemetry.raw",
  "source_partition": 3,
  "source_offset": 148223,
  "failed_at": "2026-09-06T14:23:41.905Z",
  "processor_version": "0.4.1",
  "raw_payload": "<message original, tel quel, en base64>"
}
```

Conserver la charge utile **originale non modifiée** est ce qui rend le rejeu possible après correction. Une
DLQ qui ne contient qu'un message d'erreur est un journal, pas une file de rebut.

Ce qui ne va **pas** en DLQ : une panne transitoire (broker indisponible, exécuteur perdu). Ces cas relèvent du
retry et du checkpoint. Confondre erreur de données et erreur d'infrastructure conduit à jeter des messages
parfaitement valides.

## 8. Rétentions et leurs raisons

- **`telemetry.raw` à 7 jours** : c'est la fenêtre de rejeu. Elle doit couvrir le temps de détecter un bug de
  traitement, le corriger et rejouer. Sept jours couvrent un week-end plus une semaine de travail.
- **`telemetry.scored` à 3 jours** : flux d'affichage et d'analyse de dérive à court terme, sans valeur
  d'archive.
- **`alerts` à 30 jours** : Kafka n'est **pas** la source de vérité des alertes — PostgreSQL l'est. La rétention
  sert uniquement à permettre à un consommateur reconstruit de rattraper son retard.
- **`telemetry.labels` à 90 jours** : les étiquettes sont rares, petites et durablement utiles pour
  l'entraînement et l'évaluation.

Aucun topic n'est en `cleanup.policy=compact`. La compaction ne conserve que la dernière valeur par clé : elle
convient à un état (« dernière configuration connue de la machine M-014 »), pas à un flux d'événements où
chaque message est un fait distinct. Le seul candidat serait un futur topic `machines.reference` — voir D-06.

## 9. Notes d'exploitation

### 9.1 Écouteurs en mode KRaft combiné : hôte vide, pas `0.0.0.0`

Piège rencontré au démarrage de la pile, et corrigé dans `infra/docker-compose.yml`. Déclarer

```
KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
```

fait échouer le broker avant même qu'il ne démarre :

```
requirement failed: advertised.listeners cannot use the nonroutable meta-address 0.0.0.0
```

La cause : en mode KRaft **combiné** (`process.roles=broker,controller`), Kafka dérive l'endpoint annoncé du
contrôleur depuis `listeners`, puisque `advertised.listeners` ne contient pas — et ne doit pas contenir — le
listener de contrôleur. Le littéral `0.0.0.0` se retrouve donc parmi les adresses annoncées, ce que la
validation refuse à juste titre : une adresse annoncée sert à être contactée, et personne ne contacte
`0.0.0.0`.

La forme correcte est l'hôte vide, `INTERNAL://:9092`, qui signifie également « toutes les interfaces » côté
liaison mais ne produit pas d'adresse annoncée non routable. C'est d'ailleurs la forme employée par le
`server.properties` livré avec Kafka. Le symptôme est un redémarrage en boucle du conteneur, sans que le
healthcheck ne dise pourquoi.

### 9.2 Avertissement sur les points dans les noms de topics

`kafka-topics.sh` émet, à chaque création :

```
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_')
could collide.
```

C'est attendu et sans conséquence ici : les noms des métriques JMX remplacent `.` et `_` par le même
caractère, donc `telemetry.raw` et `telemetry_raw` produiraient la même métrique. Notre convention n'utilise
que le point, jamais le tiret bas, donc aucune collision n'est possible. L'avertissement est conservé plutôt
que masqué : il faut qu'un futur `telemetry_raw` déclenche bien la même alerte.
