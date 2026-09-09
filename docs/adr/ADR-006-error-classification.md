# ADR-006 — Classification des erreurs, retry et file de rebut

- **Statut** : accepté (Phase 4)
- **Décisions liées** : ADR-002 (enveloppe de rebut), ADR-005

## Contexte

Un consommateur Kafka rencontre trois familles de problèmes qui n'ont rien en commun, et les traiter
uniformément produit un système qui a l'air de fonctionner.

## Décision

| Classe | Exemple | Traitement | Pourquoi pas l'inverse |
|---|---|---|---|
| **Transitoire** | PostgreSQL indisponible, timeout, deadlock | Retry avec backoff exponentiel, **sans épuisement**. Offset non commité | La mettre en rebut jetterait des alertes valides **pendant un incident**, c'est-à-dire au moment où les perdre coûte le plus cher (ADR-002) |
| **Données** | JSON malformé, violation de schéma, version majeure non supportée | **Rebut immédiat, zéro retry** | Rejouer un message invalide dix fois ne le rendra pas valide ; le retenter en boucle bloque sa partition derrière une pilule empoisonnée |
| **Doublon** | même `alert_id` | **Succès logique**, offset commité, compteur incrémenté | Le traiter comme une erreur produirait un rejeu infini sur un message parfaitement valide et déjà stocké |

L'asymétrie est le cœur de la décision : réessayer indéfiniment sur une panne de base est **correct** ;
réessayer indéfiniment sur une charge utile corrompue est une **boucle bloquante**.

### Mise en œuvre

`DefaultErrorHandler` + `DeadLetterPublishingRecoverer`, avec
`addNotRetryableExceptions(NonRetryableMessageException.class)` : seule cette exception court-circuite le
backoff. Le backoff plafonne l'**intervalle** (30 s), jamais le **nombre** de tentatives — une base
indisponible une heure ne doit pas provoquer une heure d'alertes perdues. La partition cale, le retard de
consommation grandit, et les deux sont visibles : c'est l'échec bruyant que l'on veut.

### Désérialisation en octets, pas en JSON

La valeur est lue en `byte[]` et désérialisée à la main. Un `JsonDeserializer` qui échoue le fait **à
l'intérieur de `poll()`**, hors de portée du gestionnaire d'erreurs, et la charge utile d'origine est perdue —
ce qui rendrait le rebut non rejouable et lui ôterait sa raison d'être.

### Un seul topic de rebut

`telemetry.dlq`, partagé avec le job Spark, dans la **même enveloppe `DlqEnvelope`**. Le contrat porte déjà
`source_topic`, `source_partition`, `source_offset` et la charge utile d'origine en base64 : il a été conçu
pour être partagé. Un second topic aurait signifié un second contrat et une seconde surveillance.

Le contrat étant défini en Python, l'implémentation Java est vérifiée contre **le même JSON Schema** plutôt que
contre elle-même — deux implémentations ne restent alignées que si un tiers les arbitre.

### Si la publication du rebut échoue

L'exception est propagée et l'offset d'origine n'est pas commité. Avaler les deux perdrait le message en
silence, ce qui est le seul résultat qui mérite un échec bruyant.

## Conséquences

- Un incident d'infrastructure se traduit par du retard de consommation, pas par des pertes.
- La file de rebut ne contient que des messages qu'un humain peut diagnostiquer et rejouer.
- Le compteur `alerts.dlq` mesure des erreurs de données, jamais des pannes.

## Vérification

`AlertConsumerTest` : message valide acquitté, doublon acquitté sans retry, JSON malformé non rejouable,
champ requis absent, version majeure non supportée distinguée par son motif, tombstone, instant de détection
incohérent, et **panne de base propagée comme exception *retryable*** — explicitement pas une
`NonRetryableMessageException`, ce qui est la ligne exacte entre « on réessaie » et « on met au rebut ».
