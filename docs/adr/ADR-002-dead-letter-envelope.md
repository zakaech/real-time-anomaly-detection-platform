# ADR-002 — Contrat de l'enveloppe de rebut (`telemetry.dlq`)

- **Statut** : accepté (Phase 3)
- **Décisions liées** : D-02 (JSON + JSON Schema), D-05 (topics de rebut)

## Contexte

La forme des messages de rebut n'existait jusqu'ici que sous forme de prose dans `docs/03` section 7. Rien ne
l'imposait. Or c'est un contrat de plateforme au même titre que les autres : un opérateur doit pouvoir
diagnostiquer un rejet, et un rejet doit pouvoir être **rejoué** après correction.

## Décision

L'enveloppe reçoit le même traitement que les autres contrats : un JSON Schema de référence
(`contracts/json-schema/telemetry-dlq.v1.json`), une dataclass gelée conforme (`telemetry_core.dlq`), et un
test de conformance.

Module distinct de `telemetry_core.schemas` : le message n'est pas de la télémétrie, c'est une **enveloppe
opérationnelle** qui en contient. Les séparer signifie que l'ajouter ne change rien à ce qui fonctionne déjà.

Le champ `raw_payload` contient les **octets d'origine**, encodés en base64 et non normalisés. Le rejeu après
correction n'est possible qu'à cette condition. Un payload `null` (tombstone) est encodé en chaîne vide plutôt
que rejeté : une tombstone arrivée jusqu'au parseur mérite d'être enregistrée.

### Ce qui va au rebut, et ce qui n'y va pas

**Uniquement les erreurs de données** : JSON malformé, violation de schéma, version majeure non supportée,
horodatage illisible.

**Jamais les erreurs d'infrastructure** : broker indisponible, executor perdu, timeout réseau. Celles-ci sont
traitées par le rejeu et le checkpoint. Les transformer en rebut reviendrait à **jeter des messages
parfaitement valides pendant un incident** — précisément le moment où les perdre coûte le plus cher.

C'est la distinction que la prose initiale ne faisait pas et que le contrat rend explicite.

## Conséquences

- Un message rejeté ne bloque pas le lot : la validation produit deux flux depuis la même lecture, et un
  message malformé n'empêche pas ses voisins d'être traités.
- Le décodage tolère les sauts de ligne. La fonction `base64` de Spark émet du **MIME découpé** — lignes de 76
  caractères séparées par des retours — au-delà de cette longueur. C'est du base64 valide, refusé par un
  décodeur strict. Trouvé en relisant le topic, pas en le supposant.
- La profondeur de la file de rebut devient une métrique de santé exploitable, puisque son contenu a une forme
  garantie.

## Vérification

`stream-processor/tests/test_source_and_policy.py` — conformance de l'enveloppe à son schéma, aller-retour,
refus d'un `dlq_detail` vide (un rejet non actionnable est une ligne de log, pas une lettre morte), et surtout
**la survie des octets d'origine pour le rejeu**. Le comptage par motif du contenu réel du topic est produit
par `inspect-stream`.
