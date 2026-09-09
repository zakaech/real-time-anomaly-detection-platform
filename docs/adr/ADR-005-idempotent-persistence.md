# ADR-005 — Idempotence Kafka → PostgreSQL

- **Statut** : accepté (Phase 4)
- **Décisions liées** : D-04 (mode `update`), ADR-002, ADR-007

## Contexte

La chaîne est **at-least-once**. Ce n'est pas une hypothèse prudente : le test de
crash de la Phase 3 a mesuré **8 livraisons dupliquées** d'alertes sur une exécution et **0** sur une autre.
Les rejeux sont réels et intermittents.

Le scénario à couvrir est précis :

```
1. Spark publie l'alerte X                → offset 42
2. alert-service consomme 42, INSERT X    → COMMIT PostgreSQL ✅
3. CRASH avant le commit de l'offset Kafka
4. Redémarrage : reprise à l'offset 42
5. Même alerte X rejouée
6. AUCUN deuxième enregistrement
7. Offset 42 finalement commité
```

## Décision

Une seule instruction, exécutée par PostgreSQL :

```sql
INSERT INTO alert (...) VALUES (...)
ON CONFLICT (id) DO UPDATE
   SET ..., updated_at = now()
 WHERE alert.status = 'NEW'
RETURNING (xmax = 0)
```

**La garantie appartient à la base, pas au code Java.** C'est le critère qui a écarté les trois autres
approches.

| Approche | Atomique ? | Pourquoi écartée |
|---|---|---|
| `existsById()` puis `save()` | ❌ | Vérification-puis-action : deux threads consommateurs passent le test simultanément, et le perdant échoue ensuite sur la contrainte — une opération censée être idempotente devient une erreur |
| `save()` de JPA | ❌ | Fait son propre SELECT puis INSERT/UPDATE : même fenêtre de course, plus un UPDATE non désiré |
| `catch DataIntegrityViolationException` | ⚠️ | Fonctionne, mais une violation de contrainte marque la transaction JPA `rollback-only` : tout travail ultérieur dans la même transaction échoue aussi. Piège Spring classique |
| **`ON CONFLICT` + `RETURNING`** | ✅ | Une seule instruction ; seule la base peut arbitrer une course de manière atomique |

### Trois détails porteurs

1. **`DO UPDATE` plutôt que `DO NOTHING`** — un rejeu peut légitimement porter un score ou un
   `consecutive_windows` révisé. On garde la dernière version du **fait**.
2. **`WHERE alert.status = 'NEW'`** — une alerte déjà acquittée n'est **jamais** réécrite par un rejeu Kafka.
   Sans cette clause, un incident Spark renverrait dans la file de l'opérateur des alertes déjà traitées.
3. **`RETURNING (xmax = 0)`** — `xmax` vaut 0 sur une ligne réellement insérée, non nul sur une ligne mise à
   jour. Ce booléen est ce qui permet d'émettre l'événement SSE **une fois par alerte** et non une fois par
   livraison ; sinon chaque doublon ferait clignoter le tableau de bord.

### Frontière transactionnelle

Il n'y a **pas** de transaction distribuée Kafka/PostgreSQL, et il ne peut pas y en avoir : Kafka ne participe
pas à un commit à deux phases, et le prétendre serait faux. L'ordre est donc toujours **commit base, puis
commit d'offset** :

| Ordre | Conséquence d'un crash entre les deux |
|---|---|
| offset puis base | **perte définitive** de l'alerte |
| **base puis offset** | **rejeu**, absorbé par l'`ON CONFLICT` |

L'idempotence n'est pas un confort : c'est la contrepartie du seul ordre qui ne perd pas de données.

**Un doublon n'est pas une erreur.** Le service retourne « 0 ligne insérée », journalise, incrémente un
compteur et acquitte l'offset normalement. Le traiter comme une erreur produirait une boucle de rejeu infinie
sur un message parfaitement valide.

## Conséquences

- La sémantique annoncée est **at-least-once + idempotence = effectively-once à la persistance**. Ce n'est
  **pas** de l'exactly-once et ne sera jamais présenté comme tel.
- Le compteur `alerts.duplicate` est la métrique qui **montre** l'idempotence à l'œuvre au lieu de l'affirmer.
- L'écriture d'ingestion n'utilise pas JPA. Les lectures et la transition d'acquittement, si.

## Vérification

`AlertIdempotencyTest`, contre un vrai PostgreSQL via Testcontainers : rejeu simple, rejeu portant un score
révisé, rejeu sur une alerte déjà acquittée, **huit threads concurrents dont exactement un insère**, contrainte
de clé naturelle, machine inconnue auto-provisionnée, et le CHECK sur `detected_at`. Aucun de ces tests ne
passerait si l'unicité venait d'un `if`.
