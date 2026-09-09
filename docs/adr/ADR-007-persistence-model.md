# ADR-007 — Modèle de persistance et clé d'idempotence

- **Statut** : accepté (Phase 4)
- **Décisions liées** : D-38, D-39, ADR-005 ; remplace le schéma esquissé dans `docs/05` section 3

## Contexte

La Phase 0 avait esquissé six tables (`production_line`, `machine`, `model_version`, `alert`,
`alert_transition`, `drift_metric`). La Phase 4 doit livrer ce qui est réellement alimenté.

## Décision — trois tables (D-38)

`machine`, `alert`, `alert_acknowledgement`.

`production_line` est fusionnée dans `machine` sous forme de colonne `line_code` : une ligne de production n'a
aujourd'hui **aucun attribut propre**, `LINE-A` est un code et rien d'autre. `model_version` et `drift_metric`
sont reportées : rien ne les produit, et une table vide est une dette qui ressemble à une fonctionnalité.

Sont également écartées les colonnes `criticality`, `commissioned_on` et `nominal_ranges` du schéma d'origine.
Aucun producteur ne les remplit — `fleet.yaml` ne contient que `machine_id`, `line_id`, `profile` et
`firmware_version`. La description du contrat affirme que la sévérité dépend « de la criticité de la
machine » ; c'est **faux**, `severity_for()` n'utilise que le score et le seuil. Créer la colonne aurait
pérennisé l'affirmation.

## Décision — `alert_id` en clé primaire

| Option | Avantages | Inconvénients |
|---|---|---|
| **A — `alert_id` (UUID) en PK** | L'unicité **est** la clé primaire ; `ON CONFLICT (id)` atomique ; identifiant déjà stable, global, et déjà exposé dans l'URL | UUID aléatoire, donc moins bonne localité d'index qu'une séquence |
| B — clé composite des 4 colonnes | Reflète la sémantique naturelle | PK large répliquée dans chaque clé étrangère ; l'URL utiliserait de toute façon `alert_id` |
| C — BIGSERIAL + UNIQUE sur `alert_id` | Bonne localité d'écriture | **Deux identités** pour un même objet, dont une jamais visible de l'extérieur : coût sans bénéfice |

**Retenu : A.** `alert_id` est un UUIDv5 **déterministe** dérivé exactement de la clé composite de l'option B —
il *est* cette clé, sous forme scalaire.

L'objection sur la localité est réelle et je la chiffre plutôt que de la balayer : l'exécution réelle de la
Phase 3 a produit **45 alertes pour 4 heures de télémétrie sur 15 machines**. Même à 10 000 alertes par jour,
un index UUID reste sans problème. Si le volume changeait d'un ordre de grandeur, C deviendrait le bon choix.

### La contrainte de clé naturelle est conservée en plus

`UNIQUE (machine_id, window_start, model_name, model_version)`. Si la dérivation d'`alert_id` changeait en
amont, la même fenêtre arriverait sous un nouvel UUID et la clé primaire l'insérerait sans rien remarquer.
Cette contrainte transforme cette duplication silencieuse en **échec bruyant**. C'est exactement le mode de
défaillance que le projet cherche à rendre visible partout ailleurs.

## Décision — machine inconnue auto-provisionnée (D-39)

Une clé étrangère stricte rejetterait une alerte dont la machine manque au référentiel. **C'est le mauvais
mode de panne** : on jetterait une détection réelle parce qu'un seed est périmé. L'alerte porte `machine_id` et
`line_id`, ce qui suffit à créer la ligne. `INSERT ... ON CONFLICT (code) DO NOTHING`, parce que trois threads
consommateurs peuvent rencontrer la même machine neuve au même instant.

Le seed Flyway reste la source des métadonnées riches (`machine_type`) et utilise `DO UPDATE` pour enrichir une
ligne déjà créée par le flux.

## Autres choix

- **`DOUBLE PRECISION`** pour les scores, pas `NUMERIC(7,6)` comme esquissé : le score réel mesuré est
  `0.9999995735846934`, que `NUMERIC(7,6)` arrondirait.
- **`event_seq BIGSERIAL UNIQUE`** : la reprise SSE a besoin d'un curseur **ordonné**, et la clé primaire est
  un UUID aléatoire qui n'ordonne rien. Les trous dans la séquence, causés par les conflits, sont sans
  importance pour un curseur.
- **`CHECK (detected_at = window_end)`** : le contrat l'impose et le producteur le valide. Le redire ici
  signifie qu'un bug producteur ne peut pas déposer discrètement une ligne montrant à un opérateur le mauvais
  instant.
- **`alert_acknowledgement` en ajout seul** : c'est un journal d'audit. Écraser un acquittement détruirait la
  seule trace qu'il a eu lieu. La table accepte déjà `RESOLVED` et `DISMISSED`, donc étendre le cycle de vie ne
  demandera aucune migration.
- **Flyway seul propriétaire du schéma**, `ddl-auto=validate`. Hibernate **vérifie** la migration au lieu de la
  modifier : une entité qui dérive fait échouer le démarrage. Cela a déjà servi — un `CHAR(64)` dans la
  migration face au `varchar` attendu par l'entité a été détecté au premier démarrage.

## Vérification

`AlertIdempotencyTest` (Testcontainers PostgreSQL) et `AlertControllerTest`. Le schéma est appliqué par les
migrations réelles à chaque exécution des tests, donc chaque exécution teste aussi les migrations.
