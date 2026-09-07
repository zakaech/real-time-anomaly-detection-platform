# ADR-003 — Watermark à 90 s et mode de sortie `update`

- **Statut** : accepté (Phase 3)
- **Décisions liées** : D-04 (mode `update`), D-36, D-12 (hystérésis d'alerte)

## Contexte

Deux réglages gouvernent le compromis latence / exhaustivité de tout le pipeline, et ils interagissent.

Le **watermark** fixe combien de temps l'état d'une fenêtre est conservé pour accueillir des données en
retard. Trop court : les retards sont écartés silencieusement. Trop long : l'état croît et la mémoire avec.

Le **mode de sortie** décide quand une fenêtre est publiée. En `append`, une fenêtre n'est émise qu'une fois
close *et* dépassée par le watermark — soit ici fenêtre + watermark ≈ 150 s de latence structurelle. En
`update`, elle est publiée à chaque lot qui la modifie.

## Décision — watermark 90 s

La conception de Phase 0 prévoyait 30 s. Mesure faite sur le simulateur : une coupure réseau met une machine en
tampon **20 à 75 s**, et la gigue de publication ajoute jusqu'à **9 s**. Le pire cas configuré est donc
d'environ **84 s**. Un watermark de 30 s aurait écarté précisément les données générées pour exercer ce
mécanisme — le pipeline aurait paru sain tout en perdant le cas qu'il devait traiter.

**Cette valeur est propre à l'environnement simulé.** Sur des données industrielles réelles elle doit être
recalibrée depuis la distribution mesurée du retard de publication, pas reprise telle quelle. Le compteur
`numRowsDroppedByWatermark`, journalisé à chaque lot, est la mesure qui doit piloter ce réglage.

## Décision — mode `update`

`append` impose ~150 s avant qu'une fenêtre atteigne le tableau de bord. Difficile à défendre pour un système
annoncé temps réel.

Le coût d'`update` est réel et connu : **une même fenêtre est publiée plusieurs fois**. Il est neutralisé, non
ignoré, à trois endroits :

1. `telemetry.scored` est une **frontière contractuelle**, pas un journal : le consommateur lit un état de
   fenêtre, pas un événement immuable.
2. La machine à états d'alerte déduplique explicitement par `(machine_id, window_start)` — plusieurs mises à
   jour d'une même fenêtre comptent pour **une** fenêtre dans l'hystérésis, sinon deux mises à jour d'une seule
   fenêtre anormale ouvriraient une alerte que la règle « deux fenêtres consécutives » devait empêcher.
3. `alert_id` est dérivé de façon déterministe (UUIDv5) : une ré-émission **met à jour** au lieu de dupliquer.

Le plan de Phase 3 s'était engagé à **mesurer** ce surcoût avant toute optimisation. Le ratio de mise à jour
mesuré figure dans `docs/10-phase-3-streaming.md` ; il n'a pas été estimé.

## Conséquences

- La sémantique reste **at-least-once** de bout en bout, *effectively-once* à la persistance grâce à
  l'identifiant déterministe et à l'upsert. Ce n'est **pas** de l'exactly-once et n'est jamais présenté comme
  tel : le sink Kafka de Spark n'est pas transactionnel ici, et un redémarrage rejoue le lot non commité.
- Un timeout d'état est armé sur le temps d'événement, donc il doit rester **devant le watermark**. Spark
  refuse le contraire par une exception qui tue la requête — ce qui s'est produit lors d'un vrai redémarrage,
  le watermark étant restauré depuis le checkpoint alors que le lot rejoué portait des fenêtres antérieures.
  Le timeout est désormais borné par le watermark courant. Le chemin nominal n'atteint jamais ce cas ; seul le
  test de crash l'a révélé.

## Vérification

`stream-processor/tests/test_alerting.py` — hystérésis, déduplication en mode `update` (mises à jour répétées
d'une fenêtre, mises à jour d'une fenêtre antérieure), frontières d'épisode, et le cas du timeout derrière le
watermark. Les effets mesurés en exécution réelle sont dans `docs/10-phase-3-streaming.md`.
