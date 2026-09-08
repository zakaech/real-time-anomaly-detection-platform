# ADR-004 — Maturité de fenêtre : ne scorer que ce que le modèle a appris

- **Statut** : accepté (correction de la Phase 3)
- **Décisions liées** : D-37, D-36 (mode `update`), D-04, ADR-003

## Contexte

Le mode `update` republie une fenêtre à chaque déclenchement qui la modifie. Une fenêtre est donc normalement
publiée **pendant qu'elle se remplit**. Le modèle, lui, a été entraîné en Phase 2 sur des fenêtres **complètes**
de 60 secondes.

Le seuil d'admission n'exigeait que `sample_count >= 30`, ce qui ne dit **rien** de l'endroit où ces
échantillons tombent dans la fenêtre. Mesure sur une exécution temps réel de 300 s :

| Échantillons à l'émission | Émissions | Jugées anormales |
|---|---|---|
| 30 – 39 | 383 | **383 (100,0 %)** |
| 40 – 49 | 351 | 335 (95,4 %) |
| 50 – 59 | 322 | 151 (46,9 %) |
| 60 et plus | 292 | **3 (1,0 %)** |

Ce ne sont pas des anomalies : ce sont des entrées **hors distribution**. Une moyenne, un écart-type et une
pente sur 30 secondes ne ressemblent pas à ceux sur 60 secondes. Conséquence observée : **16 alertes
`CRITICAL` pour 15 machines en cinq minutes**.

Le backfill ne montrait rien (4 %) : les données y arrivent par blocs, une fenêtre passe de vide à complète
dans un seul lot, et n'est presque jamais publiée partielle. **Un test en rejeu ne remplace pas une exécution
temps réel.**

## Le point central : maturité ≠ watermark

Confondre les deux est l'erreur naturelle, et elle conduit à « attendre 90 secondes ». C'est faux.

| | **Watermark** | **Maturité de fenêtre** |
|---|---|---|
| Question | quelle est la donnée la plus ancienne encore acceptée ? | cette fenêtre a-t-elle reçu ce qui lui appartient ? |
| Portée | **globale** au flux | **par (machine, fenêtre)** |
| Source | `max(event_time)` de **toutes** les clés − délai | le contenu propre de la fenêtre |
| Rôle | éviction de l'état, rejet des retardataires | cohérence de l'entrée avec l'entraînement |
| Déterministe au rejeu | **non** | **oui** |

Attendre que le watermark dépasse `window_end` serait mauvais pour trois raisons distinctes :

1. **Coût universel pour un risque rare** — 90 s ajoutées à *chaque* fenêtre pour se prémunir d'un retardataire
   occasionnel. Le watermark dimensionne la tolérance au retard, pas la complétude.
2. **Mauvaise granularité** — le watermark est global. Le trafic de la machine A ferait « mûrir » une fenêtre
   de la machine B qui n'a rien envoyé. Il ne dit rien du remplissage de B.
3. **Non déterministe** — le watermark dépend de l'ordre d'arrivée et du découpage en lots. La même entrée
   rejouée scorerait un ensemble de fenêtres différent, ce qui détruirait la propriété que tout le travail de
   parité des features existe pour garantir (voir D-26, où `first()`/`last()` ont été écartées pour ce motif
   exact).

## Décision

Une fenêtre est **mature** lorsque sa couverture en temps d'événement s'étend sur **toute** la fenêtre — aux
**deux** extrémités — à la cadence observée de la machine :

```
mean_step = (t_last − t_first) / (sample_count − 1)
head_gap  = t_first − window_start
tail_gap  = window_end − t_last
mature    ⇔  max(head_gap, tail_gap) ≤ mean_step × tolerance_steps    (tolerance_steps = 2)
```

**Les deux extrémités, et cela aussi a été mesuré plutôt que raisonné.** Une première version ne contrôlait que
la queue. Elle a supprimé 93 % des émissions anormales — et laissé **exactement 15 alertes pour 15 machines**,
toutes `CRITICAL`, toutes sur le **même** `window_start`, toutes avec **44 échantillons**. Ce n'étaient pas
quinze anomalies : c'était la première fenêtre de chaque machine, tronquée au **début** parce que le flux a
commencé quinze secondes après son ouverture. Une règle qui ne regarde que la fin les déclarait complètes.

Le diagnostic tient dans la répartition des fenêtres anormales distinctes : 15 à 30–39 échantillons, 15 à
40–49, 15 à 50–59 — **une par machine dans chaque classe**, soit les trois fenêtres d'ouverture de plus en plus
couvertes — et seulement 2 au-delà de 60. Ces deux dernières sont le taux de faux positifs calibré en Phase 2 ;
les quarante-cinq autres étaient un artefact de démarrage à froid.

`tolerance_steps = 2` absorbe un échantillon perdu à une extrémité plus la gigue de publication. **Ce n'est
pas un seuil de score** et cela n'interagit pas avec le point de fonctionnement du modèle.

Le démarrage à froid coûte donc quelques fenêtres par machine, pas le flux : dès qu'une fenêtre entière s'est
écoulée depuis le début du flux, la couverture est complète et le scoring reprend.

Une fenêtre immature est **publiée quand même**, avec `is_scored = false` et
`skip_reason = WINDOW_NOT_MATURE`. La sémantique du contrat est inchangée : un silence et « rien d'anormal » ne
doivent jamais se ressembler.

### Pourquoi la cadence est déduite et non configurée

C'est ce qui rend la règle utilisable sur une flotte hétérogène, sans réglage par machine :

- 1 Hz : une fenêtre complète de 60 s contient 60 échantillons, pas de 1 s, queue ≤ 2 s → mature ;
- 0,2 Hz : une fenêtre complète contient 12 échantillons, pas de 5 s, queue ≤ 10 s → mature.

Un seuil **en secondes** admettrait des fenêtres partielles de la machine rapide ou rejetterait des fenêtres
complètes de la lente. Un seuil **en nombre d'échantillons** exigerait une cadence que le job ne connaît pas —
et c'est précisément le défaut d'origine.

### Deux propriétés obtenues

- **Fonction pure des agrégats de la fenêtre** (`min`/`max` du temps d'événement, `sample_count`). Le rejeu
  produit exactement les mêmes décisions quel que soit le découpage en lots. Un contrôle par le watermark
  n'aurait pas cette propriété.
- **Monotone à l'arrivée** : une donnée tardive ne peut qu'**étendre** la couverture d'une fenêtre, vers
  l'avant ou vers l'arrière. La maturité est accordée et jamais retirée, donc une fenêtre déjà scorée ne
  redevient pas non scorable — ce qui empêche l'automate d'alerte d'osciller.

## Options écartées

| Option | Pourquoi non |
|---|---|
| Continuer à scorer les fenêtres partielles | C'est le défaut lui-même |
| Relever `MIN_SAMPLES_FOR_SCORING` | Exige la cadence, que le job ne connaît pas ; le seuil est partagé avec l'entraînement, donc le changer touche la Phase 2 |
| Réentraîner sur des fenêtres partielles | Statistiquement le plus correct, mais rouvre la Phase 2 et impose une réévaluation complète |
| Attendre le watermark | Voir ci-dessus : coût universel, mauvaise granularité, non déterministe |
| Ne contrôler que la queue | Mesuré : laisse passer les fenêtres d'ouverture et produit une tempête de démarrage à froid synchronisée sur toute la flotte |
| Filtrer sur le score | Masque le symptôme sans corriger l'entrée, et fausse la calibration du seuil |

## Conséquences

- **Latence** : le premier *score* d'une fenêtre arrive après `window_end` au lieu de ~30 s avant. Les fenêtres
  partielles restent publiées immédiatement, donc l'état de fenêtre garde sa précocité ; c'est la **décision**
  qui attend, pas la donnée.
- **Volume Kafka inchangé** : les fenêtres immatures sont toujours émises, avec un motif au lieu d'un score.
- **Alertes** : la requête d'alerte filtre déjà `is_scored = true`, donc aucune modification n'y a été
  nécessaire — les fenêtres immatures en disparaissent par construction.
- **Contrat** : ajout de la valeur `WINDOW_NOT_MATURE` à l'énumération `skip_reason` de
  `telemetry-scored.v1`. Ajout additif sur un champ optionnel, sans consommateur existant.

## Vérification

`stream-processor/tests/test_maturity.py` — fenêtre partielle non scorée, fenêtre complète scorée, minimum
exact d'échantillons, adaptation à la cadence (machine lente et machine rapide), cas dégénérés (un seul
échantillon, fenêtre vide, horodatages identiques), indépendance à l'ordre d'arrivée, donnée tardive qui rend
une fenêtre mature, monotonie de la maturité, **troncature en tête** (le cas du démarrage à froid, avec les
44 échantillons réellement observés), contrôle effectif des deux extrémités, et la chaîne complète à travers
l'agrégation Spark. Les résultats
mesurés avant/après figurent dans `docs/10-phase-3-streaming.md` § 5.7.
