# 00 — Choix du domaine métier

> **Convention de lecture** : aucun chiffre de performance de ce dépôt n'est mesuré tant qu'il n'est pas
> publié dans `docs/benchmarks/`. Tout nombre présent dans les documents de conception est une **cible de
> dimensionnement** ou une **hypothèse de travail**, explicitement marquée comme telle.

## Recommandation : télémétrie industrielle (maintenance prédictive)

Je recommande de **garder le domaine industriel** et d'écarter le flux de transactions bancaires.
Les deux sont défendables, mais un seul est cohérent avec la stack imposée. Voici le raisonnement.

### 1. La contrainte décisive : l'architecture doit correspondre au SLA du domaine

La détection de fraude réelle est une **décision synchrone dans le chemin d'autorisation du paiement** :
le réseau carte attend une réponse en quelques dizaines de millisecondes. Or Spark Structured Streaming est
un moteur **micro-batch** : sa latence plancher se compte en secondes, et la génération d'alerte passe ici par
une fenêtre d'agrégation puis un topic Kafka puis une base relationnelle. Construire un pipeline asynchrone
Kafka → Spark → PostgreSQL pour de la fraude, c'est produire une architecture que l'on ne peut pas défendre
en entretien : la première question sera *« comment bloquez-vous la transaction ? »*, et la réponse honnête
serait *« on ne la bloque pas »*.

Il existe bien un usage asynchrone légitime de la fraude (revue post-autorisation, file de cas à instruire),
mais il faut alors le dire et assumer que le projet ne traite pas le cas d'usage principal. C'est un handicap
gratuit.

À l'inverse, la supervision industrielle a **nativement** ce profil : latence acceptable de quelques secondes
à une minute, alerte asynchrone, opérateur humain qui acquitte. L'architecture ne se justifie pas, elle
s'impose.

### 2. La détection non supervisée est le bon outil ici, et le mauvais outil là-bas

En maintenance prédictive, **il n'y a réellement pas d'étiquettes** : personne n'annote « cette minute était
anormale ». Un détecteur non supervisé (Isolation Forest, enveloppe robuste) est l'état de l'art pragmatique.

En fraude, les étiquettes existent — les impayés et les contestations remontent avec quelques jours de délai —
et l'industrie utilise du **supervisé** (gradient boosting) avec des règles. Choisir Isolation Forest sur des
transactions expose à une critique immédiate : *« pourquoi ne pas apprendre sur les étiquettes que vous avez ? »*.
La question « comment évalue-t-on sans étiquettes ? » que tu veux traiter devient artificielle.

### 3. Les signaux se prêtent aux fenêtres glissantes

La télémétrie est un **signal continu échantillonné** : moyenne, écart-type, pente, RMS de vibration sur 60 s
ont un sens physique direct, et une dégradation de roulement n'est détectable *que* sur une fenêtre — c'est une
anomalie collective, pas ponctuelle. Cela justifie pleinement Spark et son état.

Les transactions sont des **événements discrets et irréguliers** : les features utiles sont des compteurs par
entité (nombre de transactions par carte sur 1 h, distance géographique depuis la précédente). C'est faisable,
mais on perd la lisibilité physique des features et l'explicabilité de l'alerte.

### 4. Temps d'événement et données en retard sont authentiques ici

Un capteur industriel passe par une passerelle qui bufferise, perd le réseau, se resynchronise : les
événements arrivent **réellement en retard et dans le désordre**, avec des horloges qui dérivent. Le
watermarking n'est pas un exercice de style, c'est une nécessité.

Une transaction bancaire est horodatée par un système central fiable ; simuler du retard massif serait
artificiel.

### 5. Le workflow d'acquittement est plus crédible

Un opérateur de ligne qui acquitte une alerte, y met un code de cause racine et la clôture, c'est un poste
qui existe (conduite d'installation, GMAO). Le retour de l'opérateur devient une **étiquette a posteriori**
que l'on réinjecte dans le pipeline — la boucle se referme proprement.

### Ce qu'on perd en écartant la fraude

Il faut être honnête sur le coût de ce choix :

- La fraude est **plus attractive à la lecture d'un CV** dans un contexte finance/assurance.
- Il existe des jeux de données publics étiquetés (transactions cartes) permettant une évaluation sur données
  réelles, alors qu'ici la vérité terrain vient d'un simulateur — donc d'un processus que **nous** avons écrit.
  C'est la faiblesse principale de ce choix et je la traite explicitement dans `docs/07-ml-methodology.md`
  (« le risque du simulateur complaisant » et les garde-fous : baseline triviale obligatoire, anomalies non
  triviales, séparation stricte des générateurs de bruit et d'anomalie).

### Coût d'un changement d'avis ultérieur

Faible et borné. Seuls changent : `event-simulator`, le module `features`, le contenu du contrat de données et
le libellé métier des tables. Kafka, la sémantique de streaming, le service Spring, le dashboard et toute la
partie MLOps sont identiques. Si tu veux basculer, c'est ~20 % du code.

**Décision proposée : domaine industriel. Elle t'appartient — voir D-01 dans `docs/09-open-decisions.md`.**

## Cadrage fonctionnel retenu

- Une ligne de production, **20 machines** (hypothèse de dimensionnement), 5 grandeurs mesurées par machine.
- Échantillonnage **1 Hz par machine** en régime nominal → 20 événements/s (cible nominale, à mesurer).
- Un mode « charge » du simulateur permet de monter le débit pour éprouver le backpressure (cible de test :
  ×10, à confirmer par mesure réelle — voir `docs/04-streaming-semantics.md`).
- Taxonomie des anomalies injectées : ponctuelle (pic), contextuelle (valeur normale dans l'absolu mais
  anormale pour cet état machine), collective/séquentielle (dérive lente de roulement).
