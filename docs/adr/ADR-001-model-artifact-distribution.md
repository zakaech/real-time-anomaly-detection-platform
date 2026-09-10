# ADR-001 — Distribution et vérification de l'artefact de modèle

- **Statut** : accepté (Phase 3)
- **Décisions liées** : D-35 (installation de `ml-training` dans l'image), D-16 (épinglage des versions)

## Contexte

Le job Spark doit scorer chaque fenêtre avec le modèle entraîné en Phase 2. Deux questions distinctes se
posent : **comment le modèle atteint les workers**, et **comment on garantit que c'est le bon modèle**.

La seconde est la plus dangereuse. Toutes les défaillances possibles ici partagent un mode d'échec : l'artefact
se charge, score, et se trompe sans que rien ne lève d'exception.

- Dépicklage d'un estimateur scikit-learn entre deux versions : la bibliothèque **n'a jamais garanti** de lever
  une erreur. Elle peut charger et prédire différemment.
- Une matrice NumPy ne porte pas de noms de colonnes. Scorer une matrice dont les colonnes sont permutées
  produit des nombres parfaitement plausibles.
- Un fichier corrompu ou remplacé n'est signalé par rien.

## Options

**Distribution.**

| Option | Description | Verdict |
|---|---|---|
| A | `spark.sparkContext.broadcast` de l'objet ajusté | Fonctionne à cette taille, mais un singleton par processus reste nécessaire pour le reste |
| B | Singleton de module, chargé au premier usage dans chaque worker | **Retenu** |
| C | Rechargement à chaque partition | Écarté : dépicklage d'un SVM à plusieurs centaines de vecteurs de support par lot |

**Vérification.**

| Option | Description | Verdict |
|---|---|---|
| A | Confiance : l'artefact est dans l'image, il est donc correct | Écarté |
| B | Avertissement au démarrage en cas d'écart | Écarté : un avertissement dans un log de driver n'arrête rien |
| C | Échec bloquant au chargement | **Retenu** |

## Décision

**B + C.** Un singleton de module par processus Python worker, avec verrouillage à double vérification —
plusieurs lots Arrow peuvent atteindre le chargeur simultanément dans un même worker, et dépickler deux fois
annulerait l'intérêt du cache. Ce qui traverse vers les workers est le **chemin et l'empreinte attendue**
(`ModelSpec`), pas l'objet.

Quatre contrôles bloquants, tous dans `ml_training.artifact.load_artifact`, tous **avant** le dépicklage :

1. **SHA-256** du binaire contre l'empreinte enregistrée à l'entraînement ;
2. **version de scikit-learn** exacte ;
3. **version du jeu de features** (`FEATURE_SET_VERSION`) ;
4. **ordre exact des colonnes** (`FEATURE_NAMES`).

En cas d'écart, le job **échoue au démarrage**. Un job qui refuse de démarrer est un incident visible ; un job
qui score avec le mauvais modèle est un incident que personne ne remarque.

Le `stream-processor` ne re-vérifie rien. Une révision antérieure répétait la comparaison de version de jeu de
features dans `model.py` : les deux côtés lisant la même constante, la branche était **inatteignable** et
suggérait une seconde ligne de défense qui n'existait pas. Elle a été retirée.

## Conséquences

- L'image du job de streaming dépend de `ml-training` (D-35). Dépendance de sérialisation, non
  d'architecture : elle disparaîtrait avec un format d'export neutre, au prix d'une conversion à valider.
- ~~Le binaire n'est pas versionné dans Git (sortie de build) ; seules ses métadonnées le sont.~~
  **Révisé en Phase 6 — voir ADR-009.** Le binaire du modèle retenu **est** désormais versionné : sans lui, un
  clone neuf ne pouvait pas démarrer, et il n'est pas reconstructible depuis le dépôt puisque le jeu de données
  qui l'a produit ne l'est pas non plus. Conséquence directe : les tests qui chargent l'artefact réel
  **s'exécutent** au lieu de s'abstenir. Les quatre tests de rejet restent écrits pour s'exécuter **sans** lui —
  la vérification lisant les métadonnées avant de dépickler, un fichier factice suffit à atteindre chaque
  contrôle.
  **Ce que la présente ADR conserve intégralement** : la vérification au démarrage. Versionner l'artefact ne
  remplace aucun contrôle, cela déplace seulement la façon dont il arrive sur la machine.
- Changer de modèle impose de reconstruire l'image ou de remonter le volume d'artefacts. C'est voulu :
  l'artefact est immuable une fois publié.

## Vérification

`stream-processor/tests/test_model.py` — cinq rejets (binaire altéré, version scikit-learn, version de jeu de
features, colonnes permutées, colonne manquante), plus, sur l'artefact réellement livré, le chargement
nominal, l'identité du singleton et le rejet d'un seul octet inversé dans le pickle.
