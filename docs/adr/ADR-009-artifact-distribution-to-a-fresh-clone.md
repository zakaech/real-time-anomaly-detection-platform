# ADR-009 — Distribution de l'artefact ML vers un clone neuf

- **Statut** : accepté (Phase 6)
- **Décisions liées** : D-48, ADR-001 (vérification de l'artefact), D-49
- **Remplace partiellement** : la règle « les artefacts sont produits, jamais committés » de `.gitignore`

## Contexte

La Phase 6 demandait un **lancement en une commande**. L'audit a montré que
c'était impossible, pour une raison mesurable et non pour une raison de confort.

| Fait vérifié | Valeur |
|---|---|
| `model.joblib` sur le disque | 222 347 octets (217,1 Kio) |
| SHA-256 du fichier | `0f608323f944e63d…c03a8d0aa5` |
| SHA-256 dans `metadata.json` | **identique** |
| Suivi par git avant la Phase 6 | **non** — `.gitignore` : `ml-training/artifacts/**/*.joblib` |
| Ce que git contenait | uniquement les 4 fichiers JSON |
| git-lfs | absent |

Le `stream-processor` monte `../ml-training/artifacts` en lecture seule sur
`/models` et lit `STREAM_ARTIFACT_DIR=/models/one_class_svm/2.0.0`. Sur un clone
neuf, ce répertoire ne contient **pas** `model.joblib`, et le job **refuse de
démarrer** — ce qui est le comportement correct défini par l'ADR-001, pas un
bug.

Le point décisif est que **l'artefact n'est pas reconstructible depuis le
dépôt**. Le ré-entraîner exige le jeu de données de deux jours (2 591 998
lignes), lui-même non versionné et lui-même produit par un run du simulateur. La
chaîne complète pour obtenir un modèle à partir d'un clone se compte en dizaines
de minutes. « Une commande » était donc factuellement faux.

## Options

| Option | Avantages | Inconvénients | Verdict |
|---|---|---|---|
| **A — versionner `model.joblib`** | 217 Kio ; « une commande » devient **vrai** ; le SHA-256 déjà présent dans `metadata.json` garantit l'intégrité | contredit la règle écrite dans `.gitignore` ; un binaire pickle dans git | **Retenu** |
| B — laisser hors git, échec explicite du bootstrap | respecte la règle initiale ; message d'erreur actionnable | « une commande » reste faux : il faut d'abord entraîner, ce qui suppose un dataset absent lui aussi | Rejeté |
| C — git-lfs | conçu exactement pour ça | de l'infrastructure pour 217 Kio, et le clone exige `git lfs install` — donc **toujours pas** une commande | Rejeté |
| D — release GitHub téléchargée au bootstrap | git reste propre | dépend du réseau et d'un dépôt publié ; le bootstrap deviendrait un installeur | Rejeté |

## Décision

**Le fichier `model.joblib` du modèle retenu est versionné**, tel qu'il a été
entraîné et validé en Phase 2. Il n'a été ni ré-entraîné, ni recalibré, ni
regénéré : le blob stocké par git est l'octet pour octet celui qui était sur le
disque, et son empreinte correspond à celle inscrite dans `metadata.json`.

L'exception dans `.gitignore` est **étroite et nominative** :

```gitignore
ml-training/artifacts/**/*.joblib
!ml-training/artifacts/one_class_svm/2.0.0/model.joblib
```

La règle générale survit. Le binaire d'`elliptic_envelope`, qui n'est chargé par
rien, reste ignoré — vérifié par `git check-ignore`.

### Pourquoi la règle initiale cède ici

Elle avait été écrite quand **rien ne dépendait de la présence de l'artefact** :
un binaire de modèle était alors un sous-produit d'entraînement, et l'ignorer ne
coûtait rien. La Phase 6 change la contrainte, parce qu'un composant du chemin
critique refuse de démarrer sans lui. Un principe qui rendait le système plus
propre le rend maintenant indémarrable ; c'est le principe qui plie, pas le
constat.

Ce qui reste vrai : ce sont bien des **sorties de build**, et le dépôt n'en
accueille qu'une, celle sans laquelle la plateforme ne fonctionne pas.

### `*.joblib binary` dans `.gitattributes`

Nécessaire, et pas décoratif. L'heuristique texte/binaire de git est une
**supposition**, et une seule conversion CRLF sur un checkout Windows
changerait l'empreinte du fichier. Le job Spark refuserait alors de démarrer —
**sur la machine du relecteur, jamais sur la nôtre**, ce qui est la pire forme
de panne. La déclaration retire la supposition.

## Conséquences

- `git clone` puis `make demo` fonctionne réellement, ce qui a été vérifié
  (voir `docs/11-phase-6-orchestration.md`).
- Le dépôt grossit de 217 Kio, une fois.
- **La vérification n'est pas déplacée, elle est doublée.** La garantie reste
  dans le job Spark (ADR-001 : empreinte, version de scikit-learn, version du
  jeu de features, ordre des colonnes). `scripts/bootstrap.sh` recalcule
  l'empreinte en amont, pour que le relecteur apprenne le problème tout de
  suite plutôt que dans une trace Spark.
- **Ce que cela ne rend pas reproductible** : versionner un pickle fige le
  *résultat* de l'entraînement, pas le processus. Le dataset qui l'a produit
  n'est pas dans le dépôt, et la graine, les empreintes des parquets et les
  versions des bibliothèques enregistrées dans `metadata.json` documentent le
  run sans permettre de le rejouer depuis un clone. C'est une limite réelle,
  écrite comme telle dans le README.
- Publier un nouveau modèle reste un acte explicite : il faudra une nouvelle
  ligne d'exception, ce qui rend le geste visible en revue.
