# 07 — Méthodologie ML : évaluation sans étiquettes, calibration, dérive

> Aucun chiffre de performance de ce document n'est un résultat. Les valeurs citées sont des **paramètres de
> conception** ou des **seuils de la littérature**. Les résultats réels seront produits par
> `ml-training/evaluate.py` et écrits dans `docs/benchmarks/`.

## 1. Le problème posé honnêtement

En production, personne n'annote « cette minute était anormale ». Nous n'avons donc :

- **aucune étiquette au moment d'entraîner** → apprentissage non supervisé ;
- **aucune étiquette au moment de scorer** → pas de mesure d'erreur en direct ;
- **des étiquettes rares, tardives et partielles ensuite** (verdict d'opérateur, rapport de maintenance).

Tout ce qui suit découle de cette contrainte. Le simulateur produit une vérité terrain, mais elle sert
**exclusivement à l'évaluation hors ligne** ; le pipeline de production est conçu comme s'il n'en existait pas.

### Le risque du simulateur complaisant

C'est la faiblesse principale du projet et il vaut mieux la traiter que l'attendre en entretien. Si j'écris le
générateur d'anomalies **et** le détecteur, je peux involontairement rendre le problème trivial — par exemple
en injectant une anomalie qui sort des plages physiques, détectable par un simple seuil.

Quatre garde-fous :

1. **Une baseline triviale est obligatoire dans chaque rapport d'évaluation** (règle 3-sigma par capteur). Si
   Isolation Forest ne la bat pas nettement, la conclusion à écrire est « le modèle n'apporte rien ici », pas de
   masquer la comparaison.
2. **Les anomalies restent dans les plages physiques valides.** Une température de 812 °C est un bug de
   capteur, pas une anomalie machine. Les anomalies intéressantes sont **relationnelles** : puissance qui monte
   à régime constant, vibration qui dérive lentement sur dix minutes.
3. **Bruit et anomalie sont générés par des modules indépendants**, avec des générateurs aléatoires distincts.
   Un bruit corrélé à l'injection créerait un indice parasite.
4. **Un jeu d'anomalies non vues à l'entraînement** est réservé à l'évaluation finale.

## 2. Modèle et features

### 2.1 Choix de l'algorithme

| Candidat | Retenu | Raison |
|---|---|---|
| **Isolation Forest** | **oui, modèle principal** | inférence `O(log n)` par arbre, sans données d'entraînement embarquées, sans hypothèse de distribution ; c'est ce qui le rend viable en streaming |
| Enveloppe robuste (Mahalanobis) | oui, **baseline** | rapide, interprétable, capte les corrélations linéaires ; référence à battre |
| Règle 3-sigma par capteur | oui, **baseline triviale** | plancher de comparaison obligatoire |
| One-Class SVM | non | entraînement en `O(n²)` à `O(n³)`, inférence dépendante des vecteurs support ; ne passe pas l'échelle |
| LOF | non | exige le voisinage au moment de l'inférence → transporter le jeu d'entraînement dans chaque exécuteur |
| Autoencodeur | non | hors stack imposée, et injustifiable sur 30 features tabulaires |

Isolation Forest isole les points anormaux par partitionnement aléatoire : un point atypique demande moins de
coupes pour être isolé. Ce qui compte ici : **le modèle sérialisé est un ensemble d'arbres, quelques centaines
de kilo-octets, sans données**. Il se diffuse à chaque exécuteur Spark sans coût.

`contamination='auto'`. On **ne fixe pas** la proportion d'anomalies au niveau du modèle : le modèle produit un
score, la politique produit une décision. Mélanger les deux rend impossible de changer de seuil sans
réentraîner.

### 2.2 Features (~30, par machine et par fenêtre de 60 s)

| Famille | Exemples | Ce que cela capte |
|---|---|---|
| Position | `*_mean`, `*_median` | niveau moyen |
| Dispersion | `*_std`, `*_iqr`, `*_range` | instabilité, à niveau moyen normal |
| Tendance | `(last − first) / 60` | dérive progressive |
| Extrêmes | `*_min`, `*_max` | pic transitoire noyé dans la moyenne |
| **Ratios physiques** | `power_kw / rotation_rpm`, `vibration / rotation_rpm` | **signature de frottement mécanique** |
| Qualité | `sample_count`, `null_ratio` | dégradation de la collecte |

Les ratios sont le cœur du dispositif. Une hausse de puissance **à régime constant** signe un frottement accru,
alors que puissance et régime pris séparément peuvent chacun rester dans leurs plages nominales. C'est ce type
d'anomalie qu'un seuil par capteur ne peut structurellement pas voir — et c'est ce qui justifie un modèle
multivarié plutôt qu'une table de seuils.

L'état machine est utilisé en **filtre**, pas en feature : seules les fenêtres `RUNNING` avec
`sample_count >= 30` sont scorées. Encoder l'état comme feature demanderait au modèle d'apprendre plusieurs
régimes dans un espace unique ; le filtrer garde un modèle spécialisé sur un régime homogène, ce qui est à la
fois plus simple et plus juste.

### 2.3 Éviter le décalage entraînement/service

Le risque classique : les features de l'entraînement et celles du scoring divergent (une moyenne calculée sur
60 s ici, 50 s là), et le modèle voit en production une distribution qu'il n'a jamais apprise. La panne est
silencieuse : rien ne plante, les scores sont simplement faux.

Trois mesures :

1. **Un seul module de features** — `libs/telemetry-core/features.py` — importé par `ml-training` **et** par
   `stream-processor`. Une seule définition, jamais deux implémentations à synchroniser.
2. **Le `StandardScaler` est dans le `Pipeline` sklearn sérialisé**, jamais appliqué à part. Les moyennes et
   écarts-types de normalisation voyagent avec le modèle.
3. **L'ordre des colonnes est figé dans `metadata.json`** et vérifié au chargement. Une matrice NumPy n'a pas de
   noms de colonnes : deux features permutées produisent des scores plausibles et faux.

## 3. Calibration du score et choix du seuil

### 3.1 Du score brut au score calibré

`IsolationForest.score_samples` renvoie une valeur négative non bornée, dont l'échelle dépend du jeu
d'entraînement. Elle n'est comparable ni entre deux versions du modèle, ni interprétable par un humain.

À l'entraînement, on calcule la **fonction de répartition empirique (ECDF)** des scores bruts sur le jeu de
référence, et on stocke une grille de 1000 quantiles dans l'artefact. En production :

```
anomaly_score = ECDF_référence(−raw_score)      ∈ [0, 1]
```

Trois bénéfices :

1. **Interprétabilité** : `0.995` signifie « plus extrême que 99,5 % des fenêtres de référence ». Un opérateur
   comprend cette phrase ; il ne comprend pas `raw_score = −0,0731`.
2. **Le seuil devient un budget d'alertes.** `seuil = 0,995` ⇒ environ 0,5 % des fenêtres alertent **si la
   distribution est inchangée**. À 20 machines × 6 fenêtres/minute, cela donne un ordre de grandeur d'alertes
   par heure calculable à l'avance, à confronter à la capacité réelle d'un opérateur.
3. **Le taux d'alerte devient un détecteur de dérive.** Le taux attendu est connu par construction ; s'il passe
   à 5 %, ce n'est pas que l'usine est en panne, c'est que la distribution d'entrée a bougé. Cette propriété
   tombe directement de la calibration, sans instrumentation supplémentaire.

### 3.2 Choisir le seuil

Trois méthodes, par ordre de solidité décroissante en l'absence d'étiquettes :

| Méthode | Applicable en production | Limite |
|---|---|---|
| **Budget d'alertes** (quantile) | **oui, sans étiquettes** | ne dit rien du recall |
| Coût attendu `C_FP·FP + C_FN·FN` | oui, si les coûts sont connus | exige de chiffrer un arrêt machine |
| Maximisation de F-beta | **non**, exige des étiquettes | utile hors ligne uniquement |

**Retenu : budget d'alertes en primaire, validé hors ligne par les étiquettes injectées.** C'est la seule
méthode transposable en production, et elle part de la bonne question — « combien d'alertes un opérateur
peut-il traiter par heure ? » — plutôt que d'une métrique statistique abstraite.

La validation hors ligne ne sert pas à choisir le seuil, elle sert à **documenter ce que le budget achète** :
pour un seuil de 0,995, quel recall par épisode obtient-on ? La courbe recall-vs-budget est le livrable
d'évaluation, pas un seuil unique.

Hystérésis : une alerte n'est émise qu'après **2 fenêtres consécutives** au-dessus du seuil (D-12). Une fenêtre
isolée au-delà du seuil est le plus souvent du bruit ; à 10 s de glissement, cela ajoute 10 s de latence et
supprime une part importante des alertes ponctuelles. Le nombre exact sera arbitré par la mesure du couple
(latence de détection, taux de faux positifs).

### 3.3 Seuil global ou par machine

| | Global | Par machine |
|---|---|---|
| Données nécessaires | l'ensemble de la flotte | historique propre à chaque machine |
| Machine nouvelle | fonctionne d'emblée | démarrage à froid |
| Machines hétérogènes | mal traité | bien traité |
| Exploitation | 1 valeur | N valeurs à surveiller |

**Version 1 : un modèle global, un seuil global, avec normalisation par machine des features.** Chaque feature
est centrée-réduite de façon robuste (médiane, IQR) par un profil par machine calculé à l'entraînement. Une
machine qui tourne naturellement plus chaud n'est alors plus anormale par construction.

C'est le meilleur compromis : le modèle apprend sur toute la flotte (plus de données, meilleure généralisation)
tout en respectant les particularités de chaque machine. Repli explicite pour une machine inconnue : profil
moyen de la flotte, et l'événement scoré porte `profile_fallback: true` pour que ces scores soient
identifiables. Voir D-07.

## 4. Évaluer un détecteur non supervisé

### 4.1 Hors ligne, avec les étiquettes du simulateur

**Métriques retenues**

| Métrique | Pourquoi elle |
|---|---|
| **PR-AUC** | avec ~1 % d'anomalies, la ROC-AUC est trompeuse : le taux de faux positifs varie peu quand les négatifs sont écrasants, ce qui donne des valeurs flatteuses même pour un mauvais modèle |
| **Recall par épisode** | un exploitant veut savoir si la panne a été détectée, pas si 80 % de ses points l'ont été |
| **Latence de détection** | `première_alerte − episode_started_at`. Détecter une usure de roulement 3 heures avant la casse ou 30 secondes avant n'a pas la même valeur |
| **Faux positifs par machine et par jour** | la métrique qui décide si le système sera utilisé ou ignoré |
| **Précision au budget fixé** | qualité à charge de travail constante |

**Métriques écartées et pourquoi** : l'accuracy (99 % en prédisant « rien d'anormal »), la ROC-AUC seule, et le
F1 point par point (récompense un modèle qui alerte longtemps sur un même épisode plutôt qu'un modèle qui
détecte tôt).

**Protocole de découpage** : séparation **temporelle**, jamais aléatoire. Entraînement sur les jours 1 à 5,
validation sur le jour 6, test sur le jour 7. Un découpage aléatoire placerait des points adjacents à quelques
secondes d'intervalle de part et d'autre de la séparation — une fuite qui gonfle artificiellement les
résultats.

**Entraînement sur données propres** : le jeu d'entraînement exclut les épisodes étiquetés. Isolation Forest
tolère une contamination faible, mais entraîner sur des anomalies revient à lui apprendre qu'elles sont
normales.

### 4.2 En production, sans étiquettes

C'est le cœur du sujet. Quatre familles de signaux, aucune n'étant une mesure directe de la qualité :

**a. Taux d'alerte contre taux attendu.** Connu par construction grâce à la calibration ECDF. Un écart durable
signale un changement de distribution, pas nécessairement une dégradation du modèle — mais toujours quelque
chose à examiner.

**b. Retour opérateur : la seule vraie mesure de précision.** Chaque `dismiss` produit une étiquette négative,
chaque `resolve` avec cause racine une étiquette positive. On calcule alors :

```
précision_observée = RESOLVED / (RESOLVED + DISMISSED)
```

Deux biais qu'il faut nommer plutôt que masquer : les opérateurs ne traitent pas toutes les alertes (biais de
sélection), et ils peuvent se tromper. C'est une **estimation**, présentée comme telle dans le dashboard.

**c. Stabilité des scores.** Distribution des scores par machine dans le temps ; accord entre Isolation Forest
et la baseline Mahalanobis. Un taux de désaccord qui grimpe signale une dérive de l'un des deux, sans dire
lequel — c'est un signal de vigilance, pas un diagnostic.

**d. Le recall reste non mesurable.** Il faut le dire clairement : **sans registre de maintenance, on ne peut
pas savoir ce qu'on a manqué.** Toute affirmation sur le recall en production serait une invention. La voie
pour l'obtenir est le rapprochement avec la GMAO : pour chaque intervention corrective enregistrée, une alerte
a-t-elle précédé ? C'est prévu par le topic `telemetry.labels` avec `source: "MAINTENANCE_LOG"`, mais aucune
GMAO n'est branchée ici.

## 5. Dérive du modèle

### 5.1 Distinguer trois dérives

| Type | Définition | Ce qu'il faut faire |
|---|---|---|
| **Dérive de données** — `P(X)` change | la distribution d'entrée bouge | investiguer |
| **Dérive de concept** — `P(y\|X)` change | « anormal » ne veut plus dire la même chose | réentraîner |
| **Dérive de score** | la distribution des scores bouge | conséquence, pas cause |

### 5.2 Le piège spécifique à ce domaine

**Ici, la dérive est parfois exactement le signal recherché.** Une machine dont la vibration monte lentement
sur trois semaines produit une dérive de distribution — et c'est précisément l'usure qu'on veut détecter.
Réentraîner sur ces données apprendrait au modèle que la machine dégradée est normale : on supprimerait la
détection au moment où elle devient utile.

Le critère de distinction :

| | Dérive à détecter (anomalie) | Dérive à corriger (modèle obsolète) |
|---|---|---|
| Étendue | une ou deux machines | **toute la flotte** |
| Cinétique | jours ou semaines | mois, ou saut brutal après une intervention |
| Cause | usure mécanique | saison, changement de produit, mise à jour d'automate |

**Règle opérationnelle : la dérive n'est calculée que sur la population de machines sans alerte active**, et un
réentraînement n'est déclenché que si elle touche une majorité de la flotte. Une dérive localisée est un
résultat de détection, pas un problème de modèle.

### 5.3 Mesure

**PSI** (Population Stability Index) par feature, entre la fenêtre glissante de 7 jours et le profil de
référence stocké dans l'artefact :

```
PSI = Σ (p_observé − p_référence) · ln(p_observé / p_référence)
```

Seuils usuels de la littérature (non mesurés ici) : `< 0,10` stable, `0,10–0,25` à surveiller, `> 0,25`
significatif. Complété par un test de Kolmogorov-Smirnov, plus sensible aux changements de forme.

Calcul par un job batch quotidien lisant `telemetry.scored`, résultats écrits dans `drift_metric` et affichés
dans le dashboard. La reproductibilité vient de ce que le **profil de référence est figé dans l'artefact** : on
compare toujours au même point d'origine, pas à la veille — sinon une dérive lente devient invisible, chaque
jour ressemblant au précédent.

### 5.4 Politique de réentraînement

**Déclencheurs** : périodique (hebdomadaire), ou PSI > 0,25 sur au moins 2 features **sur la flotte saine**, ou
taux d'alerte hors de la bande `[0,2×, 5×]` de l'attendu pendant 24 h, ou précision observée en baisse
soutenue.

**Procédure** — c'est un mécanisme de type *champion/challenger* :

1. entraînement d'une version candidate sur une fenêtre glissante récente, épisodes anormaux exclus ;
2. évaluation hors ligne sur le **même** jeu de test que le champion — une comparaison sur deux jeux différents
   ne compare rien ;
3. **le candidat doit battre le champion et la baseline triviale**, sinon il est refusé ;
4. promotion par configuration : `MODEL_VERSION=1.4.0`, redémarrage du job Spark. Le checkpoint reste valide
   (voir `docs/04-streaming-semantics.md` §3.3), donc la reprise se fait aux offsets exacts ;
5. `model_version.is_active` bascule en base, avec unicité garantie par index partiel ;
6. rollback = redémarrage avec la version précédente. Les artefacts sont **immuables et conservés**.

**Limite assumée** : pas de canary ni d'A/B au fil de l'eau, conséquence directe de l'ADR-001 (scoring
embarqué). Ce qu'on peut faire, et qui reste utile, c'est du **shadow scoring** : publier les deux scores dans
`telemetry.scored` pendant une période, comparer hors ligne, et ne promouvoir qu'ensuite. Le coût est un
doublement de l'inférence, acceptable à cette échelle.

## 6. Contenu de l'artefact

Un modèle est un dossier immuable, pas un fichier :

```
models/isolation_forest/1.3.0/
├── model.joblib          Pipeline sklearn complet (scaler + estimateur)
├── metadata.json         version, date, versions de librairies, sha256, ordre des colonnes,
│                         hyperparamètres, seuil, métriques hors ligne
├── calibration.json      grille ECDF (1000 quantiles)
├── machine_profiles.json médiane et IQR par machine, plus profil de repli
└── reference_profile.json histogrammes par feature pour le calcul de PSI
```

Deux propriétés à défendre : **immuabilité** (une version publiée n'est jamais modifiée ; une correction est une
nouvelle version) et **auto-description** (le job Spark vérifie au démarrage le SHA-256, la version de
scikit-learn et l'ordre des colonnes ; en cas d'écart il **échoue au démarrage** plutôt que de scorer avec un
modèle incertain).

C'est ce qui rend possible, six mois plus tard, de reprendre une alerte, de retrouver l'artefact exact via
`model.version`, et de rejouer le calcul à l'identique à partir des `features` stockées avec l'alerte.
