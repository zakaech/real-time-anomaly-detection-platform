# Captures d'écran

**État actuel : les cinq images sont présentes**, prises le 14 septembre 2026
sur la plateforme en marche après `make demo`, avec 19 alertes en base. Elles
sont intégrées dans la section 16 du README racine.

| Fichier | Page | Ce qui est visible | Taille |
|---|---|---|---|
| `architecture.png` | — | Le schéma Mermaid du README, exporté depuis mermaid.live | 8192 × 1099 |
| `live-dashboard.png` | `/live` | 19 alertes, 15 non acquittées, compteurs par sévérité, « En direct » | 1267 × 772 |
| `alert-detail.png` | `/alerts/{id}` | M-013 CRITICAL, score 0,999676 / seuil 0,992765, 3 contributeurs, modèle et SHA-256, formulaire d'acquittement | 1111 × 856 |
| `telemetry-chart.png` | même page | Pression, moyenne par fenêtre de 60 s, anomalies marquées, infobulle | 1042 × 337 |
| `alert-history.png` | `/history` | Filtres appliqués (sévérité HIGH, statut NEW) : 4 alertes sur 19, « 1–4 sur 4 », pagination | 1099 × 373 |

Aucune n'est mise en scène ni retouchée. Ce projet a passé six phases à refuser
d'afficher ce qu'il n'a pas mesuré ; une capture recomposée, retouchée ou prise
d'un autre projet donnerait une impression de preuve sans en être une, et les
captures suivent la même règle que les chiffres.

## Les refaire

Elles se produisent depuis la plateforme réellement en marche, après :

```sh
make demo
```

Ce que chacune doit montrer :

| Fichier | Page | Ce qui doit être visible |
|---|---|---|
| `architecture.png` | — | Le schéma des composants et du flux de données |
| `live-dashboard.png` | `http://localhost:4200/live` | Les compteurs par sévérité, l'indicateur « En direct », et le flux d'alertes |
| `alert-detail.png` | `http://localhost:4200/alerts/{id}` | Le score, les contributeurs classés, le modèle et son SHA-256, le formulaire d'acquittement |
| `telemetry-chart.png` | même page, zoom sur le graphique | La courbe capteur avec les anomalies marquées en rouge, et le sélecteur de signal |
| `alert-history.png` | `http://localhost:4200/history` | Le tableau, des filtres appliqués, et la pagination |

## Comment les produire

### 1. `architecture.png`

Le schéma existe déjà sous forme de diagramme Mermaid dans le README racine
(section « Schéma d'architecture »). Deux façons de l'exporter :

- ouvrir le README sur GitHub, qui rend Mermaid nativement, et capturer le bloc ;
- coller le bloc dans [mermaid.live](https://mermaid.live) et exporter en PNG.

C'est la seule image de cette liste qui ne vient pas de l'application, et elle
reste une représentation du système réel — pas une illustration décorative.

### 2. Les quatre captures d'interface

```sh
make demo
# attendre la ligne "Plateforme prete", qui indique le nombre d'alertes reellement persistees
```

Puis, dans le navigateur :

| Capture | Chemin |
|---|---|
| `live-dashboard.png` | `http://localhost:4200/live` |
| `alert-history.png` | `http://localhost:4200/history`, après avoir appliqué au moins un filtre |
| `alert-detail.png` | cliquer une alerte depuis l'une des deux pages |
| `telemetry-chart.png` | sur la même page de détail, cadrer le graphique |

## Règles à respecter

- **Ne rien mettre en scène.** Les captures montrent les données que la
  plateforme a réellement produites, avec le nombre d'alertes qu'elle a
  réellement levé. Si une période est calme et qu'il y a trois alertes, la
  capture en montre trois.
- **Ne pas retoucher les valeurs.** Ni les scores, ni les compteurs, ni les
  horodatages.
- **Un état vide est une capture valable.** Le dashboard affiche « Aucune
  alerte » avec une explication ; c'est un comportement réel, pas un échec à
  masquer.
- **Aucune donnée personnelle** : le champ `X-Operator` est libre, ne pas y
  mettre un nom réel.
- **Format** : PNG, thème clair (celui du dashboard), assez large pour que le
  texte reste lisible (les captures actuelles font 1 042 à 1 267 px de large).

## Après avoir remplacé une image

Vérifier que les liens du README racine résolvent toujours, et mettre à jour le
tableau d'état en tête de ce fichier (contenu visible, taille) pour qu'il
décrive l'image réellement présente.
