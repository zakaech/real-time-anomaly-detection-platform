# Captures d'écran

**État actuel : aucune image n'est présente dans ce répertoire.**

Les fichiers listés ci-dessous sont *attendus*, pas fournis. Le README racine y
fait référence, et ces liens resteront cassés tant que les images n'auront pas
été produites. C'est délibéré : un lien cassé se voit et se corrige, alors
qu'une capture fabriquée — recomposée, retouchée, ou prise d'un autre projet —
donne une impression de preuve sans en être une. Ce projet a passé six phases à
refuser d'afficher ce qu'il n'a pas mesuré ; les captures suivent la même règle.

Toutes se produisent depuis la plateforme réellement en marche, après :

```sh
make demo
```

## Images attendues

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
- **Format** : PNG, largeur 1280 à 1920 px, thème clair (celui du dashboard).

## Après avoir ajouté les images

Vérifier que les liens du README racine résolvent, et retirer de ce fichier la
mention « aucune image n'est présente ».
