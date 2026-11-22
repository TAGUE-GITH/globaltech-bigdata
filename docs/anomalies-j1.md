# Anomalies de la livraison J1 (échantillon)

Mesurées sur la couche Bronze par `src/exploration.py`, qui ne modifie aucune donnée :

```powershell
docker exec spark-client spark-submit src/exploration.py J1
```

Les pourcentages sont calculés sur le nombre de lignes de la source. La dernière colonne de chaque tableau est une **proposition** de traitement en Silver, à valider par la personne responsable de la source.

Vocabulaire des traitements :

- **Rejeter** : la ligne va dans `/datalake/rejects` avec son motif.
- **Corriger** : la valeur est normalisée, la ligne est gardée. La valeur d'origine reste disponible en Bronze.
- **Dédupliquer** : la ligne est retirée et comptée comme doublon.

## Commandes (101 200 lignes)

### Structure

| Anomalie | Nombre | Exemple | Proposition |
|---|---|---|---|
| Mauvais nombre de champs | 56 | `ORD…,…,WH-FR-01` (tronquée) ; `…,COMPLETED,EXTRA,42` (champs en trop) | Rejeter, motif `structure_invalide` |
| dont séparateur `;` | 16 | `ORD0000000772;2026-01-15T16:24:08;…` | Corriger : relire la ligne avec `;`. Sinon rejeter |

### Doublons

| Anomalie | Nombre | Proposition |
|---|---|---|
| Lignes strictement identiques | 1 000 | Dédupliquer (garder une ligne) |
| `order_id` présents plusieurs fois | 1 186 identifiants, 1 199 lignes en trop | Voir ci-dessous |
| dont même `order_id` avec un contenu différent | 199 identifiants | Le plus souvent, seul le `status` change (ex. `SHIPPED` puis `RETURNED`) : c'est une mise à jour. Garder le statut le plus avancé, compter l'autre comme doublon |

### Valeurs manquantes

| Colonne | Nombre | Proposition |
|---|---|---|
| `customer_id` | 330 | Rejeter (`client_manquant`) : la commande ne peut pas être rattachée |
| `unit_price_eur` | 195 | Rejeter (`prix_manquant`) : le chiffre d'affaires serait faux |
| `country` | 108 | Rejeter (`pays_manquant`), ou déduire le pays de `warehouse_id` (`WH-FR-01` → `FR`) : à décider |
| `quantity` | 31 | Rejeter (`quantite_manquante`) |
| `order_ts` | 16 | Rejeter (`date_manquante`) |

### Valeurs hors règles

| Anomalie | Nombre | Exemples | Proposition |
|---|---|---|---|
| `order_ts` dans un autre format | 264 | `2026-01-28 10:15:23`, `14/01/2026 17:56:16`, `1768233374000` (millisecondes depuis 1970) | Corriger : essayer chaque format connu |
| `order_ts` illisible | inclus ci-dessus | `hier` | Rejeter (`date_illisible`) |
| `order_ts` dans le futur | 18 | année 2030 | Rejeter (`date_future`) |
| `order_ts` en décembre 2025 | 490 | `2025-12-26T10:05:27` | À décider : commandes en retard (à garder) ou erreur (à rejeter) |
| `quantity` non numérique | 80 | `2 pcs`, `NaN`, `abc` | Rejeter (`quantite_invalide`) ; `2 pcs` pourrait être corrigé en `2` |
| `quantity` = 0 | 100 | | Rejeter (`quantite_nulle`) |
| `quantity` négative | 202 | `-1`, `-220` | Rejeter (`quantite_negative`) |
| `unit_price_eur` négatif | 60 | `-84.10` | Rejeter (`prix_negatif`) |
| `status` en minuscules | 113 | `completed`, `cancelled` | Corriger : mettre en majuscules |
| `country` mal écrit | 509 | ` FR ` (espaces), `fr`, `FRA`, `France`, `Deutschland`, `España`, `USA` | Corriger : table de correspondance vers le code ISO à 2 lettres |
| Client inconnu (absent du fichier clients) | 198 | `C9648244` | Rejeter (`client_inconnu`) |
| Produit inconnu (absent du fichier produits) | 268 | `P995038` | Rejeter (`produit_inconnu`) |

Aucune anomalie trouvée sur `channel` ni sur le format de `unit_price_eur`.

### Déséquilibre de clé (skew)

| Mesure | Valeur |
|---|---|
| Produit le plus fréquent | `P00042` : 25 286 lignes, soit 25 % des commandes |
| Médiane par produit | 79 lignes |
| **Ratio max / médiane** | **320** |

Une jointure ou une agrégation par `product_id` enverra un quart des données à une seule tâche Spark. À traiter au Lot 7 (deux méthodes de correction). `P00042` a aussi deux versions de prix (49,22 € depuis le 2025-10-01, puis 43,62 € depuis le 2026-01-09) : c'est un bon cas de test pour le prix à la date de commande.

## Clients (5 127 lignes)

| Anomalie | Nombre | Proposition |
|---|---|---|
| Lignes strictement identiques | 83 | Dédupliquer |
| `customer_id` présents plusieurs fois | 127 | Même `updated_at` : comparer les colonnes qui diffèrent et garder la version la plus complète |
| `email` manquant | 142 | Garder : l'e-mail n'est pas utile à l'analyse et sera pseudonymisé |
| `email` au format invalide | 29 | Garder, signaler ; pseudonymiser comme les autres |
| `country` manquant | 111 | Garder avec pays « inconnu » : ne pas perdre les commandes de ces clients |
| `segment` manquant | 53 | Garder avec segment « inconnu » |
| `phone` manquant | 57 | Garder |
| Données personnelles | toutes les lignes | `contact_first_name`, `contact_last_name`, `email`, `phone` à pseudonymiser |

## Produits (592 versions, 500 produits)

| Constat | Nombre | Proposition |
|---|---|---|
| Produits à 1 / 2 / 3 versions | 413 / 82 / 5 | Historiser en SCD type 2 : `date_debut` = `updated_at`, `date_fin` = début de la version suivante |
| Prix au format français (`"205,16"`) | 2 | Corriger : remplacer la virgule par un point |
| Versions en double (même produit, même date) | 9 | Dédupliquer ; garder l'`extracted_at` le plus récent |
| Produits ayant changé de famille | 11 | Normal : c'est ce que le SCD type 2 doit conserver |

## Logs web (50 000 lignes)

| Constat | Nombre | Proposition |
|---|---|---|
| Lignes au format Apache combined | 49 809 (99,6 %) | Découper en colonnes |
| Lignes vides | 20 | Rejeter (`ligne_vide`) |
| Lignes mal formées | 171 | Entrées coupées en deux lignes (`192.0.2.7 - - [01/Jan/2026:` puis la suite sur la ligne suivante). Rejeter (`format_invalide`) |
| Codes HTTP | 200 = 46 025 ; 404 = 1 732 ; 500 = 499 ; 503 = 5 ; autres = 1 548 | Base de l'indicateur « taux d'erreurs 404 et 500 par jour » du Lot 4 |

## Capteurs (120 fichiers)

| Constat | Nombre | Proposition |
|---|---|---|
| Lignes JSON tronquées | 27 | Écartées par le streaming (Lot 5), à compter |

## Ce que l'on ne sait pas encore

- L'échantillon contient-il toutes les anomalies des vraies livraisons ? À revérifier sur J1 complet, en relançant le même programme.
- J2 et J3 peuvent apporter un **changement de schéma** (colonne ajoutée ou renommée) : le Bronze le conserve déjà, le Silver devra le gérer.
