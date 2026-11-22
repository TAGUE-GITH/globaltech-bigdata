# Couche Silver

## Rôle

Transformer les données brutes du Bronze en données **fiables** : chaque colonne a son vrai type, chaque ligne respecte les règles métier, les doublons sont retirés, les données personnelles sont pseudonymisées. Ce qui est écarté n'est jamais perdu : chaque ligne rejetée est rangée avec son motif.

## Modèle commun à toutes les sources

Les outils partagés sont dans `src/qualite.py`. Chaque source (logs, commandes, clients, produits) a son propre programme `src/silver_<source>.py`, qui suit les mêmes étapes :

| Étape | Fonction | Rôle |
|---|---|---|
| 1 | `lire_bronze` | Lire la partition Bronze de la livraison |
| 2 | *(propre à la source)* | Découper, typer, normaliser les colonnes |
| 3 | `separer` | Appliquer **toutes** les règles qualité **en un seul passage** |
| 4 | `dedupliquer` | Retirer les doublons parmi les lignes valides |
| 5 | `ecrire_partition`, `ecrire_rejets` | Écrire les lignes propres et les rejets |
| 6 | `bilan` | Vérifier **propres + rejets + doublons = total** |

### Les règles en un seul passage

Une règle est un couple `(motif, condition)`, où la condition est **vraie quand la ligne est invalide**. Exemple :

```python
("quantite_negative", F.col("quantity") < 0)
```

`separer` combine toutes les conditions dans une seule expression (`coalesce` de `when`) : la ligne reçoit le motif de la **première** règle qu'elle enfreint. Spark évalue cette expression en une seule lecture des données, quel que soit le nombre de règles. L'approche naïve (un filtre puis un comptage par règle) relirait les données autant de fois qu'il y a de règles.

L'ordre des règles compte : on met d'abord les défauts de structure (ligne vide, format invalide), puis les valeurs manquantes, puis les valeurs hors règles.

### Format commun des rejets

Tous les rejets vont dans `/datalake/rejects/<source>`, partitionnés par `date_ingestion`, avec les mêmes colonnes :

| Colonne | Contenu |
|---|---|
| `motif_rejet` | Le nom de la règle enfreinte |
| `contenu` | La ligne rejetée, en JSON, pour pouvoir l'examiner ou la corriger puis la rejouer |
| `_livraison`, `_fichier_source` | D'où vient la ligne |
| `_rejete_le` | Date et heure du rejet |

### Pseudonymisation

`pseudonymiser` remplace une donnée personnelle par son empreinte SHA-256, calculée avec un **sel** (une chaîne secrète ajoutée avant le calcul, dans `conf/app.json`) :

- La même valeur donne toujours la même empreinte : on peut encore compter les visiteurs distincts ou relier les commandes d'un même client.
- On ne peut pas retrouver la valeur d'origine à partir de l'empreinte.
- Sans le sel, quelqu'un pourrait calculer l'empreinte de toutes les adresses IP possibles et retrouver les vraies. Le sel l'en empêche.

Limite assumée : dans ce prototype, le sel est dans le dépôt Git. En production, il serait stocké dans un coffre à secrets, hors du code.

### Rejeu

Comme pour le Bronze, l'écriture `overwrite` en mode `dynamic` ne remplace que la partition de la livraison traitée : relancer le Silver de J1 ne crée aucun doublon.

## Logs web

```powershell
docker exec spark-client spark-submit src/silver_logs.py J1
```

### Transformation

Chaque ligne au format Apache combined est découpée en colonnes typées :

| Colonne | Type | Exemple |
|---|---|---|
| `ip_pseudo` | texte | empreinte SHA-256 de l'adresse IP |
| `horodatage` | horodatage | 2026-01-07 14:21:03 (heure de Paris) |
| `jour` | date | 2026-01-07 |
| `methode`, `url`, `protocole` | texte | GET, /catalogue/produit/P00042, HTTP/1.1 |
| `statut` | entier | 200, 404, 500 |
| `taille_octets` | entier long | 5123 |
| `referer` | texte, vide si `-` | https://www.google.com/ |
| `user_agent` | texte | Mozilla/5.0 ... |
| `temps_reponse_ms` | entier | 84 |

### Règles

| Ordre | Motif | Condition de rejet |
|---|---|---|
| 1 | `ligne_vide` | Ligne vide ou faite d'espaces |
| 2 | `format_invalide` | Ne respecte pas le format Apache combined (lignes coupées en deux) |
| 3 | `horodatage_illisible` | Date dans aucun format connu |

**Correction plutôt que rejet :** 58 lignes portent une date au format `2026-01-03 11:53:24` au lieu du format Apache. Elles sont lues avec un second format, à l'heure de Paris, au lieu d'être rejetées.

**Doublons :** une ligne de log identique au caractère près (même IP, même seconde, même URL, même temps de réponse) est considérée comme un doublon. Il n'y en a aucun dans J1.

**Adresse IP :** c'est une donnée personnelle au sens du RGPD ; elle est pseudonymisée. Les lignes rejetées gardent l'adresse en clair dans `contenu`, mais le dossier `rejects` n'est pas lisible par les analystes (droits `750`).

### Résultat sur J1

```
[silver] logs_web : total=50000 | propres=49809 + rejets=191 + doublons=0 = 50000 | OK
[silver]   rejet format_invalide               171
[silver]   rejet ligne_vide                     20
```

Relancé une seconde fois : mêmes nombres, 49 809 lignes au total dans le Silver, aucun doublon. Les jours couverts vont du 2026-01-01 au 2026-01-31.
