# Couche Bronze

## Rôle

Conserver chaque livraison **telle qu'elle a été reçue**, sans perte, dans un format efficace (Parquet). C'est le point de reprise du projet : si un traitement Silver ou Gold est faux, on le corrige et on le relance à partir du Bronze, sans redemander les fichiers.

## Chaîne d'ingestion

```
PC : data/<dossier>/J1/...
        │  tools/deposer_livraison.sh J1        (hdfs dfs -put)
        ▼
HDFS : /datalake/landing/<source>/livraison=J1/  (fichiers bruts, inchangés)
        │  src/bronze.py J1                     (Spark sur YARN)
        ▼
HDFS : /datalake/bronze/<source>/date_ingestion=2026-02-01/*.snappy.parquet
```

Commandes :

```powershell
docker exec namenode bash /opt/project/tools/deposer_livraison.sh J1
docker exec spark-client spark-submit src/bronze.py J1
```

Sources traitées : `commandes`, `produits`, `clients` (CSV) et `logs_web` (texte). Les capteurs arrivent en flux continu et sont traités par le streaming (Lot 5).

## Choix et justifications

| Choix | Justification |
|---|---|
| Toutes les colonnes en texte | Le Bronze ne transforme rien. Typer une colonne (`quantity` en entier, par exemple) ferait disparaître en `null` une valeur mal formée : ce serait une perte. Le typage a lieu en Silver, où les rejets sont tracés avec leur motif |
| Colonne `_ligne_invalide` | Une ligne CSV qui n'a pas le bon nombre de champs est recopiée en entier dans cette colonne au lieu d'être tronquée ou ignorée |
| Noms de colonnes lus dans l'en-tête de chaque livraison | Si une livraison ajoute une colonne (l'énoncé prévient que le schéma peut évoluer), elle est conservée sans modifier le code |
| Logs web lus ligne à ligne | Le format Apache n'est pas un CSV ; chaque ligne est conservée intacte, y compris les lignes vides ou mal formées |
| Colonnes `_livraison`, `_fichier_source`, `_ingere_le` | Traçabilité : pour chaque ligne, on sait de quelle livraison et de quel fichier elle vient, et quand elle a été ingérée |
| Parquet compressé Snappy | Format en colonnes : Spark ne lit que les colonnes utiles. Mesuré sur J1 : les commandes passent de 8,3 Mo (CSV) à 2,1 Mo, les logs de 9,4 Mo à 2,0 Mo, soit environ 4 fois moins |
| Partition `date_ingestion` = date logique de la livraison | J1 = 2026-02-01, J2 = 2026-03-01, J3 = 2026-04-01 (dans `conf/app.json`). Une livraison correspond toujours à la même partition, quel que soit le jour où on la rejoue |
| Écriture `overwrite` avec `partitionOverwriteMode=dynamic` | Spark ne remplace que la partition de la livraison écrite. Réingérer J1 remplace J1 au lieu de l'ajouter une seconde fois, sans toucher à J2 ni J3 |

Les dates de J2 et J3 sont des hypothèses (une livraison par mois, extraite le 1er du mois suivant, comme J1). Elles seront ajustées à la réception des livraisons.

## Preuves

### Aucune perte

À chaque exécution, le programme compare le nombre de lignes des fichiers déposés (en-têtes exclus) au nombre de lignes écrites en Bronze :

```
[bronze] commandes  J1 -> date_ingestion=2026-02-01 | source=101200 bronze=101200 dont invalides=56 | OK
[bronze] produits   J1 -> date_ingestion=2026-02-01 | source=592 bronze=592 dont invalides=0 | OK
[bronze] clients    J1 -> date_ingestion=2026-02-01 | source=5127 bronze=5127 dont invalides=0 | OK
[bronze] logs_web   J1 -> date_ingestion=2026-02-01 | source=50000 bronze=50000 dont invalides=0 | OK
Ingestion Bronze de J1 terminée sans perte.
```

Le programme s'arrête en erreur si un écart apparaît.

### Rejeu sans doublon

J1 déposé et ingéré une seconde fois, puis lecture de **toutes** les partitions du Bronze :

```
[rejeu] commandes  lignes=101200 partitions=[2026-02-01]
[rejeu] produits   lignes=592    partitions=[2026-02-01]
[rejeu] clients    lignes=5127   partitions=[2026-02-01]
[rejeu] logs_web   lignes=50000  partitions=[2026-02-01]
```

Mêmes totaux qu'après la première ingestion, une seule partition : aucun doublon.

## Anomalies repérées dans l'échantillon J1 (à traiter en Silver)

Constatées en préparant le Bronze, conservées telles quelles :

| Source | Anomalie | Nombre |
|---|---|---|
| commandes | Lignes avec `;` comme séparateur au lieu de `,` | 17 |
| commandes | Lignes tronquées (2 à 9 champs au lieu de 10) | 26 |
| commandes | Lignes avec 2 champs en trop (`EXTRA,42`) | 13 |
| produits | Prix au format français, entre guillemets (`"205,16"`) | 2 |
| logs_web | Lignes vides | 20 |
| capteurs | Lignes JSON tronquées | 27 (sur 120 fichiers) |

Les 56 lignes de commandes à structure invalide sont dans `_ligne_invalide`. Les autres anomalies (doublons, valeurs manquantes, codes invalides, produit très fréquent) se cherchent dans le contenu des colonnes et relèvent du Lot 3.
