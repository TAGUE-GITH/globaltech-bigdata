"""Couche Bronze : copie brute des livraisons en Parquet, sans perte.

Lit les fichiers déposés dans /datalake/landing/<source>/livraison=<J>
et les écrit dans /datalake/bronze/<source>, partitionnés par date d'ingestion.

Principes :
- Aucune donnée n'est modifiée ni filtrée : toutes les colonnes restent du texte.
  Le typage et le nettoyage sont le rôle de la couche Silver.
- Une ligne CSV mal formée n'est pas perdue : elle est gardée en entier dans
  la colonne _ligne_invalide.
- Rejeu sans doublon : la date d'ingestion est la date logique de la livraison
  (conf/app.json). Réingérer J1 remplace la partition de J1 au lieu de l'ajouter.

Lancement :
    docker exec spark-client spark-submit src/bronze.py J1
"""
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from commun import charger_conf, creer_spark

COLONNE_INVALIDE = "_ligne_invalide"


def lire_csv_brut(spark, chemin):
    """Lit des CSV en gardant toutes les valeurs en texte et les lignes invalides."""
    # 1. Les noms de colonnes viennent de l'en-tête de la livraison : si une
    #    livraison ajoute une colonne, elle est conservée automatiquement.
    colonnes = spark.read.option("header", True).csv(chemin).columns
    schema = StructType(
        [StructField(c, StringType()) for c in colonnes]
        + [StructField(COLONNE_INVALIDE, StringType())]
    )
    # 2. Lecture avec ce schéma : une ligne qui n'a pas le bon nombre de
    #    champs est recopiée telle quelle dans _ligne_invalide.
    return (spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", COLONNE_INVALIDE)
            .schema(schema)
            .csv(chemin))


def lire_texte_brut(spark, chemin):
    """Lit un fichier texte ligne à ligne (logs web), sans interprétation."""
    return spark.read.text(chemin).withColumnRenamed("value", "ligne")


def ajouter_metadonnees(df, livraison, date_ingestion):
    """Colonnes techniques qui tracent l'origine de chaque ligne."""
    return (df
            .withColumn("_livraison", F.lit(livraison))
            .withColumn("_fichier_source", F.input_file_name())
            .withColumn("_ingere_le", F.current_timestamp())
            .withColumn("date_ingestion", F.lit(date_ingestion)))


def compter_lignes_source(spark, chemin, avec_entete):
    """Nombre de lignes de données dans les fichiers déposés (en-têtes exclus)."""
    lignes = spark.read.text(chemin)
    total = lignes.count()
    if avec_entete:
        total -= len(lignes.inputFiles())  # une ligne d'en-tête par fichier
    return total


def ingerer_source(spark, conf, source, livraison, est_csv):
    date_ingestion = conf["dates_livraison"][livraison]
    entree = f"{conf['datalake']}/landing/{source}/livraison={livraison}"
    sortie = f"{conf['datalake']}/bronze/{source}"

    df = lire_csv_brut(spark, entree) if est_csv else lire_texte_brut(spark, entree)
    df = ajouter_metadonnees(df, livraison, date_ingestion)

    (df.write
       .mode("overwrite")              # avec partitionOverwriteMode=dynamic :
       .partitionBy("date_ingestion")  # seule la partition de cette livraison est remplacée
       .parquet(sortie))

    # Contrôle « sans perte » : autant de lignes en Bronze que dans les fichiers déposés
    attendu = compter_lignes_source(spark, entree, avec_entete=est_csv)
    bronze = (spark.read.parquet(sortie)
                   .where(F.col("date_ingestion") == date_ingestion))
    obtenu = bronze.count()
    invalides = bronze.where(F.col(COLONNE_INVALIDE).isNotNull()).count() if est_csv else 0
    statut = "OK" if attendu == obtenu else "ECART"
    print(f"[bronze] {source:<10} {livraison} -> date_ingestion={date_ingestion} | "
          f"source={attendu} bronze={obtenu} dont invalides={invalides} | {statut}")
    return statut == "OK"


def main():
    if len(sys.argv) != 2:
        sys.exit("Usage : spark-submit src/bronze.py <livraison>   (ex. J1)")
    livraison = sys.argv[1]
    conf = charger_conf()
    if livraison not in conf["dates_livraison"]:
        sys.exit(f"Livraison inconnue : {livraison}")

    spark = creer_spark(conf, f"bronze-{livraison}")
    resultats = [ingerer_source(spark, conf, s, livraison, est_csv=True)
                 for s in conf["sources_csv"]]
    resultats += [ingerer_source(spark, conf, s, livraison, est_csv=False)
                  for s in conf["sources_texte"]]
    spark.stop()

    if not all(resultats):
        sys.exit("Ingestion Bronze terminée avec des écarts de comptage.")
    print(f"Ingestion Bronze de {livraison} terminée sans perte.")


if __name__ == "__main__":
    main()
