"""Couche Silver des logs web : découpage du format Apache, contrôles, rejets.

Entrée  : /datalake/bronze/logs_web   (une colonne `ligne`, texte brut)
Sorties : /datalake/silver/logs_web   (une colonne par champ, typée)
          /datalake/rejects/logs_web  (lignes écartées, avec leur motif)

Lancement :
    docker exec spark-client spark-submit src/silver_logs.py J1
"""
import sys

from pyspark.sql import functions as F

from commun import charger_conf, creer_spark
from qualite import (bilan, dedupliquer, ecrire_partition, ecrire_rejets,
                     lire_bronze, pseudonymiser, separer, MOTIF)

SOURCE = "logs_web"

# Format Apache combined, suivi du temps de réponse en millisecondes :
# 203.0.113.12 - - [07/Jan/2026:14:21:03 +0100] "GET /catalogue HTTP/1.1" 200 5123 "-" "Mozilla/5.0 ..." 84
REGEX = (r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) '
         r'"([^"]*)" "([^"]*)" (\d+)$')
CHAMPS = {1: "ip", 2: "horodatage_brut", 3: "methode", 4: "url", 5: "protocole",
          6: "statut", 7: "taille_octets", 8: "referer", 9: "user_agent", 10: "temps_reponse_ms"}


def decouper(df):
    """Extrait chaque champ du format Apache et le convertit dans son type."""
    for numero, nom in CHAMPS.items():
        df = df.withColumn(nom, F.regexp_extract("ligne", REGEX, numero))
    # Deux formats de date coexistent (voir docs/anomalies-j1.md) :
    #  - le format Apache, avec fuseau : 07/Jan/2026:14:21:03 +0100
    #  - un format ISO sans fuseau, interprété à l'heure de Paris : 2026-01-03 11:53:24
    horodatage = F.coalesce(
        F.to_timestamp("horodatage_brut", "dd/MMM/yyyy:HH:mm:ss Z"),
        F.to_timestamp("horodatage_brut", "yyyy-MM-dd HH:mm:ss"))
    return (df
            .withColumn("horodatage", horodatage)
            .withColumn("jour", F.to_date("horodatage"))
            .withColumn("statut", F.col("statut").cast("int"))
            .withColumn("taille_octets", F.col("taille_octets").cast("long"))
            .withColumn("temps_reponse_ms", F.col("temps_reponse_ms").cast("int"))
            .withColumn("referer", F.when(F.col("referer") == "-", None).otherwise(F.col("referer"))))


def regles():
    """Règles qualité, dans l'ordre où elles sont testées."""
    return [
        ("ligne_vide", F.trim("ligne") == ""),
        ("format_invalide", ~F.col("ligne").rlike(REGEX)),
        ("horodatage_illisible", F.col("horodatage").isNull()),
    ]


def main():
    livraison = sys.argv[1] if len(sys.argv) > 1 else "J1"
    conf = charger_conf()
    spark = creer_spark(conf, f"silver-logs-{livraison}")

    bronze = lire_bronze(spark, conf, SOURCE, livraison)
    total = bronze.count()

    propres, rejets = separer(decouper(bronze), regles())
    propres, nb_doublons = dedupliquer(propres, ["ligne"])

    # L'adresse IP est une donnée personnelle (RGPD) : elle est pseudonymisée
    propres = (propres
               .withColumn("ip_pseudo", pseudonymiser("ip", conf["sel_pseudonymisation"]))
               .select("ip_pseudo", "horodatage", "jour", "methode", "url", "protocole",
                       "statut", "taille_octets", "referer", "user_agent", "temps_reponse_ms",
                       "_livraison", "_fichier_source", "date_ingestion"))

    ecrire_partition(propres, f"{conf['datalake']}/silver/{SOURCE}")
    ecrire_rejets(rejets, conf, SOURCE, ["ligne"])

    # Bilan, relu depuis HDFS : on vérifie ce qui a réellement été écrit
    date = conf["dates_livraison"][livraison]
    ecrit = lambda couche: (spark.read.parquet(f"{conf['datalake']}/{couche}/{SOURCE}")
                                 .where(F.col("date_ingestion") == date))
    nb_propres = ecrit("silver").count()
    rejets_ecrits = ecrit("rejects")
    ok = bilan(SOURCE, total, nb_propres, rejets_ecrits.count(), nb_doublons,
               rejets_ecrits.groupBy(MOTIF).count().orderBy(F.desc("count")).collect())
    spark.stop()
    if not ok:
        sys.exit("Silver logs_web : l'égalité propres + rejets + doublons = total n'est pas respectée.")


if __name__ == "__main__":
    main()
