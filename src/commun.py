"""Fonctions partagées par tous les traitements : configuration et session Spark."""
import json
import os

from pyspark.sql import SparkSession

CHEMIN_CONF = os.environ.get("APP_CONF", "/opt/project/conf/app.json")


def charger_conf(chemin=CHEMIN_CONF):
    """Lit la configuration externalisée (conf/app.json)."""
    with open(chemin, encoding="utf-8") as f:
        return json.load(f)


def creer_spark(conf, nom_traitement):
    """Crée la session Spark. Le master (YARN) vient de spark-defaults.conf."""
    return (SparkSession.builder
            .appName(f"{conf['app_name']}-{nom_traitement}")
            .config("spark.sql.shuffle.partitions", conf["shuffle_partitions"])
            # En écriture « overwrite », ne remplacer que les partitions
            # présentes dans les données écrites : c'est ce qui rend le rejeu idempotent
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.session.timeZone", "Europe/Paris")
            .getOrCreate())
