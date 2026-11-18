"""Vérifie que le cluster fonctionne de bout en bout : Spark sur YARN, Python, HDFS.

Lancement, depuis le poste client :
    docker exec spark-client spark-submit tools/verif_cluster.py
"""
import sys

from pyspark.sql import SparkSession

CHEMIN_TEST = "hdfs://namenode:8020/tmp/verif_cluster"

spark = SparkSession.builder.appName("verif-cluster").getOrCreate()
sc = spark.sparkContext

print("Gestionnaire de ressources :", sc.master)
print("Python du pilote           :", sys.version.split()[0])

# Chaque tâche renvoie la version de Python de l'exécuteur qui la traite
versions = (sc.parallelize(range(4), 4)
              .map(lambda _: __import__("sys").version.split()[0])
              .distinct().collect())
print("Python des exécuteurs      :", versions)

print("Somme de 0 à 999 999       :", spark.range(1_000_000).selectExpr("sum(id)").first()[0])

spark.range(1000).write.mode("overwrite").parquet(CHEMIN_TEST)
print("Lignes relues depuis HDFS  :", spark.read.parquet(CHEMIN_TEST).count())

spark.stop()
print("Cluster OK")
