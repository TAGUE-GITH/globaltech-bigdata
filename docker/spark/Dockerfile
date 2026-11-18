# Image Spark du projet : Spark 3.5 installé sur l'image officielle Hadoop 3.5.0.
#
# Pourquoi partir de l'image Hadoop plutôt que de l'image officielle apache/spark ?
# Avec PySpark sur YARN, le pilote (ici) et les exécuteurs (dans les NodeManagers)
# doivent utiliser exactement la même version de Python. L'image Hadoop contient
# Python 3.12 ; partir de la même image garantit des versions identiques partout,
# ainsi que les mêmes commandes hdfs et yarn et la même configuration Hadoop.
FROM apache/hadoop:3.5.0

ARG SPARK_VERSION=3.5.9

USER root
RUN set -e; \
    f="spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    curl -fsSL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${f}" -o /tmp/spark.tgz \
      || curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${f}" -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark; \
    rm /tmp/spark.tgz; \
    chown -R hadoop:users /opt/spark

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

USER hadoop
WORKDIR /opt/project
