# Image Hadoop du projet : l'image officielle, plus les dossiers de stockage de HDFS.
#
# Pourquoi ce petit ajout ? Les données de HDFS sont gardées dans des volumes Docker
# pour survivre à un « docker compose down ». Un volume vide prend le propriétaire
# du dossier de l'image sur lequel il est monté. Si ce dossier n'existe pas dans
# l'image, Docker le crée au nom de root, et HDFS (qui tourne sous l'utilisateur
# hadoop) ne peut pas y écrire. On crée donc ces dossiers ici, au nom de hadoop.
FROM apache/hadoop:3.5.0

USER root
RUN mkdir -p /data/namenode /data/datanode \
 && chown -R hadoop:users /data
USER hadoop
