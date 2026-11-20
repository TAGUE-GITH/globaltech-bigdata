#!/usr/bin/env bash
# Crée l'arborescence du data lake dans HDFS, avec ses droits et ses quotas.
#
# Le script peut être relancé autant de fois que l'on veut : il ne recrée
# que ce qui manque et réapplique les mêmes droits (il est idempotent).
#
# Lancement, depuis le poste Windows :
#   docker exec namenode bash /opt/project/tools/init_hdfs.sh
#
# Justification des choix : docs/arborescence-hdfs.md
set -euo pipefail

RACINE=/datalake

echo "== Création des dossiers =="
hdfs dfs -mkdir -p \
  "$RACINE/landing/commandes" "$RACINE/landing/produits" "$RACINE/landing/clients" \
  "$RACINE/landing/logs_web"  "$RACINE/landing/capteurs" \
  "$RACINE/bronze" "$RACINE/silver" "$RACINE/gold" "$RACINE/rejects" \
  /checkpoints /audit

echo "== Droits d'accès =="
# Propriétaire : hadoop, le compte qui exécute les traitements.
# Groupe : supergroup. « Autres » : tout autre utilisateur, par exemple un analyste.
#
# landing : dépôt des fichiers bruts et du flux capteurs
hdfs dfs -chmod -R 770 "$RACINE/landing"
# bronze, silver, rejects : écrits par les traitements uniquement, lisibles par l'équipe
hdfs dfs -chmod 750 "$RACINE/bronze" "$RACINE/silver" "$RACINE/rejects"
# gold : données publiées, lisibles par tous, modifiables par les traitements seulement
hdfs dfs -chmod 755 "$RACINE/gold"
# checkpoints du streaming et table d'audit : réservés aux traitements
hdfs dfs -chmod 700 /checkpoints /audit
hdfs dfs -chmod 755 "$RACINE"

echo "== Quotas =="
# Quota d'espace : il compte les octets APRÈS réplication (×3).
# Calcul détaillé dans docs/arborescence-hdfs.md.
hdfs dfsadmin -setSpaceQuota 10g "$RACINE/landing"
hdfs dfsadmin -setSpaceQuota 10g "$RACINE/bronze"
hdfs dfsadmin -setSpaceQuota 10g "$RACINE/silver"
hdfs dfsadmin -setSpaceQuota 5g  "$RACINE/gold"
hdfs dfsadmin -setSpaceQuota 2g  "$RACINE/rejects"
# Quota de nombre de fichiers sur le flux capteurs : un fichier toutes les 5 s
# donne 17 280 fichiers par jour ; 50 000 couvre près de 3 jours de rejeu.
hdfs dfsadmin -setQuota 50000 "$RACINE/landing/capteurs"

echo "== Résultat =="
hdfs dfs -ls "$RACINE" /checkpoints /audit
echo
hdfs dfs -count -q -h -v "$RACINE/landing" "$RACINE/landing/capteurs" \
  "$RACINE/bronze" "$RACINE/silver" "$RACINE/gold" "$RACINE/rejects"
