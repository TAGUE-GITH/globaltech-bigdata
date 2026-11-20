#!/usr/bin/env bash
# Copie une livraison du PC (dossier data/ du projet) vers la zone landing de HDFS.
#
# Usage, depuis le poste Windows :
#   docker exec namenode bash /opt/project/tools/deposer_livraison.sh J1
#   docker exec namenode bash /opt/project/tools/deposer_livraison.sh J2 /opt/project/data
#
# Chaque source va dans /datalake/landing/<source>/livraison=<J>.
# Relancer le dépôt d'une livraison écrase ses fichiers (-f) : pas de doublon.
# Les capteurs ne sont pas déposés ici : ils arrivent en flux (Lot 5, stream_replay.py).
set -euo pipefail

LIVRAISON=${1:?Indiquer la livraison, par exemple J1}
DOSSIER=${2:-/opt/project/data/echantillon}
SOURCES="commandes produits clients logs_web"

for source in $SOURCES; do
  local_dir="$DOSSIER/$LIVRAISON/$source"
  cible="/datalake/landing/$source/livraison=$LIVRAISON"
  if [ ! -d "$local_dir" ]; then
    echo "ERREUR : dossier introuvable $local_dir" >&2
    exit 1
  fi
  hdfs dfs -mkdir -p "$cible"
  hdfs dfs -put -f "$local_dir"/* "$cible/"
  echo "$source : $(ls "$local_dir" | wc -l) fichier(s) -> $cible"
done

echo
hdfs dfs -du -s -h /datalake/landing/*/livraison="$LIVRAISON"
