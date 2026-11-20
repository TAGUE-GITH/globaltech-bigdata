# Guide de relance du cluster

Ce guide permet à une personne extérieure au projet de démarrer le cluster Hadoop et Spark sur son poste, puis de vérifier qu'il fonctionne, **en moins de 10 minutes**.

## Ce que l'on obtient

| Composant | Conteneurs | Interface web |
|---|---|---|
| HDFS (stockage) | `namenode`, `datanode1`, `datanode2`, `datanode3` | http://localhost:9870 |
| YARN (ressources) | `resourcemanager`, `nodemanager1`, `nodemanager2` | http://localhost:8088 |
| Spark 3.5.9 | `spark-history`, `spark-client` | http://localhost:18080 |

## 1. Prérequis (à vérifier avant de chronométrer)

| Prérequis | Vérification | Résultat attendu |
|---|---|---|
| Docker Desktop installé et démarré | `docker --version` | `Docker version 2x.x` |
| Moteur Docker actif | Docker Desktop, en bas à gauche | `Engine running` |
| Mémoire donnée à Docker | `docker info --format "{{.MemTotal}}"` | au moins 10 Go (environ 10 000 000 000) |
| Git installé | `git --version` | `git version 2.x` |
| Ports libres | aucun autre programme sur 9870, 8088, 18080, 4040 | |
| Terminal | PowerShell **non administrateur** | la ligne commence par `PS C:\Users\...` |

Le cluster réserve 8 Go aux calculs (2 NodeManagers de 4 Go). Avec moins de 10 Go donnés à Docker, des conteneurs risquent d'être arrêtés faute de mémoire. Sous Windows, Docker reçoit par défaut la moitié de la mémoire du PC : il faut donc un PC d'au moins 20 Go, ou augmenter la limite dans le fichier `.wslconfig`.

## 2. Récupérer le projet (environ 1 minute)

```powershell
git clone https://github.com/TAGUE-GITH/globaltech-bigdata.git
cd globaltech-bigdata
```

Copier ensuite le dossier `skeleton/data/echantillon` du kit étudiant v3 dans le dossier `data/` du projet. Les données ne sont pas dans Git, le fichier `.gitignore` les exclut.

Si le kit a été téléchargé et décompressé dans le dossier Téléchargements, ces deux commandes retrouvent le dossier `echantillon` et le copient au bon endroit :

```powershell
$src = Get-ChildItem "$HOME\Downloads" -Recurse -Directory -Filter echantillon | Select-Object -First 1
Copy-Item -Recurse $src.FullName data\echantillon
```

Si la seconde commande signale une valeur `null`, le kit n'est pas décompressé : clic droit sur le fichier zip, puis *Extraire tout*.

On doit obtenir :

```
data/echantillon/J1/capteurs
data/echantillon/J1/clients
data/echantillon/J1/commandes
data/echantillon/J1/logs_web
data/echantillon/J1/produits
```

Le cluster démarre aussi sans les données ; elles ne servent qu'aux traitements.

## 3. Démarrer le cluster (3 à 6 minutes la première fois, 1 minute ensuite)

```powershell
docker compose up -d --build
```

- `--build` construit l'image Spark du projet (fichier `docker/spark/Dockerfile`).
- La première fois, Docker télécharge l'image `apache/hadoop:3.5.0` (environ 600 Mo) et Spark 3.5.9 (environ 400 Mo). La durée dépend surtout de la connexion.
- Les fois suivantes, tout est déjà en cache.

Attendre environ 45 secondes que les services démarrent, puis :

```powershell
docker compose ps
```

Les **9 conteneurs** doivent être à l'état `Up`.

## 4. Vérifier le cluster (environ 1 minute)

**HDFS : 3 DataNodes vivants**

```powershell
docker exec namenode hdfs dfsadmin -report
```

Attendu : `Live datanodes (3):`

**YARN : 2 NodeManagers actifs**

```powershell
docker exec resourcemanager yarn node -list
```

Attendu : `Total Nodes:2`, avec `nodemanager1` et `nodemanager2` à l'état `RUNNING`.

**Spark sur YARN, de bout en bout**

```powershell
docker exec spark-client spark-submit tools/verif_cluster.py
```

Pendant environ 25 secondes, de nombreuses lignes `INFO` défilent : c'est normal, Spark décrit chaque étape. Attendre le retour de l'invite `PS C:\...>`, puis remonter légèrement dans la fenêtre pour trouver le bloc suivant, situé au milieu des lignes `INFO` et non à la toute fin :

```
Gestionnaire de ressources : yarn
Python du pilote           : 3.12.3
Python des exécuteurs      : ['3.12.3']
Somme de 0 à 999 999       : 499999500000
Lignes relues depuis HDFS  : 1000
Cluster OK
```

**Interfaces web**

- http://localhost:9870, onglet *Datanodes* : 3 DataNodes *In operation*
- http://localhost:8088 : l'application `verif-cluster` en *FINISHED / SUCCEEDED*
- http://localhost:18080 : l'application `verif-cluster` dans la liste

Si les trois vérifications passent, le cluster est opérationnel.

Si `Cluster OK` n'apparaît pas, la vérification a échoué : remonter jusqu'à la première ligne contenant `Error` ou `Exception` et consulter la section 6.

## 5. Arrêter le cluster

| Commande | Effet | Données HDFS |
|---|---|---|
| `docker compose stop` | Met les conteneurs en pause | **conservées** |
| `docker compose start` | Redémarre les conteneurs arrêtés | conservées |
| `docker compose down` | Supprime les conteneurs | **perdues** : HDFS est reformaté au prochain démarrage |

Pour une pause entre deux séances de travail, utiliser `stop` puis `start`.

## 6. Problèmes fréquents

| Symptôme | Cause | Solution |
|---|---|---|
| `docker : Le terme «docker» n'est pas reconnu` | Terminal ouvert avant l'installation de Docker | Fermer et rouvrir le terminal |
| `error during connect` ou `Cannot connect to the Docker daemon` | Docker Desktop n'est pas démarré | Lancer Docker Desktop, attendre `Engine running` |
| `fatal: detected dubious ownership in repository` | Projet cloné dans un terminal administrateur | `git config --global --add safe.directory <chemin du projet>` |
| `Bind for 0.0.0.0:9870 failed: port is already allocated` | Un autre programme utilise ce port | Arrêter ce programme, ou un autre cluster avec `docker compose down` dans son dossier |
| `Live datanodes (0)` juste après le démarrage | Les DataNodes s'inscrivent encore | Attendre 30 secondes et relancer la commande |
| Un traitement Spark reste à l'état `ACCEPTED` | Mémoire YARN insuffisante, ou un autre traitement l'occupe | Vérifier sur http://localhost:8088 qu'aucune autre application ne tourne |
| Un conteneur est à l'état `Exited` | Erreur au démarrage | `docker compose logs <nom du conteneur>` pour lire l'erreur |

## 7. Validation par un tiers

Critère du projet : une personne extérieure relance le cluster **en moins de 10 minutes**, du clonage au message `Cluster OK`, prérequis déjà installés.

| 20/11/2026 | Ornelle Kouaye | PC Windows, 16 Go | Non | 4 min 30 | Cluster OK | Copie des données non décrite (corrigé) |