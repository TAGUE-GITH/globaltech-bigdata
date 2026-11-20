# Arborescence HDFS, droits et quotas

Création ou remise en conformité, à tout moment (le script est idempotent) :

```powershell
docker exec namenode bash /opt/project/tools/init_hdfs.sh
```

## 1. Arborescence

```
/datalake
├── landing/            fichiers bruts déposés tels quels, avant ingestion
│   ├── commandes/
│   ├── produits/
│   ├── clients/
│   ├── logs_web/
│   └── capteurs/       dossier surveillé par Spark Streaming (stream_replay.py y dépose un fichier toutes les 5 s)
├── bronze/             copie brute en Parquet, partitionnée par date d'ingestion
├── silver/             données nettoyées, dédupliquées, historisées
├── gold/               modèle en étoile et indicateurs
└── rejects/            lignes écartées, avec le motif du rejet
/checkpoints            états de reprise de Spark Streaming
/audit                  table d'audit des exécutions
/spark-logs             journaux lus par le Spark History Server
```

**Justification**

| Choix | Raison |
|---|---|
| Une racine `/datalake` | Sépare les données du projet des dossiers techniques de Hadoop et de Spark (`/tmp`, `/user`, `/spark-logs`) ; un seul chemin à sauvegarder ou à protéger |
| Un dossier par couche | Reprend l'architecture imposée par l'énoncé (bronze, silver, gold, rejects) : chaque couche a son propre rôle, ses propres droits et son propre quota |
| Une zone `landing` | Les exécuteurs Spark tournent dans les NodeManagers, qui ne voient pas les fichiers du PC : les livraisons doivent d'abord être copiées dans HDFS pour être lues sur YARN. `landing` est aussi la source du flux capteurs |
| Un sous-dossier par source | Chaque source a son format (CSV, texte, JSON) et son rythme de livraison ; on peut les charger, les rejouer ou les purger séparément |
| `/checkpoints` et `/audit` hors de `/datalake` | Ce ne sont pas des données métier mais l'état technique des traitements : ils ne doivent jamais être lus par un analyste ni effacés par une purge des données |

## 2. Droits d'accès

Le cluster n'utilise pas Kerberos : HDFS fait confiance au nom d'utilisateur déclaré. Les droits ci-dessous protègent contre les erreurs de manipulation, pas contre un utilisateur malveillant. En production, on activerait Kerberos.

| Dossier | Droits | Propriétaire (hadoop) | Groupe (supergroup) | Autres (ex. analyste) |
|---|---|---|---|---|
| `/datalake/landing` | `770` | lecture, écriture | lecture, écriture | aucun accès |
| `/datalake/bronze` | `750` | lecture, écriture | lecture | aucun accès |
| `/datalake/silver` | `750` | lecture, écriture | lecture | aucun accès |
| `/datalake/rejects` | `750` | lecture, écriture | lecture | aucun accès |
| `/datalake/gold` | `755` | lecture, écriture | lecture | **lecture** |
| `/checkpoints`, `/audit` | `700` | lecture, écriture | aucun accès | aucun accès |

**Principe :** seuls les traitements écrivent ; plus une donnée est fiable, plus elle est ouverte. Le bronze et le silver contiennent encore des données personnelles (clients) : seule la couche gold, pseudonymisée et agrégée, est lisible par les analystes.

**Démonstration** (utilisateur fictif `analyste`) :

```powershell
docker exec -e HADOOP_USER_NAME=analyste namenode hdfs dfs -ls /datalake/gold        # autorisé
docker exec -e HADOOP_USER_NAME=analyste namenode hdfs dfs -ls /datalake/bronze      # refusé
docker exec -e HADOOP_USER_NAME=analyste namenode hdfs dfs -touchz /datalake/bronze/x # refusé
```

Résultat obtenu pour les deux refus : `Permission denied: user=analyste, ... inode="/datalake/bronze":hadoop:supergroup:drwxr-x---`.

## 3. Quotas

Un quota d'espace HDFS compte les octets **après réplication** : avec une réplication de 3, un fichier de 1 Go consomme 3 Go de quota.

### Estimation des volumes

Mesurés sur l'échantillon J1 du kit (100 000 commandes), puis extrapolés aux 4 millions de commandes d'une livraison réelle (facteur 40) :

| Source | Échantillon | Livraison réelle estimée |
|---|---|---|
| Commandes (CSV) | 8,3 Mo | 330 Mo |
| Logs web (texte) | 9,4 Mo | 380 Mo |
| Capteurs (JSON) | 2,3 Mo | 90 Mo |
| Clients, produits (CSV) | 0,8 Mo | moins de 35 Mo |
| **Total par livraison** | **21 Mo** | **environ 0,8 Go** |

Hypothèse à vérifier à la réception de J1 : les logs et les capteurs croissent comme les commandes.

### Quotas retenus

| Dossier | Quota | Calcul |
|---|---|---|
| `landing` | 10 Go | 3 livraisons × 0,8 Go × 3 copies = 7,2 Go, plus une marge pour un rejeu |
| `bronze` | 10 Go | Parquet compressé, environ 30 % du brut : 3 × 0,25 Go × 3 copies = 2,3 Go ; la marge couvre un rejeu complet et l'écart à l'hypothèse |
| `silver` | 10 Go | Même ordre de grandeur que le bronze, plus l'historique des produits (SCD type 2) |
| `gold` | 5 Go | Agrégats et dimensions, bien plus petits que les faits détaillés |
| `rejects` | 2 Go | Les rejets ne représentent qu'une petite part des lignes ; un quota bas sert d'alarme si un contrôle qualité rejette tout par erreur |
| `landing/capteurs` | 50 000 fichiers | Un fichier toutes les 5 s = 17 280 fichiers par jour ; limiter le nombre de fichiers protège la mémoire du NameNode, qui garde chaque fichier en mémoire |

### Point d'attention : la réservation d'un bloc entier

Pour écrire un fichier, HDFS réserve d'avance un bloc complet multiplié par la réplication : 128 Mo × 3 = **384 Mo**, même pour un fichier de quelques octets. Un dossier avec un quota inférieur à 384 Mo refuse donc toute écriture. Test réalisé avec un quota de 100 Mo et un fichier de 6 octets :

```
put: The DiskSpace quota of /tmp/demo-quota is exceeded: quota = 104857600 B = 100 MB
but diskspace consumed = 402653184 B = 384 MB
```

C'est pourquoi aucun quota n'est fixé en dessous de 2 Go.

### Consulter l'occupation

```powershell
docker exec namenode hdfs dfs -count -q -h -v /datalake/landing /datalake/bronze /datalake/silver /datalake/gold /datalake/rejects
```
