"""Outils communs de la couche Silver : contrôles qualité, rejets, doublons, bilan.

Toutes les sources (logs, commandes, clients, produits) suivent le même schéma :

    1. lire la partition Bronze de la livraison          -> lire_bronze
    2. préparer les colonnes (découpage, typage, ...)      (propre à chaque source)
    3. appliquer toutes les règles en un seul passage    -> separer
    4. retirer les doublons parmi les lignes valides     -> dedupliquer
    5. écrire les lignes propres et les rejets           -> ecrire_partition, ecrire_rejets
    6. vérifier propres + rejets + doublons = total      -> bilan
"""
from pyspark.sql import functions as F

MOTIF = "motif_rejet"


def lire_bronze(spark, conf, source, livraison):
    """Lit la partition Bronze d'une livraison."""
    date = conf["dates_livraison"][livraison]
    return (spark.read.parquet(f"{conf['datalake']}/bronze/{source}")
                 .where(F.col("date_ingestion") == date))


def separer(df, regles):
    """Applique toutes les règles qualité en un seul passage sur les données.

    regles : liste de (motif, condition). La condition est une expression Spark
    vraie quand la ligne est INVALIDE. Les règles sont testées dans l'ordre :
    une ligne reçoit le motif de la première règle qu'elle enfreint.

    Toutes les conditions sont évaluées dans une seule expression (coalesce) :
    Spark ne lit donc les données qu'une fois, quel que soit le nombre de règles,
    au lieu d'une lecture par règle.

    Retourne (propres, rejets) ; les rejets ont une colonne motif_rejet.
    """
    motif = F.coalesce(*[F.when(condition, F.lit(nom)) for nom, condition in regles])
    df = df.withColumn(MOTIF, motif).cache()
    propres = df.where(F.col(MOTIF).isNull()).drop(MOTIF)
    rejets = df.where(F.col(MOTIF).isNotNull())
    return propres, rejets


def dedupliquer(df, colonnes):
    """Garde une seule ligne par valeur de `colonnes`. Retourne (df, nb_doublons)."""
    avant = df.count()
    unique = df.dropDuplicates(colonnes)
    return unique, avant - unique.count()


def pseudonymiser(colonne, sel):
    """Remplace une donnée personnelle par une empreinte SHA-256 salée.

    La même valeur donne toujours la même empreinte : on peut encore compter
    ou relier les lignes d'une même personne, sans connaître son identité.
    Le sel empêche de retrouver la valeur en calculant l'empreinte de valeurs connues.
    """
    return F.when(F.col(colonne).isNull(), None).otherwise(
        F.sha2(F.concat(F.lit(sel), F.col(colonne).cast("string")), 256))


def ecrire_partition(df, chemin):
    """Écrit en Parquet, en ne remplaçant que la partition date_ingestion concernée."""
    df.write.mode("overwrite").partitionBy("date_ingestion").parquet(chemin)


def ecrire_rejets(rejets, conf, source, colonnes_contenu):
    """Écrit les rejets dans /datalake/rejects/<source>, au même format pour toutes les sources.

    contenu : la ligne rejetée, en JSON, pour pouvoir l'examiner ou la rejouer.
    """
    a_ecrire = rejets.select(
        F.col(MOTIF),
        F.to_json(F.struct(*colonnes_contenu)).alias("contenu"),
        "_livraison", "_fichier_source",
        F.current_timestamp().alias("_rejete_le"),
        "date_ingestion")
    ecrire_partition(a_ecrire, f"{conf['datalake']}/rejects/{source}")


def bilan(source, total, propres, rejets, doublons, motifs):
    """Affiche le bilan et vérifie l'égalité propres + rejets + doublons = total."""
    somme = propres + rejets + doublons
    statut = "OK" if somme == total else "ECART"
    print(f"\n[silver] {source} : total={total} | propres={propres} + rejets={rejets} "
          f"+ doublons={doublons} = {somme} | {statut}")
    for r in motifs:
        print(f"[silver]   rejet {r[MOTIF]:<25} {r['count']:>7}")
    return statut == "OK"
