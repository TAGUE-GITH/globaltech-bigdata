"""Exploration de la couche Bronze : découvrir et mesurer les anomalies d'une livraison.

L'énoncé ne liste pas les anomalies : ce programme les cherche et les compte,
source par source, avant que l'on écrive les règles de nettoyage de la couche Silver.
Il ne modifie aucune donnée.

Lancement :
    docker exec spark-client spark-submit src/exploration.py J1
"""
import sys

from pyspark.sql import functions as F

from commun import charger_conf, creer_spark

# Valeurs autorisées, d'après le dictionnaire des fichiers bruts (README du kit)
CANAUX = ["web", "edi", "commercial", "partenaire"]
STATUTS = ["COMPLETED", "SHIPPED", "CANCELLED", "RETURNED"]

# Format Apache combined, suivi du temps de réponse en millisecondes
REGEX_LOG = (r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) '
             r'"([^"]*)" "([^"]*)" (\d+)$')


def titre(texte):
    print("\n" + "=" * 70 + f"\n{texte}\n" + "=" * 70)


def ligne(libelle, valeur, total=None):
    pct = f"  ({100 * valeur / total:.2f} %)" if total else ""
    print(f"  {libelle:<55} {valeur:>8}{pct}")


def nb_valeurs_non_vides(df, col):
    return F.sum(F.when(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""), 1).otherwise(0))


def explorer_commandes(spark, base, date):
    titre("COMMANDES")
    df = spark.read.parquet(f"{base}/commandes").where(F.col("date_ingestion") == date).cache()
    total = df.count()
    ligne("Lignes au total", total)

    invalides = df.where(F.col("_ligne_invalide").isNotNull())
    ligne("Structure invalide (mauvais nombre de champs)", invalides.count(), total)
    ligne("  dont séparateur ';'", invalides.where(F.col("_ligne_invalide").contains(";")).count(), total)

    ok = df.where(F.col("_ligne_invalide").isNull())
    colonnes = [c for c in df.columns if not c.startswith("_") and c != "date_ingestion"]
    print("\n  Valeurs manquantes par colonne (lignes de structure valide) :")
    manquants = ok.select([(F.count("*") - nb_valeurs_non_vides(ok, c)).alias(c) for c in colonnes]).first()
    for c in colonnes:
        if manquants[c]:
            ligne(f"    {c}", manquants[c], total)

    print("\n  Doublons :")
    ligne("Lignes identiques (toutes colonnes métier)", ok.count() - ok.dropDuplicates(colonnes).count(), total)
    par_id = ok.groupBy("order_id").count().where("count > 1")
    ligne("order_id présents plusieurs fois", par_id.count(), total)
    ligne("Lignes en trop dues à ces order_id", par_id.select(F.sum(F.col("count") - 1)).first()[0] or 0, total)

    print("\n  Valeurs hors règles :")
    qte = F.col("quantity").cast("int")
    prix = F.col("unit_price_eur").cast("double")
    ts = F.to_timestamp("order_ts", "yyyy-MM-dd'T'HH:mm:ss")
    ligne("quantity non numérique", ok.where(F.col("quantity").isNotNull() & qte.isNull()).count(), total)
    ligne("quantity <= 0", ok.where(qte <= 0).count(), total)
    ligne("unit_price_eur non numérique", ok.where(F.col("unit_price_eur").isNotNull() & prix.isNull()).count(), total)
    ligne("unit_price_eur <= 0", ok.where(prix <= 0).count(), total)
    ligne("order_ts illisible", ok.where(F.col("order_ts").isNotNull() & ts.isNull()).count(), total)
    ligne("channel hors liste", ok.where(~F.col("channel").isin(CANAUX)).count(), total)
    ligne("status hors liste", ok.where(~F.col("status").isin(STATUTS)).count(), total)
    ligne("country absent ou pas sur 2 lettres majuscules",
          ok.where(~F.col("country").rlike("^[A-Z]{2}$") | F.col("country").isNull()).count(), total)

    print("\n  Répartition des valeurs :")
    for c in ["status", "channel", "country"]:
        vals = ok.groupBy(c).count().orderBy(F.desc("count")).collect()
        print(f"    {c:<8}: " + ", ".join(f"{r[c]}={r['count']}" for r in vals[:15]))
    bornes = ok.select(F.min(ts).alias("min"), F.max(ts).alias("max")).first()
    print(f"    order_ts : du {bornes['min']} au {bornes['max']}")
    hors_mois = ok.where(ts.isNotNull() & (F.date_format(ts, "yyyy-MM") != "2026-01")).count()
    ligne("order_ts hors du mois livré (janvier 2026)", hors_mois, total)

    print("\n  Déséquilibre de clé (skew) sur product_id :")
    par_produit = ok.groupBy("product_id").count()
    stats = par_produit.select(F.max("count").alias("max"),
                               F.percentile_approx("count", 0.5).alias("mediane")).first()
    top = par_produit.orderBy(F.desc("count")).first()
    print(f"    produit le plus fréquent : {top['product_id']} ({top['count']} lignes)")
    print(f"    médiane par produit      : {stats['mediane']} lignes")
    print(f"    ratio max / médiane      : {stats['max'] / stats['mediane']:.1f}")
    return ok


def explorer_clients(spark, base, date):
    titre("CLIENTS")
    df = spark.read.parquet(f"{base}/clients").where(F.col("date_ingestion") == date).cache()
    total = df.count()
    ligne("Lignes au total", total)
    ligne("Structure invalide", df.where(F.col("_ligne_invalide").isNotNull()).count(), total)
    colonnes = [c for c in df.columns if not c.startswith("_") and c != "date_ingestion"]
    print("\n  Valeurs manquantes par colonne :")
    manquants = df.select([(F.count("*") - nb_valeurs_non_vides(df, c)).alias(c) for c in colonnes]).first()
    for c in colonnes:
        if manquants[c]:
            ligne(f"    {c}", manquants[c], total)
    print("\n  Doublons :")
    ligne("Lignes identiques", total - df.dropDuplicates(colonnes).count(), total)
    par_id = df.groupBy("customer_id").count().where("count > 1")
    ligne("customer_id présents plusieurs fois", par_id.count(), total)
    ligne("email au format invalide",
          df.where(F.col("email").isNotNull() & ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")).count(), total)
    segs = df.groupBy("segment").count().orderBy(F.desc("count")).collect()
    print("    segment : " + ", ".join(f"{r['segment']}={r['count']}" for r in segs))
    return df


def explorer_produits(spark, base, date):
    titre("PRODUITS")
    df = spark.read.parquet(f"{base}/produits").where(F.col("date_ingestion") == date).cache()
    total = df.count()
    ligne("Lignes au total (une ligne = une version)", total)
    ligne("Structure invalide", df.where(F.col("_ligne_invalide").isNotNull()).count(), total)
    ligne("Produits distincts", df.select("product_id").distinct().count())
    versions = df.groupBy("product_id").count()
    print("    versions par produit : " + ", ".join(
        f"{r['count']} version(s)={r['n']}" for r in
        versions.groupBy("count").agg(F.count("*").alias("n")).orderBy("count").collect()))
    prix = F.col("unit_price_eur")
    ligne("Prix au format français (virgule décimale)", df.where(prix.contains(",")).count(), total)
    ligne("Prix non numérique (après remplacement de la virgule)",
          df.where(prix.isNotNull() & F.regexp_replace(prix, ",", ".").cast("double").isNull()).count(), total)
    ligne("Versions identiques en double (même produit, même date)",
          total - df.dropDuplicates(["product_id", "updated_at"]).count(), total)
    chg_famille = (df.groupBy("product_id").agg(F.countDistinct("family").alias("n")).where("n > 1").count())
    ligne("Produits ayant changé de famille", chg_famille)
    return df


def explorer_logs(spark, base, date):
    titre("LOGS WEB")
    df = spark.read.parquet(f"{base}/logs_web").where(F.col("date_ingestion") == date).cache()
    total = df.count()
    ligne("Lignes au total", total)
    ligne("Lignes vides", df.where(F.trim("ligne") == "").count(), total)
    lisibles = df.where(F.col("ligne").rlike(REGEX_LOG))
    ligne("Lignes au format Apache combined", lisibles.count(), total)
    ligne("Lignes mal formées (non vides)",
          df.where((F.trim("ligne") != "") & ~F.col("ligne").rlike(REGEX_LOG)).count(), total)
    exemples = df.where((F.trim("ligne") != "") & ~F.col("ligne").rlike(REGEX_LOG)).limit(3).collect()
    for r in exemples:
        print(f"    exemple : {r['ligne'][:110]}")
    statuts = (lisibles.select(F.regexp_extract("ligne", REGEX_LOG, 6).alias("statut"))
                       .groupBy("statut").count().orderBy("statut").collect())
    print("    codes HTTP : " + ", ".join(f"{r['statut']}={r['count']}" for r in statuts))


def main():
    livraison = sys.argv[1] if len(sys.argv) > 1 else "J1"
    conf = charger_conf()
    date = conf["dates_livraison"][livraison]
    base = f"{conf['datalake']}/bronze"
    spark = creer_spark(conf, f"exploration-{livraison}")
    spark.sparkContext.setLogLevel("WARN")

    commandes = explorer_commandes(spark, base, date)
    clients = explorer_clients(spark, base, date)
    produits = explorer_produits(spark, base, date)
    explorer_logs(spark, base, date)

    titre("COHÉRENCE ENTRE SOURCES")
    total = commandes.count()
    ligne("Commandes dont le client est inconnu",
          commandes.join(clients, "customer_id", "left_anti").count(), total)
    ligne("Commandes dont le produit est inconnu",
          commandes.join(produits.select("product_id").distinct(), "product_id", "left_anti").count(), total)
    spark.stop()


if __name__ == "__main__":
    main()
