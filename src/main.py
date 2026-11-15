from pyspark.sql import SparkSession, functions as F

def build_spark():
    return (SparkSession.builder.appName("BigDataStudentProject").getOrCreate())

def main():
    spark = build_spark()
    # TODO: ingestion Bronze -> cleaning Silver -> aggregations Gold
    transactions = spark.read.option("header", True).option("inferSchema", True).csv("/opt/project/data/transactions.csv")
    transactions.printSchema()
    print("rows:", transactions.count())
    spark.stop()

if __name__ == "__main__":
    main()
