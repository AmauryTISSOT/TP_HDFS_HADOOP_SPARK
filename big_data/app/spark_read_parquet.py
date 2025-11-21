import os
from pyspark.sql import SparkSession


def main():
    hdfs_output = "hdfs://namenode:9000/user/spark/kafka_stream/data"
    spark = SparkSession.builder.appName("ValidateParquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=== Lecture Parquet depuis HDFS ===")
    print("Path:", hdfs_output)

    try:
        df = spark.read.parquet(hdfs_output)
        print("Nombre de partitions/files (approx) :", len(df.rdd.glom().collect()))
        print("Schéma :")
        df.printSchema()
        print("Exemples (5 lignes) :")
        df.show(5, truncate=False)
        print("Comptage total (approx) :")
        print(df.count())
    except Exception as e:
        print("Erreur lecture Parquet :", e)


if __name__ == "__main__":
    main()
