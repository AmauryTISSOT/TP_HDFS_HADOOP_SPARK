import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp, date_format
from pyspark.sql.types import *


def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "animal")
    hdfs_output = os.getenv(
        "HDFS_OUTPUT_PATH", "hdfs://namenode:9000/user/petcare/animals"
    )
    checkpoint = os.getenv(
        "CHECKPOINT_LOCATION", "hdfs://namenode:9000/user/petcare/checkpoints"
    )

    spark = SparkSession.builder.appName("PetCare_Kafka_to_Parquet").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("animal_id", StringType()),
            StructField("espece", StringType()),
            StructField("poids", DoubleType()),
            StructField("temperature", DoubleType()),
            StructField("alerte", StringType()),
            StructField("vet", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = kafka_df.select(col("value").cast("string").alias("json_str"))

    parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select(
        "data.*"
    )

    parsed = parsed.withColumn("ts", to_timestamp(col("timestamp"))).withColumn(
        "date", date_format(col("ts"), "yyyy-MM-dd")
    )

    parsed = parsed.withColumn(
        "en_alerte",
        (col("alerte").isNotNull() & (col("alerte") != "RAS"))
        | (col("temperature") > 39.0)
        | (col("temperature") < 35.5),
    )

    parsed = parsed.withColumn(
        "niveau_urgence",
        when(col("alerte") == "fièvre", "HAUT")
        .when(col("alerte") == "léthargie", "HAUT")
        .when(col("alerte") == "vomissements", "MOYEN")
        .when(col("alerte") == "boiterie", "MOYEN")
        .when(col("alerte") == "toux", "MOYEN")
        .when(col("alerte") == "RAS", "FAIBLE")
        .otherwise(
            # si alerte inconnue mais température critique -> HAUT
            when(
                (col("temperature") > 39.0) | (col("temperature") < 35.5), "HAUT"
            ).otherwise("MOYEN")
        ),
    )

    result = parsed.select(
        "animal_id",
        "espece",
        "poids",
        "temperature",
        "alerte",
        "vet",
        "timestamp",
        "ts",
        "date",
        "en_alerte",
        "niveau_urgence",
    )

    query = (
        result.writeStream.format("parquet")
        .option("path", hdfs_output)
        .option("checkpointLocation", checkpoint)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )

    print("=== STREAMING PARQUET DÉMARRÉ ===")
    print(f"Kafka -> {kafka_topic}")
    print(f"HDFS output -> {hdfs_output}")
    print(f"Checkpoint -> {checkpoint}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
