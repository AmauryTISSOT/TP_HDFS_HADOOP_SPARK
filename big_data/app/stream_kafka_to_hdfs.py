import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format
from pyspark.sql.types import *


def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "ecommerce")
    hdfs_output = os.getenv(
        "HDFS_OUTPUT_PATH", "hdfs://namenode:9000/user/ecommerce/events"
    )
    checkpoint = os.getenv(
        "CHECKPOINT_LOCATION", "hdfs://namenode:9000/user/ecommerce/checkpoints"
    )

    spark = SparkSession.builder.appName("Ecommerce_Kafka_to_HDFS").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    #  Schéma des messages envoyés par ton backend Express
    schema = StructType(
        [
            StructField("type", StringType()),
            StructField("produitId", StringType()),
            StructField("title", StringType()),
            StructField("price", DoubleType()),
            StructField("userId", StringType()),
            StructField("timestamp", LongType()),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Convertit la valeur Kafka en string JSON
    json_df = kafka_df.select(col("value").cast("string").alias("json_str"))

    # Parse JSON → colonnes exploitables
    parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select(
        "data.*"
    )

    # Convertit timestamp (ms) → timestamp Spark
    parsed = parsed.withColumn("ts", to_timestamp(col("timestamp") / 1000))

    # Partition par jour
    parsed = parsed.withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))

    # Colonnes finales
    result = parsed.select(
        "type", "produitId", "title", "price", "userId", "timestamp", "ts", "date"
    )

    #  Écriture continue vers HDFS en Parquet
    query = (
        result.writeStream.format("parquet")
        .option("path", hdfs_output)
        .option("checkpointLocation", checkpoint)
        .partitionBy("date")
        .outputMode("append")
        .start()
    )

    print("=== STREAMING ECOMMERCE → HDFS DÉMARRÉ ===")
    print(f"Topic Kafka : {kafka_topic}")
    print(f"HDFS output : {hdfs_output}")
    print(f"Checkpoint : {checkpoint}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
