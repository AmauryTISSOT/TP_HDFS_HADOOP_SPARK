import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "animal")

    spark = SparkSession.builder.appName("KafkaConsoleStream").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=== CONFIG STREAM CONSOLE ===")
    print(f"KAFKA_BOOTSTRAP_SERVERS = {kafka_bootstrap_servers}")
    print(f"KAFKA_TOPIC             = {kafka_topic}")
    print("=============================")

    # Lecture des messages depuis Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # On ne garde que le value (en string) + timestamp pour voir quelque chose de lisible
    messages_df = kafka_df.select(
        col("timestamp"), col("value").cast("string").alias("value")
    )

    # Écriture en streaming vers la console
    query = (
        messages_df.writeStream.outputMode(
            "append"
        )  # on affiche chaque nouveau message
        .format("console")  # console = stdout
        .option("truncate", "false")  # on n’abrège pas les colonnes
        .start()
    )

    print("=== STREAMING CONSOLE DÉMARRÉ ===")
    print("Kafka -> Console (Spark Structured Streaming)")
    print("Regarde les logs du conteneur pour voir les messages :)")

    query.awaitTermination()


if __name__ == "__main__":
    main()
