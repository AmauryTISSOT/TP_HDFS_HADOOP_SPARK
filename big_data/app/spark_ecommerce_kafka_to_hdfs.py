# ========================================
# JE CRÉE MON JOB SPARK 1 POUR L'E-COMMERCE SHOPNOW+
# SCRIPT 1: LIRE KAFKA → TRANSFORMER → ÉCRIRE HDFS
# ========================================
# Mon objectif : lire les événements depuis Kafka,
# les enrichir avec des colonnes de calcul,
# et les sauvegarder dans HDFS en format Parquet

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format,
    when, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

print("=" * 70)
print("JE DÉMARRE MON JOB SPARK 1: KAFKA → HDFS")
print("=" * 70)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# J'initialise Spark pour pouvoir lire depuis Kafka
# et écrire dans HDFS

spark = SparkSession.builder \
    .appName("ShopNow-Kafka-To-HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n ÉTAPE 1: Ma session Spark est créée!")
print("    → Je peux lire depuis Kafka")
print("    → Je peux écrire dans HDFS")

# ========================================
# ÉTAPE 2: JE DÉFINIS LE SCHÉMA JSON
# ========================================
# Je vais lire des messages JSON depuis Kafka
# Le schéma décrit la structure que j'attends

schema = StructType([
    StructField("type", StringType(), True),        # VIEW_PRODUCT ou ADD_TO_CART
    StructField("produitId", StringType(), True),   # ID du produit
    StructField("title", StringType(), True),       # Nom du produit
    StructField("price", DoubleType(), True),       # Prix unitaire
    StructField("stock", DoubleType(), True),       # Stock (pour VIEW_PRODUCT)
    StructField("newStock", DoubleType(), True),    # Stock après achat (pour ADD_TO_CART)
    StructField("timestamp", LongType(), True)      # Timestamp Unix
])

print("\n ÉTAPE 2: J'ai défini le schéma JSON!")
print("    → Je sais qu'il y aura: type, produitId, title, price, stock/newStock, timestamp")

# ========================================
# ÉTAPE 3: JE RÉCUPÈRE LES CONFIGURATION
# ========================================
# Je lis les variables d'environnement pour savoir où écrire

kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "ecommerce")
hdfs_output = os.getenv("HDFS_OUTPUT_PATH", "hdfs://namenode:9000/user/spark/kafka_stream/data")
checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "hdfs://namenode:9000/user/spark/kafka_stream/checkpoints")

print("\n Ma configuration:")
print(f"    → Serveur Kafka: {kafka_servers}")
print(f"    → Topic: {kafka_topic}")
print(f"    → Sortie HDFS: {hdfs_output}")
print(f"    → Checkpoint: {checkpoint_location}")

# ========================================
# ÉTAPE 4: JE LIS DEPUIS KAFKA EN STREAMING
# ========================================
# Je me connecte à Kafka et je commence à lire les messages en continu

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

print("\n ÉTAPE 4: Je suis connecté à Kafka!")
print("    → Je reçois les événements du topic: ecommerce")
print("    → Les données arrivent en continu (streaming)")

# ========================================
# ÉTAPE 5: JE PARSE LES MESSAGES JSON
# ========================================
# Je transforme le message brut Kafka (bytes) en colonnes exploitables

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

print("\n ÉTAPE 5: J'ai parsé les messages Kafka!")
print("    → J'ai extrait les colonnes importantes")
print("    → Les données sont maintenant dans un DataFrame Spark")

# ========================================
# ÉTAPE 6: J'ENRICHIS LES DONNÉES
# ========================================
# J'ajoute des colonnes calculées utiles pour l'analyse

enriched_df = json_df \
    .withColumn("event_datetime", to_timestamp(col("timestamp") / 1000)) \
    .withColumn("event_date", date_format(col("event_datetime"), "yyyy-MM-dd")) \
    .withColumn("montant_potentiel", 
                when(col("type") == "ADD_TO_CART", col("price")).otherwise(0.0)) \
    .withColumn("prix_importance",
                when(col("price") > 500, "TRÈS_CHER")
                .when(col("price") > 100, "CHER")
                .otherwise("ABORDABLE")) \
    .withColumn("alerte_rupture",
                when((col("type") == "ADD_TO_CART") & (col("newStock") == 0), 1).otherwise(0)) \
    .withColumn("processing_date", to_timestamp(unix_timestamp()))

print("\n ÉTAPE 6: J'ai enrichi les données!")
print("    → J'ai ajouté: event_datetime, event_date, montant_potentiel")
print("    → J'ai ajouté: prix_importance, alerte_rupture, processing_date")
print("    → Ces colonnes vont servir pour l'analyse")

# ========================================
# ÉTAPE 7: J'ÉCRIS LES DONNÉES DANS HDFS
# ========================================
# Je sauvegarde les données en continu dans HDFS au format Parquet

query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", hdfs_output) \
    .option("checkpointLocation", checkpoint_location) \
    .outputMode("append") \
    .start()

print("\n ÉTAPE 7: J'ai lancé la sauvegarde dans HDFS!")
print(f"    → Format: Parquet")
print(f"    → Chemin: {hdfs_output}")
print(f"    → Mode: Append (ajout continu)")

print("\n" + "=" * 70)
print("JOB SPARK 1 ACTIF!")
print("=" * 70)
print("En attente de messages Kafka...")
print("Sauvegarde en HDFS: " + hdfs_output)
print("=" * 70)

# Je garde le job actif
query.awaitTermination()