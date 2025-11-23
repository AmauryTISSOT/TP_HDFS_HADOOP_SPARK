# ========================================
# JE CRÉE MON JOB SPARK POUR L'E-COMMERCE SHOPNOW+
# SCRIPT 1: LIRE KAFKA → TRANSFORMER → ÉCRIRE HDFS
# ========================================
# Mon objectif: lire les événements e-commerce depuis Kafka
# puis les transformer et les sauvegarder dans HDFS en Parquet
# pour que le job d'analyse puisse les utiliser

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp, date_format, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

print("=" * 70)
print(" JE DÉMARRE MON JOB SPARK 1: KAFKA → HDFS")
print("=" * 70)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# Je dois initialiser Spark pour pouvoir faire du streaming
# J'ajoute aussi le package Kafka pour lire depuis Kafka

spark = SparkSession.builder \
    .appName("ShopNow-EcommercePipeline-KafkaToHDFS") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Je diminue les logs pour voir que les messages importants
spark.sparkContext.setLogLevel("WARN")

print(" ÉTAPE 1: Ma session Spark est créée!")
print("   → Je peux lire depuis Kafka")
print("   → Je peux écrire dans HDFS")

# ========================================
# ÉTAPE 2: JE DÉFINIS LE SCHÉMA DES ÉVÉNEMENTS KAFKA
# ========================================
# Je dois dire à Spark à quoi ressemblent les données JSON que je vais recevoir
# Chaque événement (VIEW_PRODUCT, ADD_TO_CART) a une certaine structure

schema = StructType([
    StructField("type", StringType(), True),           # Type d'événement (VIEW_PRODUCT ou ADD_TO_CART)
    StructField("produitId", StringType(), True),      # ID unique du produit
    StructField("title", StringType(), True),          # Nom du produit (ex: "Laptop Dell")
    StructField("price", DoubleType(), True),          # Prix unitaire du produit (ex: 999.99)
    StructField("stock", DoubleType(), True),          # Stock actuel (pour VIEW_PRODUCT)
    StructField("newStock", DoubleType(), True),       # Stock après achat (pour ADD_TO_CART)
    StructField("timestamp", LongType(), True)         # Timestamp Unix de quand l'événement s'est passé
])

print(" ÉTAPE 2: J'ai défini le schéma JSON!")
print("   → Je sais qu'il y aura: type, produitId, title, price, stock/newStock, timestamp")

# ========================================
# ÉTAPE 3: JE RÉCUPÈRE LES VARIABLES DE CONFIGURATION
# ========================================
# Les chemins HDFS et autres paramètres viennent du docker-compose.yml
# Si je n'en donne pas, j'utilise des valeurs par défaut

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "ecommerce")
hdfs_output = os.getenv("HDFS_OUTPUT_PATH", "hdfs://namenode:9000/user/spark/kafka_stream/data")
checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "hdfs://namenode:9000/user/spark/kafka_stream/checkpoints")

print(f"\n Ma configuration:")
print(f"   → Serveur Kafka: {kafka_bootstrap_servers}")
print(f"   → Topic: {kafka_topic}")
print(f"   → Sortie HDFS: {hdfs_output}")
print(f"   → Checkpoint: {checkpoint_location}")

# ========================================
# ÉTAPE 4: JE ME CONNECTE À KAFKA EN STREAMING
# ========================================
# Je crée un DataFrame streaming qui lit depuis Kafka en continu
# Je dois spécifier le serveur, le topic, et quand commencer (latest = derniers messages)

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

print("\n ÉTAPE 4: Je suis connecté à Kafka!")
print(f"   → Je reçois les événements du topic: {kafka_topic}")
print("   → Les données arrivent en continu (streaming)")

# ========================================
# ÉTAPE 5: JE PARSE LES MESSAGES KAFKA EN JSON
# ========================================
# Les messages Kafka arrivent en format binaire (bytes)
# Je dois les transformer en JSON et extraire les champs que je veux

events_df = kafka_df.select(
    # Je récupère la clé du message Kafka = l'ID du client (ou du produit)
    col("key").cast(StringType()).alias("clientOrProductId"),
    
    # Je parse le JSON du message (value) en utilisant mon schéma défini plus haut
    from_json(col("value").cast(StringType()), schema).alias("event"),
    
    # Je garde aussi le timestamp du message Kafka lui-même
    col("timestamp").alias("kafka_timestamp")
).select(
    # Maintenant je sélectionne les colonnes que je veux vraiment garder
    col("clientOrProductId"),
    col("event.type").alias("eventType"),           # Type: VIEW_PRODUCT ou ADD_TO_CART
    col("event.produitId").alias("productId"),
    col("event.title").alias("productName"),
    col("event.price").alias("unitPrice"),
    col("event.stock").alias("stockBefore"),        # Stock avant (pour VIEW)
    col("event.newStock").alias("stockAfter"),      # Stock après (pour ADD_TO_CART)
    col("event.timestamp").alias("eventTimestamp"),
    col("kafka_timestamp")
)

print("\n ÉTAPE 5: J'ai parsé les messages Kafka!")
print("   → J'ai extrait les colonnes importantes")
print("   → Les données sont maintenant dans un DataFrame Spark")

# ========================================
# ÉTAPE 6: J'ENRICHIS LES DONNÉES AVEC DES COLONNES CALCULÉES
# ========================================
# Je vais ajouter des colonnes utiles pour l'analyse
# Par exemple: une colonne date, une colonne montant (pour futur), etc

enriched_df = events_df.withColumn(
    # Je convertis le timestamp Unix en timestamp lisible
    "event_datetime",
    to_timestamp(col("eventTimestamp") / 1000)
).withColumn(
    # Je crée une colonne date (YYYY-MM-DD) pour grouper par jour
    "event_date",
    date_format(col("event_datetime"), "yyyy-MM-dd")
).withColumn(
    # Je crée une colonne pour le montant potentiel de la vente (price × 1 car qty implicite)
    "montant_potentiel",
    when(col("eventType") == "ADD_TO_CART", col("unitPrice"))
    .otherwise(0)
).withColumn(
    # Je crée une colonne d'importance basée sur le prix
    "prix_importance",
    when(col("unitPrice") > 500, "HAUTE")
    .when(col("unitPrice") > 100, "MOYENNE")
    .otherwise("BASSE")
).withColumn(
    # Je crée une colonne d'alerte rupture de stock
    "alerte_rupture",
    when((col("stockAfter") == 0) & (col("eventType") == "ADD_TO_CART"), 1)
    .otherwise(0)
).withColumn(
    # Je garde la date de traitement (quand Spark traite le message)
    "processing_date",
    current_timestamp()
)

print("\n ÉTAPE 6: J'ai enrichi les données!")
print("   → J'ai ajouté: event_datetime, event_date, montant_potentiel")
print("   → J'ai ajouté: prix_importance, alerte_rupture, processing_date")
print("   → Ces colonnes vont servir pour l'analyse")

# ========================================
# ÉTAPE 7: J'ÉCRIS LES DONNÉES DANS HDFS EN PARQUET
# ========================================
# Je sauvegarde tous les événements enrichis dans HDFS
# Format Parquet = compressé et optimisé pour Spark/Hadoop
# Je partitionne par date pour retrouver les données facilement

query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", hdfs_output) \
    .option("checkpointLocation", checkpoint_location) \
    .partitionBy("event_date") \
    .mode("append") \
    .start()

print("\n ÉTAPE 7: J'ai lancé l'écriture dans HDFS!")
print(f"   → Chemin: {hdfs_output}")
print("   → Format: Parquet (compressé et optimisé)")
print("   → Partitionné par date pour facile recherche")
print("   → Mode: append (je rajoute à chaque fois)")

# ========================================
# ÉTAPE 8: JE TOURNE EN CONTINU
# ========================================
# Mon job ne doit jamais s'arrêter
# Il écoute Kafka en continu et écrit dans HDFS en continu

print("\n" + "=" * 70)
print("MON JOB SPARK 1 TOURNE MAINTENANT EN CONTINU!")
print("=" * 70)
print("CE QUE JE FAIS:")
print("   1. Je lis les événements e-commerce depuis Kafka (topic: ecommerce)")
print("   2. Je transforme et enrichis les données")
print("   3. J'écris tout dans HDFS en Parquet (/user/spark/kafka_stream/data)")
print("   4. Les données sont partitionnées par date")
print("   5. ShopNow+ peut maintenant analyser ces données")
print("=" * 70 + "\n")

# Je garde la requête active pour toujours
query.awaitTermination()