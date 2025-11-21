# ========================================
# JE CRÉE MON JOB SPARK POUR L'ECOMMERCE SHOPNOW+
# ========================================
# Mon objectif principal: lire les événements e-commerce depuis Kafka
# puis analyser les ventes et les écrire dans HDFS en Parquet

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, count, when, date_format, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

print("=" * 60)
print("🛒 JE DÉMARRE MON JOB SPARK POUR SHOPNOW+ ECOMMERCE")
print("=" * 60)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# Je dois initialiser Spark pour pouvoir faire du streaming
# J'ajoute aussi le package Kafka pour lire depuis Kafka
spark = SparkSession.builder \
    .appName("ShopNow-EcommercePipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Je diminue les logs pour voir que les messages importants
spark.sparkContext.setLogLevel("WARN")

print("✅ ÉTAPE 1: Ma session Spark est maintenant créée!")
print("   → Je peux lire depuis Kafka")
print("   → Je peux écrire dans HDFS")

# ========================================
# ÉTAPE 2: JE DÉFINIS LE SCHÉMA DES ÉVÉNEMENTS E-COMMERCE
# ========================================
# Je dois dire à Spark à quoi ressemble les données JSON que je vais recevoir
# Chaque événement (ajout panier, commande, etc) a une certaine structure

schema = StructType([
    StructField("type", StringType(), True),           # Type d'événement (ADD_TO_CART, CHECKOUT, PAYMENT, etc)
    StructField("clientId", StringType(), True),       # ID unique du client qui fait l'action
    StructField("produitId", StringType(), True),      # ID du produit concerné
    StructField("title", StringType(), True),          # Nom du produit (ex: "Laptop Dell")
    StructField("price", DoubleType(), True),          # Prix unitaire du produit (ex: 999.99)
    StructField("quantity", DoubleType(), True),       # Quantité commandée (ex: 2)
    StructField("timestamp", LongType(), True)         # Timestamp Unix de quand l'événement s'est passé
])

print("✅ ÉTAPE 2: J'ai défini le schéma JSON!")
print("   → Je sais qu'il y aura: type, clientId, produitId, title, price, quantity, timestamp")

# ========================================
# ÉTAPE 3: JE ME CONNECTE À KAFKA
# ========================================
# Je dois lire depuis Kafka pour recevoir les événements e-commerce en continu
# Les variables d'environnement me donnent l'adresse du serveur Kafka et le topic

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "cart-events")

print(f"🔗 Je vais lire depuis Kafka:")
print(f"   → Serveur: {kafka_bootstrap_servers}")
print(f"   → Topic: {kafka_topic}")

# Je créé un DataFrame streaming qui lit depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

print("✅ ÉTAPE 3: Je suis maintenant connecté à Kafka!")
print(f"   → Je reçois les événements du topic: {kafka_topic}")
print("   → Les données arrivent en continu")

# ========================================
# ÉTAPE 4: JE TRANSFORME LES MESSAGES KAFKA
# ========================================
# Les messages Kafka arrivent en format binaire JSON
# Je dois les parser (transformer) en DataFrame avec des colonnes exploitables

events_df = kafka_df.select(
    # Je récupère la clé du message Kafka = l'ID du client
    col("key").cast(StringType()).alias("clientId"),
    
    # Je parse le JSON du message (value) en utilisant mon schéma défini plus haut
    from_json(col("value").cast(StringType()), schema).alias("event")
).select(
    # Maintenant je sélectionne les colonnes que je veux vraiment garder
    col("clientId"),
    col("event.type").alias("eventType"),           # Je renomme pour que ce soit clair
    col("event.produitId").alias("productId"),
    col("event.title").alias("productName"),
    col("event.price").alias("unitPrice"),
    col("event.quantity").alias("quantity"),
    col("event.timestamp").alias("eventTimestamp")
)

print("✅ ÉTAPE 4: J'ai transformé les messages Kafka en DataFrame!")
print("   → J'ai extrait les colonnes importantes: clientId, eventType, productId, etc")
print("   → Les données sont maintenant prêtes à être analysées")

# ========================================
# ÉTAPE 5: J'ENRICHIS LES DONNÉES AVEC DES INDICATEURS
# ========================================
# Je vais calculer des infos utiles pour ShopNow+
# Par exemple: le montant de chaque vente, la date, etc

enriched_df = events_df.withColumn(
    "montant_vente",
    col("unitPrice") * col("quantity")  # Je calcule: prix unitaire × quantité
).withColumn(
    "ts",
    to_timestamp(col("eventTimestamp") / 1000)  # Je convertis le timestamp Unix en date lisible
).withColumn(
    "date_vente",
    date_format(col("ts"), "yyyy-MM-dd")  # Je crée une colonne date (YYYY-MM-DD)
).withColumn(
    "priorite_vente",
    when(col("montant_vente") > 1000, "HAUTE")      # Si vente > 1000€ = importante
    .when(col("montant_vente") > 100, "MOYENNE")
    .otherwise("BASSE")
)

print("✅ ÉTAPE 5: J'ai enrichi les données!")
print("   → J'ai ajouté: montant_vente, date_vente, priorite_vente")
print("   → Maintenant je peux analyser les ventes par jour, par importance, etc")

# ========================================
# ÉTAPE 6: J'ÉCRIS LES DONNÉES DANS HDFS
# ========================================
# Je sauvegarde tous les événements enrichis dans HDFS en format Parquet
# Parquet = format compressé et optimisé pour Spark/Hadoop

hdfs_output = os.getenv("HDFS_OUTPUT_PATH", "hdfs://namenode:9000/user/spark/kafka_stream/data")
checkpoint_location = os.getenv("CHECKPOINT_LOCATION", "hdfs://namenode:9000/user/spark/kafka_stream/checkpoints")

print(f"\n💾 Je vais écrire dans HDFS:")
print(f"   → Chemin: {hdfs_output}")
print(f"   → Format: Parquet (compressé)")
print(f"   → Checkpoint (pour redémarrage): {checkpoint_location}")

# Je crée le job streaming qui écrit en continu dans HDFS
query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", hdfs_output) \
    .option("checkpointLocation", checkpoint_location) \
    .partitionBy("date_vente") \
    .mode("append") \
    .start()

print("✅ ÉTAPE 6: J'ai lancé l'écriture dans HDFS!")
print("   → Les données arrivent et je les sauvegarde immédiatement")
print("   → Je partitionne par date pour retrouver les données facilement")

# ========================================
# ÉTAPE 7: JE TOURNE EN CONTINU
# ========================================
# Mon job ne doit jamais s'arrêter
# Il écoute Kafka en continu et écrit dans HDFS en continu

print("\n" + "=" * 60)
print("🔄 MON JOB SPARK TOURNE MAINTENANT EN CONTINU!")
print("=" * 60)
print("📊 CE QUE JE FAIS:")
print("   1. Je lis les événements e-commerce depuis Kafka")
print("   2. Je transforme et enrichis les données")
print("   3. J'écris tout dans HDFS en Parquet")
print("   4. ShopNow+ peut maintenant analyser les ventes")
print("=" * 60 + "\n")

query.awaitTermination()