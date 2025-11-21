from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType


# 1. Initialisation Spark


spark = SparkSession.builder \
    .appName("ShopNow+ Streaming") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")


# 2. Schéma des événements

schema = StructType() \
    .add("event_type", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("quantity", IntegerType()) \
    .add("order_id", StringType()) \
    .add("total_amount", DoubleType()) \
    .add("payment_method", StringType()) \
    .add("stock_level", IntegerType()) \
    .add("warehouse_id", StringType()) \
    .add("timestamp", StringType())


# 3. Lecture depuis Kafka

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce") \
    .option("startingOffsets", "latest") \
    .load()


df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# 4. Traitement par type


# Commandes payées
df_commandes = df_json.filter(col("event_type") == "commande_payee") \
    .withColumn("statut_paiement", when(col("total_amount") > 100, "VIP").otherwise("standard"))

# Ajouts au panier
df_paniers = df_json.filter(col("event_type") == "ajout_au_panier")

# Mises à jour de stock
df_stock = df_json.filter(col("event_type") == "maj_stock") \
    .withColumn("alerte_stock", when(col("stock_level") < 10, "rupture proche").otherwise("ok"))



# 5. Écriture dans HDFS

# Commandes
df_commandes.writeStream \
    .format("parquet") \
    .option("path", "/user/shopnow/commandes") \
    .option("checkpointLocation", "/user/shopnow/checkpoints/commandes") \
    .outputMode("append") \
    .start()

# Paniers
df_paniers.writeStream \
    .format("parquet") \
    .option("path", "/user/shopnow/paniers") \
    .option("checkpointLocation", "/user/shopnow/checkpoints/paniers") \
    .outputMode("append") \
    .start()

# Stock
df_stock.writeStream \
    .format("parquet") \
    .option("path", "/user/shopnow/stock") \
    .option("checkpointLocation", "/user/shopnow/checkpoints/stock") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
