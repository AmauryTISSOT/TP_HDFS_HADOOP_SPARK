# ========================================
# JE CRÉE MON JOB SPARK 2 POUR L'E-COMMERCE SHOPNOW+
# SCRIPT 2: LIRE HDFS → ANALYSER → INDICATEURS BUSINESS
# ========================================
# Mon objectif: lire les données que le job 1 a sauvegardées dans HDFS
# puis les analyser pour créer des indicateurs métier utiles pour ShopNow+
# (TOP produits, CA par jour, alertes rupture, etc.)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, desc, when

print("=" * 70)
print(" JE DÉMARRE MON JOB SPARK 2: HDFS → ANALYSE")
print("=" * 70)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# J'initialise Spark pour lire depuis HDFS et faire des calculs

spark = SparkSession.builder \
    .appName("ShopNow-EcommerceAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(" ÉTAPE 1: Ma session Spark est créée!")

# ========================================
# ÉTAPE 2: JE RÉCUPÈRE LES CHEMINS HDFS
# ========================================
# Je vais lire depuis le chemin où le job 1 a écrit les données
# Et j'écris les résultats d'analyse dans un autre chemin

hdfs_input = os.getenv("HDFS_INPUT_PATH", "hdfs://namenode:9000/user/spark/kafka_stream/data")
hdfs_output_indicators = os.getenv("HDFS_OUTPUT_INDICATORS", "hdfs://namenode:9000/user/spark/kafka_stream/indicators")

print(f"\n Mes chemins HDFS:")
print(f"   → Entrée (données brutes): {hdfs_input}")
print(f"   → Sortie (indicateurs): {hdfs_output_indicators}")

# ========================================
# ÉTAPE 3: JE LIS LES DONNÉES DEPUIS HDFS
# ========================================
# Je charge les données Parquet que le job 1 a écrites

print(f"\n Je lis les données depuis HDFS...")

try:
    orders_df = spark.read.parquet(hdfs_input)
    print(f" ÉTAPE 3: J'ai lu les données depuis HDFS!")
    print(f"   → Format: Parquet")
    print(f"   → Nombre d'événements: {orders_df.count()}")
except Exception as e:
    print(f" Erreur lecture HDFS: {e}")
    print(f"   → Les données ne sont peut-être pas encore disponibles")
    print(f"   → Attends que le job 1 écrive les données...")
    exit(1)

# ========================================
# ÉTAPE 4: JE CALCULE LES INDICATEURS BUSINESS
# ========================================

print("\n ÉTAPE 4: Je calcule les indicateurs business!")

# ========================================
# INDICATEUR 1: TOP 10 PRODUITS LES PLUS VUES (VIEW_PRODUCT)
# ========================================
# Je compte combien de fois chaque produit a été VU

print("\n Indicateur 1: TOP 10 PRODUITS LES PLUS VUES")

top_viewed_products = orders_df \
    .filter(col("eventType") == "VIEW_PRODUCT") \
    .groupBy("productId", "productName") \
    .agg(
        count("*").alias("nombre_vues")  # Je compte les vues
    ) \
    .orderBy(desc("nombre_vues")) \
    .limit(10)

print("   ✓ Groupé par produit")
print("   ✓ Compté les vues")
print("   ✓ Trié par nombre de vues décroissant")

# ========================================
# INDICATEUR 2: TOP 10 PRODUITS LES PLUS ACHETES (ADD_TO_CART)
# ========================================
# Je compte combien de fois chaque produit a été ACHETÉ

print("\n Indicateur 2: TOP 10 PRODUITS LES PLUS ACHETES")

top_bought_products = orders_df \
    .filter(col("eventType") == "ADD_TO_CART") \
    .groupBy("productId", "productName") \
    .agg(
        count("*").alias("nombre_achats"),  # Je compte les achats
        spark_sum("unitPrice").alias("ca_total")  # Je somme le CA (price × 1)
    ) \
    .orderBy(desc("nombre_achats")) \
    .limit(10)

print("   ✓ Filtré sur ADD_TO_CART")
print("   ✓ Compté les achats")
print("   ✓ Sommé le chiffre d'affaires")

# ========================================
# INDICATEUR 3: CHIFFRE D'AFFAIRES PAR JOUR
# ========================================
# Je calcule le CA total par jour

print("\n Indicateur 3: CHIFFRE D'AFFAIRES PAR JOUR")

daily_revenue = orders_df \
    .filter(col("eventType") == "ADD_TO_CART") \
    .groupBy("event_date") \
    .agg(
        spark_sum("unitPrice").alias("ca_jour"),  # CA total du jour
        count("*").alias("nombre_achats"),  # Nombre de commandes du jour
        avg("unitPrice").alias("panier_moyen")  # Panier moyen du jour
    ) \
    .orderBy(desc("ca_jour"))

print("   ✓ Groupé par date")
print("   ✓ Calculé CA total, nombre de commandes, panier moyen")

# ========================================
# INDICATEUR 4: ALERTES RUPTURE DE STOCK
# ========================================
# Je détecte les produits en rupture de stock (stock = 0 après achat)

print("\n  Indicateur 4: ALERTES RUPTURE DE STOCK")

stock_alerts = orders_df \
    .filter((col("eventType") == "ADD_TO_CART") & (col("stockAfter") == 0)) \
    .groupBy("productId", "productName") \
    .agg(
        count("*").alias("nombre_ruptures"),  # Combien de fois en rupture
        spark_sum("unitPrice").alias("ca_perdu")  # CA potentiellement perdu
    ) \
    .orderBy(desc("nombre_ruptures"))

print("   ✓ Filtré les ADD_TO_CART avec stock = 0")
print("   ✓ Compté les ruptures par produit")
print("   ✓ Calculé le CA potentiellement perdu")

# ========================================
# INDICATEUR 5: PRODUITS PAR GAMME DE PRIX
# ========================================
# Je groupe les produits par gamme de prix (cher vs pas cher)

print("\n Indicateur 5: PRODUITS PAR GAMME DE PRIX")

products_by_price = orders_df \
    .withColumn(
        "gamme_prix",
        when(col("unitPrice") > 500, "TRÈS_CHER")
        .when(col("unitPrice") > 100, "CHER")
        .otherwise("ABORDABLE")
    ) \
    .groupBy("productId", "productName", "gamme_prix") \
    .agg(
        count("*").alias("total_interactions")  # Interactions pour ce produit
    ) \
    .orderBy(desc("total_interactions"))

print("   ✓ Créé des gammes de prix")
print("   ✓ Groupé par gamme")
print("   ✓ Compté interactions par gamme")

# ========================================
# ÉTAPE 5: J'ÉCRIS LES RÉSULTATS DANS HDFS
# ========================================

print("\n ÉTAPE 5: J'écris les résultats dans HDFS!")

try:
    # J'écris TOP produits vus
    top_viewed_products.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{hdfs_output_indicators}/top_viewed_products")
    print(f"   ✓ TOP produits vus → {hdfs_output_indicators}/top_viewed_products")
    
    # J'écris TOP produits achetés
    top_bought_products.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{hdfs_output_indicators}/top_bought_products")
    print(f"   ✓ TOP produits achetés → {hdfs_output_indicators}/top_bought_products")
    
    # J'écris CA par jour
    daily_revenue.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{hdfs_output_indicators}/daily_revenue")
    print(f"   ✓ CA par jour → {hdfs_output_indicators}/daily_revenue")
    
    # J'écris alertes rupture
    stock_alerts.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{hdfs_output_indicators}/stock_alerts")
    print(f"   ✓ Alertes rupture → {hdfs_output_indicators}/stock_alerts")
    
    # J'écris produits par prix
    products_by_price.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{hdfs_output_indicators}/products_by_price")
    print(f"   ✓ Produits par prix → {hdfs_output_indicators}/products_by_price")
    
except Exception as e:
    print(f" Erreur écriture HDFS: {e}")
    exit(1)

# ========================================
# ÉTAPE 6: J'AFFICHE LES RÉSULTATS
# ========================================

print("\n" + "=" * 70)
print(" RÉSUMÉ DES INDICATEURS CALCULÉS")
print("=" * 70)

print("\n TOP 10 PRODUITS LES PLUS VUES:")
top_viewed_products.show(10, truncate=False)

print("\n TOP 10 PRODUITS LES PLUS ACHETES:")
top_bought_products.show(10, truncate=False)

print("\n CHIFFRE D'AFFAIRES PAR JOUR:")
daily_revenue.show(10, truncate=False)

print("\n  ALERTES RUPTURE DE STOCK:")
stock_alerts.show(10, truncate=False)

print("\n PRODUITS PAR GAMME DE PRIX:")
products_by_price.show(20, truncate=False)

print("\n" + "=" * 70)
print(" ANALYSE TERMINÉE!")
print("=" * 70)
print(" Les indicateurs sont sauvegardés dans HDFS:")
print(f"   → Chemin: {hdfs_output_indicators}")
print("   → Format: Parquet")
print("   → ShopNow+ peut maintenant voir les insights métier")
print("=" * 70)