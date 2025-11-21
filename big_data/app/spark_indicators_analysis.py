# ========================================
# JE CRÉE MON JOB SPARK D'ANALYSE SHOPNOW+
# ========================================
# Mon objectif: lire les ventes depuis HDFS
# puis calculer les indicateurs métier (TOP produits, CA, etc)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, desc, rank, window
from pyspark.sql.window import Window

print("=" * 60)
print("📊 JE DÉMARRE MON JOB D'ANALYSE SHOPNOW+")
print("=" * 60)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# J'initialise Spark pour lire depuis HDFS et faire des calculs

spark = SparkSession.builder \
    .appName("ShopNow-IndicatorsAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ ÉTAPE 1: Ma session Spark est créée!")

# ========================================
# ÉTAPE 2: JE LIS LES DONNÉES DEPUIS HDFS
# ========================================
# Je lis les données que mon premier job a écrites dans HDFS
# Ces données sont en format Parquet partitionné par date

hdfs_input = os.getenv("HDFS_INPUT_PATH", "hdfs://namenode:9000/user/spark/kafka_stream/data")

print(f"📖 Je lis les données depuis HDFS:")
print(f"   → Chemin: {hdfs_input}")

# Je charge les données Parquet
orders_df = spark.read.parquet(hdfs_input)

print("✅ ÉTAPE 2: J'ai lu les données depuis HDFS!")
print(f"   → Format: Parquet")
print(f"   → Nombre d'événements: {orders_df.count()}")

# ========================================
# ÉTAPE 3: JE CALCULE LES INDICATEURS
# ========================================

print("\n✅ ÉTAPE 3: Je calcule les indicateurs métier!")

# INDICATEUR 1: TOP 10 produits les plus vendus
print("\n📈 Indicateur 1: TOP 10 PRODUITS LES PLUS VENDUS")
top_products = orders_df \
    .groupBy("productId", "productName") \
    .agg(
        spark_sum("quantity").alias("total_quantity"),  # Je compte les quantités
        spark_sum("montant_vente").alias("total_revenue"),  # Je somme le chiffre d'affaires
        count("clientId").alias("nombre_achats")  # Je compte les achats
    ) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

print("   → Je groupe par produit")
print("   → Je somme la quantité, le chiffre d'affaires, les achats")
print("   → Je trie par revenus décroissants")

# INDICATEUR 2: Chiffre d'affaires par jour
print("\n💰 Indicateur 2: CHIFFRE D'AFFAIRES PAR JOUR")
daily_revenue = orders_df \
    .groupBy("date_vente") \
    .agg(
        spark_sum("montant_vente").alias("ca_jour"),  # CA total du jour
        count("clientId").alias("nombre_commandes"),  # Nombre de commandes
        avg("montant_vente").alias("panier_moyen")  # Panier moyen
    ) \
    .orderBy(desc("ca_jour"))

print("   → Je groupe par date")
print("   → Je calcule CA total, nombre de commandes, panier moyen")
print("   → Je trie par CA décroissant")

# INDICATEUR 3: Produits en rupture de stock
print("\n⚠️ Indicateur 3: ALERTES RUPTURE DE STOCK")
stock_alerts = orders_df \
    .where(col("eventType") == "CHECKOUT") \
    .groupBy("productId", "productName") \
    .agg(
        spark_sum("quantity").alias("total_vendu")
    ) \
    .where(col("total_vendu") > 50) \
    .orderBy(desc("total_vendu"))

print("   → Je filtre sur les CHECKOUT (vraies commandes)")
print("   → Je détecte produits avec plus de 50 ventes = risque rupture")

# INDICATEUR 4: Clients les plus dépensiers
print("\n👑 Indicateur 4: TOP 10 CLIENTS")
top_clients = orders_df \
    .groupBy("clientId") \
    .agg(
        spark_sum("montant_vente").alias("total_depense"),  # Total dépensé
        count("*").alias("nombre_actes")  # Nombre d'interactions
    ) \
    .orderBy(desc("total_depense")) \
    .limit(10)

print("   → Je groupe par client")
print("   → Je somme les dépenses")
print("   → Je trie par montant décroissant")

# ========================================
# ÉTAPE 4: J'ÉCRIS LES RÉSULTATS DANS HDFS
# ========================================

hdfs_output = os.getenv("HDFS_OUTPUT_INDICATORS", "hdfs://namenode:9000/user/hive/warehouse/shopnow/indicators")

print(f"\n💾 J'écris les indicateurs dans HDFS:")
print(f"   → Chemin: {hdfs_output}")

# J'écris le TOP 10 produits
top_products.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{hdfs_output}/top_products")
print("   ✅ TOP produits sauvegardé")

# J'écris le CA par jour
daily_revenue.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{hdfs_output}/daily_revenue")
print("   ✅ CA par jour sauvegardé")

# J'écris les alertes rupture
stock_alerts.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{hdfs_output}/stock_alerts")
print("   ✅ Alertes rupture sauvegardées")

# J'écris le TOP clients
top_clients.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(f"{hdfs_output}/top_clients")
print("   ✅ TOP clients sauvegardé")

# ========================================
# ÉTAPE 5: J'AFFICHE UN RÉSUMÉ
# ========================================

print("\n" + "=" * 60)
print("📊 RÉSUMÉ DES INDICATEURS CALCULÉS")
print("=" * 60)

print("\n🏆 TOP 10 PRODUITS:")
top_products.show()

print("\n💰 CHIFFRE D'AFFAIRES PAR JOUR:")
daily_revenue.show()

print("\n⚠️ ALERTES RUPTURE DE STOCK:")
stock_alerts.show()

print("\n👑 TOP 10 CLIENTS:")
top_clients.show()

print("\n" + "=" * 60)
print("✅ ANALYSE TERMINÉE!")
print("   → Les indicateurs sont sauvegardés dans HDFS")
print("   → ShopNow+ peut maintenant voir les insights métier")
print("=" * 60)