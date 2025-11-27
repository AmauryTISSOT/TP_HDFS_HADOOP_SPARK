# ========================================
# JE CRÉE MON JOB SPARK 2 POUR L'ANALYSE DES KPIs
# SCRIPT 2: LIRE HDFS → ANALYSER → AFFICHER KPIs
# ========================================
# Mon objectif : lire les données écrites par Job 1 dans HDFS,
# les analyser pour extraire les KPIs métier,
# et les sauvegarder pour le reporting

import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, when, round as spark_round
from datetime import datetime

# ========================================
# CONFIGURATION LOGGING
# ========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("=" * 70)
print("JE DÉMARRE MON JOB SPARK 2: ANALYSE DES KPIs E-COMMERCE")
print("=" * 70)

# ========================================
# ÉTAPE 1: JE CRÉE MA SESSION SPARK
# ========================================
# Je crée une session Spark pour l'analyse batch des données HDFS

try:
    spark = SparkSession.builder \
        .appName("ShopNow-EcommerceAnalysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n ÉTAPE 1: Ma session Spark est créée!")
    print(f"    → Version Spark: {spark.version}")
    print(f"    → Master: {spark.sparkContext.master}")
    
except Exception as e:
    print(f"\n ERREUR lors de la création de la session: {e}")
    sys.exit(1)

# ========================================
# ÉTAPE 2: JE DÉFINIS LES CHEMINS HDFS
# ========================================
# Je définis où lire les données brutes et où écrire les KPIs

hdfs_input_path = "hdfs://namenode:9000/user/spark/kafka_stream/data"
hdfs_output_path = "hdfs://namenode:9000/user/spark/kafka_stream/indicators"

print("\n ÉTAPE 2: Je définis les chemins HDFS")
print(f"    → Entrée (données brutes): {hdfs_input_path}")
print(f"    → Sortie (KPIs): {hdfs_output_path}")

# ========================================
# ÉTAPE 3: JE LIS LES DONNÉES AVEC RETRY
# ========================================
# Je lis les données Parquet écrites par Job 1 (avec retry si pas prêtes)

print("\n" + "=" * 70)
print("JE LIS LES DONNÉES DEPUIS HDFS AVEC PATIENCE...")
print("=" * 70)

MAX_TENTATIVES = 6
DELAI_ATTENTE = 5  # secondes
df = None

# Je boucle et réessaie si les données ne sont pas encore là
for tentative in range(1, MAX_TENTATIVES + 1):
    try:
        print(f"\n Tentative {tentative}/{MAX_TENTATIVES}...", end=" ")
        
        # Je lis les données Parquet depuis HDFS
        df = spark.read.parquet(hdfs_input_path)
        
        # Je compte le nombre de lignes
        nombre_lignes = df.count()
        
        if nombre_lignes > 0:
            print(f"✓ SUCCÈS!")
            print(f"    → {nombre_lignes} événements trouvés et chargés")
            break  # Les données sont là, je quitte la boucle
        else:
            print(f"Dossier vide, je réessaie dans {DELAI_ATTENTE}sec...")
            time.sleep(DELAI_ATTENTE)
            
    except Exception as e:
        # Les données n'existent pas encore (normal au démarrage)
        if tentative < MAX_TENTATIVES:
            print(f"Données pas encore disponibles")
            print(f"    → Raison: {str(e)[:50]}...")
            print(f"    → Attente {DELAI_ATTENTE}sec avant tentative {tentative + 1}...")
            time.sleep(DELAI_ATTENTE)
        else:
            # Après 6 tentatives = 30 secondes d'attente total
            print(f"\n ERREUR définitive après {MAX_TENTATIVES} tentatives")
            print(f"    → Les données ne sont toujours pas disponibles en HDFS")
            print(f"    → Assurez-vous que Job 1 (sparkpy) écrit les données")
            print(f"    → Attente 120 secondes avant nouveau cycle...")
            time.sleep(120)
            sys.exit(0)  # Je ferme proprement pour que Docker redémarre

# Si j'arrive ici, j'ai les données!
print(f"\n Données chargées avec succès!")
print(f"    → Schéma détecté automatiquement:")
df.printSchema()

# ========================================
# ÉTAPE 4: JE MONTRE LES 5 PREMIERS ÉVÉNEMENTS
# ========================================
# Je montre un aperçu pour valider que les données sont correctes

print("\n Aperçu des 5 premiers événements:")
df.show(5, truncate=False)

# ========================================
# KPI 1: TOP 10 PRODUITS LES PLUS VUES
# ========================================
# Je compte combien de fois chaque produit a été consulté

print("\n" + "=" * 70)
print(" KPI 1: TOP 10 PRODUITS LES PLUS VUES")
print("=" * 70)

try:
    # Je filtre les événements VIEW_PRODUCT et je compte les occurrences
    top_viewed = df.filter(col("type") == "VIEW_PRODUCT") \
        .groupBy("produitId", "title") \
        .agg(count("*").alias("nombre_vues")) \
        .orderBy(col("nombre_vues").desc()) \
        .limit(10)
    
    print("\n Résultats:")
    top_viewed.show(10, truncate=False)
    
    # Je sauvegarde les résultats en Parquet
    top_viewed.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_output_path}/top_viewed_products")
    print(f"\n ✓ Résultats sauvegardés: {hdfs_output_path}/top_viewed_products")
    
except Exception as e:
    print(f" ✗ Erreur calcul TOP VUES: {e}")

# ========================================
# KPI 2: TOP 10 PRODUITS LES PLUS ACHETÉS + CA
# ========================================
# Je compte les achats et je calcule le chiffre d'affaires par produit

print("\n" + "=" * 70)
print(" KPI 2: TOP 10 PRODUITS LES PLUS ACHETÉS + CHIFFRE D'AFFAIRES")
print("=" * 70)

try:
    # Je filtre les ajouts au panier (achats) et je calcule le CA
    top_bought = df.filter(col("type") == "ADD_TO_CART") \
        .groupBy("produitId", "title", "price") \
        .agg(
            count("*").alias("nombre_achats"),
            (col("price") * count("*")).alias("CA_total")
        ) \
        .orderBy(col("nombre_achats").desc()) \
        .limit(10)
    
    print("\n Résultats:")
    top_bought.show(10, truncate=False)
    
    # Je sauvegarde
    top_bought.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_output_path}/top_bought_products")
    print(f"\n ✓ Résultats sauvegardés: {hdfs_output_path}/top_bought_products")
    
except Exception as e:
    print(f" ✗ Erreur calcul TOP ACHATS: {e}")

# ========================================
# KPI 3: CHIFFRE D'AFFAIRES PAR JOUR
# ========================================
# Je calcule le CA total par jour

print("\n" + "=" * 70)
print(" KPI 3: CHIFFRE D'AFFAIRES PAR JOUR")
print("=" * 70)

try:
    # Je groupe par jour et je somme les montants des achats
    daily_revenue = df.filter(col("type") == "ADD_TO_CART") \
        .groupBy("event_date") \
        .agg(
            spark_sum("montant_potentiel").alias("CA_jour"),
            count("*").alias("nombre_achats")
        ) \
        .orderBy("event_date")
    
    print("\n Résultats:")
    daily_revenue.show(20, truncate=False)
    
    # Je sauvegarde
    daily_revenue.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_output_path}/daily_revenue")
    print(f"\n ✓ Résultats sauvegardés: {hdfs_output_path}/daily_revenue")
    
except Exception as e:
    print(f" ✗ Erreur calcul CA/JOUR: {e}")

# ========================================
# KPI 4: ALERTES RUPTURE DE STOCK
# ========================================
# Je détecte tous les produits passés en rupture (stock = 0)

print("\n" + "=" * 70)
print(" KPI 4: ALERTES RUPTURE DE STOCK (stock = 0)")
print("=" * 70)

try:
    # Je filtre les événements ADD_TO_CART où newStock = 0 (rupture!)
    stock_alerts = df.filter((col("type") == "ADD_TO_CART") & (col("newStock") == 0)) \
        .select(
            col("event_datetime"),
            col("produitId"),
            col("title"),
            col("newStock").alias("stock_final"),
            col("alerte_rupture")
        ) \
        .orderBy(col("event_datetime").desc())
    
    nombre_alertes = stock_alerts.count()
    print(f"\n Total d'alertes rupture: {nombre_alertes}")
    print("\n Résultats:")
    stock_alerts.show(20, truncate=False)
    
    # Je sauvegarde
    stock_alerts.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_output_path}/stock_alerts")
    print(f"\n ✓ Résultats sauvegardés: {hdfs_output_path}/stock_alerts")
    
except Exception as e:
    print(f" ✗ Erreur calcul ALERTES: {e}")

# ========================================
# KPI 5: PRODUITS PAR GAMME DE PRIX
# ========================================
# Je catégorise les produits selon leur gamme de prix

print("\n" + "=" * 70)
print(" KPI 5: PRODUITS PAR GAMME DE PRIX")
print("=" * 70)

try:
    # Je crée une gamme de prix et je compte les produits distincts par gamme
    price_ranges = df.select(
        col("produitId"),
        col("title"),
        col("price"),
        # Je catégorise selon le prix
        when(col("price") >= 500, "TRÈS_CHER (500€+)") \
        .when(col("price") >= 100, "CHER (100-500€)") \
        .when(col("price") >= 30, "MOYEN (30-100€)") \
        .otherwise("ABORDABLE (<30€)").alias("gamme_prix")
    ) \
    .dropDuplicates(["produitId"]) \
    .groupBy("gamme_prix") \
    .agg(count("produitId").alias("nombre_produits")) \
    .orderBy("gamme_prix")
    
    print("\n Résultats:")
    price_ranges.show(truncate=False)
    
    # Je sauvegarde
    price_ranges.coalesce(1).write.mode("overwrite").parquet(f"{hdfs_output_path}/products_by_price")
    print(f"\n ✓ Résultats sauvegardés: {hdfs_output_path}/products_by_price")
    
except Exception as e:
    print(f" ✗ Erreur calcul GAMME PRIX: {e}")

# ========================================
# KPI 6: STATISTIQUES GLOBALES
# ========================================
# Je calcule les statistiques globales de la plateforme

print("\n" + "=" * 70)
print(" KPI 6: STATISTIQUES GLOBALES SHOPNOW+")
print("=" * 70)

try:
    total_events = df.count()
    total_views = df.filter(col("type") == "VIEW_PRODUCT").count()
    total_purchases = df.filter(col("type") == "ADD_TO_CART").count()
    total_revenue = df.filter(col("type") == "ADD_TO_CART").agg(spark_sum("montant_potentiel")).collect()[0][0]
    
    print(f"\n Résumé global:")
    print(f"    → Total événements: {total_events}")
    print(f"    → Total consultations: {total_views}")
    print(f"    → Total achats: {total_purchases}")
    print(f"    → Chiffre d'affaires total: {total_revenue:,.2f}€")
    print(f"    → Taux de conversion: {(total_purchases / total_views * 100):.1f}%" if total_views > 0 else "    → Taux de conversion: N/A")
    
except Exception as e:
    print(f" ✗ Erreur calcul stats globales: {e}")

# ========================================
# RÉSUMÉ FINAL
# ========================================

print("\n" + "=" * 70)
print(" ✓ JOB SPARK 2 TERMINÉ AVEC SUCCÈS!")
print("=" * 70)
print("\n KPIs CALCULÉS:")
print(f"   ✓ TOP 10 produits vus → {hdfs_output_path}/top_viewed_products")
print(f"   ✓ TOP 10 produits achetés + CA → {hdfs_output_path}/top_bought_products")
print(f"   ✓ CA par jour → {hdfs_output_path}/daily_revenue")
print(f"   ✓ Alertes rupture stock → {hdfs_output_path}/stock_alerts")
print(f"   ✓ Produits par gamme prix → {hdfs_output_path}/products_by_price")

print(f"\n Exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# Je ferme la session Spark
spark.stop()
print("\n Session Spark fermée")