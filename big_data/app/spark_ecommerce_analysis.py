"""
JOB SPARK 2 - ANALYSE DES KPIs E-COMMERCE
Je lis les données brutes écrites par Job 1, je les analyse et j'extrais les KPIs business
"""

import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, when
from datetime import datetime

# ===================================================================
# CONFIGURATION LOGGING
# ===================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================================================================
# ÉTAPE 1: INITIALISATION SPARK SESSION
# ===================================================================
print("=" * 70)
print("JOB SPARK 2: ANALYSE DES KPIs E-COMMERCE")
print("=" * 70)

try:
    # Je crée ma session Spark pour l'analyse
    spark = SparkSession.builder \
        .appName("ShopNow-EcommerceAnalysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
        .getOrCreate()
    
    print("\n ÉTAPE 1: Ma session Spark est créée!")
    print(f"   → Version Spark: {spark.version}")
    print(f"   → Master: {spark.sparkContext.master}")
    
except Exception as e:
    print(f"\n ERREUR lors de la création de la session: {e}")
    sys.exit(1)

# ===================================================================
# ÉTAPE 2: DÉFINITION DES CHEMINS HDFS
# ===================================================================
hdfs_input_path = "hdfs://namenode:9000/user/spark/kafka_stream/data"
hdfs_output_path = "hdfs://namenode:9000/user/spark/kafka_stream/indicators"

print("\n ÉTAPE 2: Chemins HDFS définis")
print(f"   → Entrée (données brutes): {hdfs_input_path}")
print(f"   → Sortie (KPIs): {hdfs_output_path}")

# ===================================================================
# ÉTAPE 3: LECTURE DES DONNÉES AVEC RETRY (PARTIE CRITIQUE!)
# ===================================================================
print("\n" + "=" * 70)
print("Je lis les données depuis HDFS avec patience...")
print("=" * 70)

# Je configure les paramètres de retry
MAX_TENTATIVES = 6
DELAI_ATTENTE = 5  # secondes
df = None

# Je boucle et réessaie si les données ne sont pas là
for tentative in range(1, MAX_TENTATIVES + 1):
    try:
        print(f"\n Tentative {tentative}/{MAX_TENTATIVES}...", end=" ")
        
        # Je lis les données Parquet depuis HDFS
        df = spark.read.parquet(hdfs_input_path)
        
        # Si on arrive ici, les données existent!
        nombre_lignes = df.count()
        
        if nombre_lignes > 0:
            print(f"SUCCÈS!")
            print(f"   → {nombre_lignes} événements trouvés et chargés")
            break  # Je quitte la boucle, les données sont là
        else:
            print(f"Dossier vide, réessai dans {DELAI_ATTENTE}sec...")
            time.sleep(DELAI_ATTENTE)
            
    except Exception as e:
        # Les données n'existent pas encore (normal au démarrage)
        if tentative < MAX_TENTATIVES:
            print(f" Données pas encore disponibles")
            print(f"   → Raison: {str(e)[:50]}...")
            print(f"   → Attente {DELAI_ATTENTE}sec avant tentative {tentative + 1}...")
            time.sleep(DELAI_ATTENTE)
        else:
            # Après 6 tentatives = 30 secondes d'attente
            print(f"\n ERREUR définitive après {MAX_TENTATIVES} tentatives")
            print(f"   → Les données ne sont toujours pas disponibles en HDFS")
            print(f"   → Assurez-vous que Job 1 (sparkpy) écrit les données")
            print(f"   → Nouveau cycle dans 10 minutes...")
            sys.exit(1)

# Si on arrive ici, on a les données!
print(f"\n Données chargées avec succès!")
print(f"   → Schéma détecté automatiquement")
df.printSchema()

# ===================================================================
# ÉTAPE 4: AFFICHAGE DES 5 PREMIÈRES LIGNES
# ===================================================================
print("\n Aperçu des 5 premiers événements:")
df.show(5)

# ===================================================================
# ÉTAPE 5: KPI 1 - TOP 10 PRODUITS LES PLUS VUS
# ===================================================================
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
    top_viewed.show(10)
    
    # Je sauvegarde les résultats en Parquet
    top_viewed.write.mode("overwrite").parquet(f"{hdfs_output_path}/top_viewed_products")
    print(f"\n Résultats sauvegardés: {hdfs_output_path}/top_viewed_products")
    
except Exception as e:
    print(f" Erreur calcul TOP VUES: {e}")

# ===================================================================
# ÉTAPE 6: KPI 2 - TOP 10 PRODUITS LES PLUS ACHETES + CHIFFRE D'AFFAIRES
# ===================================================================
print("\n" + "=" * 70)
print(" KPI 2: TOP 10 PRODUITS LES PLUS ACHETES + CA")
print("=" * 70)

try:
    # Je filtre les ajouts au panier (achat) et je calcule le CA
    top_bought = df.filter(col("type") == "ADD_TO_CART") \
        .groupBy("produitId", "title", "price") \
        .agg(
            count("*").alias("nombre_achats"),
            (col("price") * count("*")).alias("CA_total")
        ) \
        .orderBy(col("nombre_achats").desc()) \
        .limit(10)
    
    print("\n Résultats:")
    top_bought.show(10)
    
    # Je sauvegarde
    top_bought.write.mode("overwrite").parquet(f"{hdfs_output_path}/top_bought_products")
    print(f"\n Résultats sauvegardés: {hdfs_output_path}/top_bought_products")
    
except Exception as e:
    print(f" Erreur calcul TOP ACHATS: {e}")

# ===================================================================
# ÉTAPE 7: KPI 3 - CHIFFRE D'AFFAIRES PAR JOUR
# ===================================================================
print("\n" + "=" * 70)
print(" KPI 3: CHIFFRE D'AFFAIRES PAR JOUR")
print("=" * 70)

try:
    # Je extrais la date depuis event_date et je somme les montants
    daily_revenue = df.filter(col("type") == "ADD_TO_CART") \
        .groupBy("event_date") \
        .agg(
            spark_sum("montant_potentiel").alias("CA_jour")
        ) \
        .orderBy("event_date")
    
    print("\n Résultats:")
    daily_revenue.show()
    
    # Je sauvegarde
    daily_revenue.write.mode("overwrite").parquet(f"{hdfs_output_path}/daily_revenue")
    print(f"\n Résultats sauvegardés: {hdfs_output_path}/daily_revenue")
    
except Exception as e:
    print(f" Erreur calcul CA/JOUR: {e}")

# ===================================================================
# ÉTAPE 8: KPI 4 - ALERTES RUPTURE DE STOCK
# ===================================================================
print("\n" + "=" * 70)
print(" KPI 4: ALERTES RUPTURE DE STOCK (stock = 0)")
print("=" * 70)

try:
    # Je détecte quand stock passe à 0 (rupture de stock)
    stock_alerts = df.filter(col("newStock") == 0) \
        .select(
            col("event_datetime"),
            col("produitId"),
            col("title"),
            col("newStock"),
            col("alerte_rupture")
        ) \
        .orderBy("event_datetime")
    
    print("\n Résultats:")
    stock_alerts.show()
    
    # Je sauvegarde
    stock_alerts.write.mode("overwrite").parquet(f"{hdfs_output_path}/stock_alerts")
    print(f"\n Résultats sauvegardés: {hdfs_output_path}/stock_alerts")
    
except Exception as e:
    print(f" Erreur calcul ALERTES: {e}")

# ===================================================================
# ÉTAPE 9: KPI 5 - PRODUITS PAR GAMME DE PRIX
# ===================================================================
print("\n" + "=" * 70)
print(" KPI 5: PRODUITS PAR GAMME DE PRIX")
print("=" * 70)

try:
    # Je catégorise par gamme de prix
    price_ranges = df.select(
        col("produitId"),
        col("title"),
        col("price"),
        # Je crée une colonne gamme_prix selon le tarif
        when(col("price") > 500, "TRES_CHER_500+") \
        .when(col("price") > 100, "CHER_100-500") \
        .when(col("price") > 30, "MOYEN_30-100") \
        .otherwise("ABORDABLE_<30").alias("gamme_prix")
    ) \
    .groupBy("gamme_prix") \
    .agg(count("*").alias("nombre_produits"))
    
    print("\n Résultats:")
    price_ranges.show()
    
    # Je sauvegarde
    price_ranges.write.mode("overwrite").parquet(f"{hdfs_output_path}/products_by_price")
    print(f"\n Résultats sauvegardés: {hdfs_output_path}/products_by_price")
    
except Exception as e:
    print(f" Erreur calcul GAMME PRIX: {e}")

# ===================================================================
# RÉSUMÉ FINAL
# ===================================================================
print("\n" + "=" * 70)
print(" JOB SPARK 2 TERMINÉ AVEC SUCCÈS!")
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