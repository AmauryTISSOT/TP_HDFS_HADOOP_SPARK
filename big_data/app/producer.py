# ========================================
# JE CRÉE LE PRODUCTEUR SHOPNOW+ E-COMMERCE
# UTILISE DES PRODUITS FICTIFS RÉALISTES
# ENVOIE LES ÉVÉNEMENTS À KAFKA
# ========================================

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Je crée une liste de produits fictifs réalistes pour ShopNow+
produits = [
    {"_id": "PROD001", "title": "Laptop Dell XPS 15", "price": 1299, "stock": 5},
    {"_id": "PROD002", "title": "iPhone 15 Pro", "price": 999, "stock": 15},
    {"_id": "PROD003", "title": "AirPods Pro", "price": 249, "stock": 30},
    {"_id": "PROD004", "title": "Samsung Galaxy S24", "price": 899, "stock": 12},
    {"_id": "PROD005", "title": "iPad Air 6", "price": 599, "stock": 8},
    {"_id": "PROD006", "title": "Sony WH-1000XM5", "price": 379, "stock": 20},
    {"_id": "PROD007", "title": "Mechanical Keyboard RGB", "price": 149, "stock": 25},
    {"_id": "PROD008", "title": "Gaming Mouse Logitech", "price": 69, "stock": 40},
    {"_id": "PROD009", "title": "USB-C Hub 7-in-1", "price": 49, "stock": 50},
    {"_id": "PROD010", "title": "Phone Stand Metallic", "price": 29, "stock": 60},
]

# Je crée un dictionnaire pour tracker le stock en temps réel
stock_par_produit = {}

def initialiser_produits():
    """Je initialise les produits fictifs et leur stock."""
    global stock_par_produit
    for p in produits:
        stock_par_produit[p["_id"]] = p.get("stock", 10)
    
    print(f" {len(produits)} produits ShopNow+ chargés avec succès !")
    return True

# Je crée une fonction pour générer un événement VIEW_PRODUCT
def generer_event_view():
    """Je génère un événement quand un client consulte un produit."""
    produit = random.choice(produits)
    return {
        "type": "VIEW_PRODUCT",
        "produitId": str(produit["_id"]),
        "title": produit["title"],
        "price": produit["price"],
        "stock": stock_par_produit.get(str(produit["_id"]), 0),
        "timestamp": int(datetime.now().timestamp() * 1000)
    }

# Je crée une fonction pour générer un événement ADD_TO_CART
def generer_event_add_to_cart():
    """Je génère un événement quand un client ajoute un produit au panier."""
    produit = random.choice(produits)
    produit_id = str(produit["_id"])
    stock_actuel = stock_par_produit.get(produit_id, 0)
    
    # Je décrémente le stock si possible
    if stock_actuel > 0:
        stock_par_produit[produit_id] -= 1
        nouveau_stock = stock_par_produit[produit_id]
    else:
        nouveau_stock = 0
    
    return {
        "type": "ADD_TO_CART",
        "produitId": produit_id,
        "title": produit["title"],
        "price": produit["price"],
        "newStock": nouveau_stock,
        "timestamp": int(datetime.now().timestamp() * 1000)
    }

# ====================================================================
# PROGRAMME PRINCIPAL
# ====================================================================

print(" Producteur ShopNow+ → Kafka (topic=ecommerce) lancé...")
print("=" * 70)

# Étape 1 : Je charge les produits fictifs
if not initialiser_produits():
    exit(1)

# Étape 2 : Je me connecte à Kafka
print("\n Connexion à Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(" Kafka connecté avec succès !\n")
except Exception as e:
    print(f" Erreur Kafka : {e}")
    exit(1)

# Étape 3 : Je génère des événements en continu
counter = 0
try:
    while True:
        # 70% VIEW_PRODUCT, 30% ADD_TO_CART
        if random.random() < 0.7:
            event = generer_event_view()
        else:
            event = generer_event_add_to_cart()
        
        # J'envoie l'événement à Kafka
        producer.send("ecommerce", event)
        producer.flush()
        
        counter += 1
        print(f"[{counter}] {event['type']} - {event['title']} (Price: {event['price']}€)")
        
        # J'attends 0.5 secondes avant le prochain événement
        time.sleep(0.5)

except KeyboardInterrupt:
    print("\n\n  Producteur arrêté par l'utilisateur")
    producer.close()
except Exception as e:
    print(f"\n\n Erreur : {e}")
    producer.close()