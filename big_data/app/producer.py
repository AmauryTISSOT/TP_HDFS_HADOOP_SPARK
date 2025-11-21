import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = "ecommerce"

event_types = ["ajout_au_panier", "commande_payee", "maj_stock"]
products = ["p001", "p002", "p003"]
users = ["u001", "u002", "u003"]
warehouses = ["w1", "w2"]


def generate_event():
    event_type = random.choice(event_types)
    base = {
        "event_type": event_type,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    if event_type == "ajout_au_panier":
        base.update({
            "user_id": random.choice(users),
            "product_id": random.choice(products),
            "quantity": random.randint(1, 5)
        })
    elif event_type == "commande_payee":
        base.update({
            "user_id": random.choice(users),
            "order_id": f"o{random.randint(1000, 9999)}",
            "total_amount": round(random.uniform(10, 200), 2),
            "payment_method": random.choice(["CB", "PayPal", "crypto"])
        })
    elif event_type == "maj_stock":
        base.update({
            "product_id": random.choice(products),
            "stock_level": random.randint(0, 100),
            "warehouse_id": random.choice(warehouses)
        })

    return base



print("[producer] Envoi d’événements e-commerce (Ctrl+C pour arrêter)")
try:
    while True:
        event = generate_event()
        producer.send(topic, value=event)
        print("[producer] envoyé :", event)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[producer] arrêt")
finally:
    producer.flush()
    producer.close()

