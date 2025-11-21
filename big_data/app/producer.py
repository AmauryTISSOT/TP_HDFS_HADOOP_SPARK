import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

animaux = [
    ("A101", "chien"),
    ("A102", "chat"),
    ("A103", "lapin"),
    ("A104", "chien"),
    ("A105", "tortue"),
    ("A106", "perroquet"),
    ("A107", "hamster"),
]

alertes = ["RAS", "fièvre", "vomissements", "léthargie", "toux", "boiterie"]
veterinaires = ["Dr. Martin", "Dr. Lopez", "Dr. Silva", "Dr. Perrin"]


def generer_event():
    animal_id, espece = random.choice(animaux)
    return {
        "animal_id": animal_id,
        "espece": espece,
        "poids": round(random.uniform(0.2, 60.0), 2),
        "temperature": round(random.uniform(36.0, 41.5), 2),
        "alerte": random.choice(alertes),
        "vet": random.choice(veterinaires),
        "timestamp": datetime.now().isoformat(),
    }


print("🐾 Producteur PetCare+ → Kafka(topic=animal) lancé...")

while True:
    event = generer_event()
    producer.send("animal", event)
    producer.flush()
    print("Event envoyé :", event)
    time.sleep(1)
