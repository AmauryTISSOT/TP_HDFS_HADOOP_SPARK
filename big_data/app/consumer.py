from kafka import KafkaConsumer
import pyhdfs
import os
import time

# =========================
# CONFIG EN DUR
# =========================
BOOTSTRAP_SERVERS = "kafka:9092"  # dans Docker
TOPIC = "ecommerce"

HDFS_HOSTS = "namenode:9870"  # WebHDFS du namenode
HDFS_USER = "root"
HDFS_OUTPUT_PATH = "/user/kafka/flux.txt"


# =========================
# FONCTIONS HDFS
# =========================
def wait_for_hdfs(fs, timeout_sec=60):
    """Attend que le NameNode réponde avant de continuer."""
    start = time.time()
    while True:
        try:
            fs.get_home_directory()
            print("[consumer] HDFS est joignable ✅")
            return
        except Exception as e:
            if time.time() - start > timeout_sec:
                print("[consumer] HDFS toujours pas joignable après timeout ❌")
                raise
            print("[consumer] HDFS pas encore prêt, on attend un peu...", e)
            time.sleep(3)


def safe_append(fs, path, data: bytes, retries=20, base_sleep=0.5):
    """
    Append avec gestion des cas léchés d'HDFS :
    - AlreadyBeingCreatedException  -> fichier encore ouvert par un autre client
    - RecoveryInProgressException   -> HDFS récupère un lease, il faut juste attendre
    """
    last_exc = None
    for i in range(retries):
        try:
            fs.append(path, data)
            return
        except pyhdfs.HdfsHttpException as e:
            msg = str(e)
            if (
                "AlreadyBeingCreatedException" in msg
                or "RecoveryInProgressException" in msg
            ):
                delay = base_sleep * (i + 1)
                print(
                    f"[consumer] HDFS occupé (lease / recovery), retry {i+1}/{retries} dans {delay:.1f}s…"
                )
                last_exc = e
                time.sleep(delay)
                continue
            else:
                print("[consumer] Erreur HDFS non gérée :", e)
                raise
    print("[consumer] Impossible de faire l'append après plusieurs tentatives ❌")
    if last_exc:
        raise last_exc


# =========================
# CLIENT HDFS
# =========================
fs = pyhdfs.HdfsClient(hosts=HDFS_HOSTS, user_name=HDFS_USER)

# On attend que le namenode soit vraiment prêt
wait_for_hdfs(fs)

# On s’assure que le répertoire existe
hdfs_dir = os.path.dirname(HDFS_OUTPUT_PATH)
if hdfs_dir and not fs.exists(hdfs_dir):
    print(f"[consumer] Création du répertoire HDFS {hdfs_dir}")
    fs.mkdirs(hdfs_dir)

# On crée le fichier s’il n’existe pas
if not fs.exists(HDFS_OUTPUT_PATH):
    print(f"[consumer] Création du fichier HDFS {HDFS_OUTPUT_PATH}")
    fs.create(HDFS_OUTPUT_PATH, b"")

# =========================
# CONSUMER KAFKA
# =========================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id="mon-consumer-group",
    value_deserializer=lambda v: v.decode("utf-8"),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print(
    f"[consumer] En écoute sur {TOPIC} (Kafka={BOOTSTRAP_SERVERS}) "
    f"-> HDFS {HDFS_HOSTS}{HDFS_OUTPUT_PATH} (Ctrl+C pour quitter)"
)

try:
    for msg in consumer:
        valeur = msg.value  # déjà en str
        print(
            f"[consumer] topic={msg.topic} "
            f"partition={msg.partition} "
            f"offset={msg.offset} "
            f"value={valeur}"
        )

        # Append dans le fichier HDFS
        data = (valeur + "\n").encode("utf-8")
        safe_append(fs, HDFS_OUTPUT_PATH, data)

except KeyboardInterrupt:
    print("\n[consumer] stop")
finally:
    consumer.close()
