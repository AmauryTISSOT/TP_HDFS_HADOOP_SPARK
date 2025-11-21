import pyhdfs
import time
import requests

HDFS_HOSTS = "namenode:9870"
HDFS_USER = "root"

BASE_DIR = "/user/kafka"
TEST_FILE = f"{BASE_DIR}/test.txt"
LOCAL_FLUX = "/tmp/flux.txt"
FLUX_COPY = f"{BASE_DIR}/flux_copy.txt"
FLUX_HDFS = f"{BASE_DIR}/flux.txt"


def wait_for_hdfs_ready(fs, timeout_sec=120):
    """
    Attend que le NameNode soit accessible ET sorti du safe mode.
    """
    start = time.time()
    last_exc = None

    while True:
        elapsed = time.time() - start
        if elapsed > timeout_sec:
            print("[pyhdfs-demo] HDFS toujours pas prêt après timeout ❌")
            if last_exc:
                raise last_exc
            raise TimeoutError("HDFS pas prêt")

        try:
            # Vérifier que le NN répond
            home = fs.get_home_directory()
            print(f"[pyhdfs-demo] HDFS joignable, home dir = {home}")

            # Tester une petite opération d'écriture pour détecter le safe mode
            test_dir = "/tmp/_pyhdfs_check"
            try:
                fs.mkdirs(test_dir)
            except pyhdfs.HdfsHttpException as e:
                msg = str(e)
                if "SafeModeException" in msg:
                    print("[pyhdfs-demo] Namenode en Safe Mode, on attend un peu...")
                    last_exc = e
                    time.sleep(3)
                    continue
                else:
                    # autre erreur HDFS
                    raise

            print("[pyhdfs-demo] HDFS prêt (NN OK + plus de Safe Mode) ✅")
            return

        except pyhdfs.HdfsNoServerException as e:
            print("[pyhdfs-demo] Namenode pas joignable, on attend un peu...")
            last_exc = e
            time.sleep(3)

        except requests.exceptions.ConnectionError as e:
            print("[pyhdfs-demo] Erreur de connexion WebHDFS, retry...")
            last_exc = e
            time.sleep(3)


def main():
    print("[pyhdfs-demo] Connexion au cluster HDFS...")
    fs = pyhdfs.HdfsClient(hosts=HDFS_HOSTS, user_name=HDFS_USER)

    # On attend que le namenode soit prêt (et plus en safe mode)
    wait_for_hdfs_ready(fs)

    # === Existence / listing ===
    print("[pyhdfs-demo] Vérification de l'existence du répertoire", BASE_DIR)
    exists = fs.exists(BASE_DIR)
    print(f"[pyhdfs-demo] {BASE_DIR} existe ? {exists}")

    print("[pyhdfs-demo] Contenu de /user :")
    try:
        print(fs.listdir("/user"))
    except pyhdfs.HdfsFileNotFoundException:
        print("[pyhdfs-demo] /user n'existe pas encore, on le créera au besoin.")

    # === Répertoires ===
    print(f"[pyhdfs-demo] Création du répertoire {BASE_DIR} (si besoin)")
    fs.mkdirs(BASE_DIR)

    print(f"[pyhdfs-demo] Statut de {BASE_DIR} :")
    print(fs.list_status(BASE_DIR))

    # === Fichiers : création ===
    print(f"[pyhdfs-demo] Création du fichier {TEST_FILE}")
    fs.create(TEST_FILE, b"hello\n")

    # === Lecture ===
    print(f"[pyhdfs-demo] Lecture du fichier {TEST_FILE}")
    with fs.open(TEST_FILE) as f:
        contenu = f.read().decode("utf-8")
        print("CONTENU:\n", contenu)

    # === Append ===
    print(f"[pyhdfs-demo] Append sur {TEST_FILE}")
    fs.append(TEST_FILE, b"again\n")

    print(f"[pyhdfs-demo] Re-lecture de {TEST_FILE}")
    with fs.open(TEST_FILE) as f:
        contenu = f.read().decode("utf-8")
        print("CONTENU:\n", contenu)

    # === Renommer / supprimer ===
    old_file = f"{BASE_DIR}/test_old.txt"
    print(f"[pyhdfs-demo] Renommage {TEST_FILE} -> {old_file}")
    fs.rename(TEST_FILE, old_file)

    print(f"[pyhdfs-demo] Suppression de {old_file}")
    fs.delete(old_file, recursive=False)

    # === Walk récursif ===
    print("[pyhdfs-demo] Parcours récursif de /user :")
    for root, dirs, files in fs.walk("/user"):
        print("root :", root)
        print("dirs :", dirs)
        print("files:", files)
        print("-----")

    # === Copies local <-> HDFS ===
    print(f"[pyhdfs-demo] Test des copies local <-> HDFS")

    # On s'assure d'abord que FLUX_HDFS existe (sinon copy_to_local va râler)
    if not fs.exists(FLUX_HDFS):
        print(f"[pyhdfs-demo] {FLUX_HDFS} n'existe pas, création d'un petit fichier pour le test.")
        fs.create(FLUX_HDFS, b"ligne 1\n")

    # HDFS -> local
    print(f"[pyhdfs-demo] copy_to_local : {FLUX_HDFS} -> {LOCAL_FLUX}")
    fs.copy_to_local(FLUX_HDFS, LOCAL_FLUX)

    # local -> HDFS
    print(f"[pyhdfs-demo] copy_from_local : {LOCAL_FLUX} -> {FLUX_COPY}")
    fs.copy_from_local(LOCAL_FLUX, FLUX_COPY)

    print("[pyhdfs-demo] Attente 40 secondes avant fin...")
    time.sleep(40)
    print("fin du programme")


if __name__ == "__main__":
    main()