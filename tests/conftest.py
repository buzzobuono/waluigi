import subprocess
import time
import pytest
import requests
import os
import tempfile
import shutil

@pytest.fixture(scope="session", autouse=True)
def start_catalog_server():
    # Crea una cartella temporanea per isolare DB e dati dei test
    test_dir = tempfile.mkdtemp()
    test_db = os.path.join(test_dir, "test_catalog.db")
    test_data = os.path.join(test_dir, "test_data")
    os.makedirs(test_data, exist_ok=True)

    # Copia l'ambiente corrente e aggiungi le variabili per il test
    test_env = os.environ.copy()
    test_env["WALUIGI_CATALOG_DB_PATH"] = test_db
    test_env["WALUIGI_CATALOG_DATA_PATH"] = test_data

    proc = subprocess.Popen(
        ["wlcatalog"], 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        env=test_env
    )
    
    timeout = 5
    start_time = time.time()
    while True:
        try:
            requests.get("http://localhost:9000/")
            break
        except requests.ConnectionError:
            if time.time() - start_time > timeout:
                proc.terminate()
                raise RuntimeError("Il server del catalogo non si è avviato in tempo")
            time.sleep(0.5)

    yield 
    
    proc.terminate()
    proc.wait()
    
    # Pulizia: rimuove il database e i file prodotti durante il test
    shutil.rmtree(test_dir)
