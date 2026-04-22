import subprocess
import time
import pytest
import requests

@pytest.fixture(scope="session", autouse=True)
def start_catalog_server():
    # 1. Comando per avviare il tuo server (es. usando l'entry point wlcatalog o uvicorn)
    # Assicurati di usare una porta diversa da quella di produzione se necessario
    proc = subprocess.Popen(
        ["wlcatalog"], 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL
    )
    
    # 2. "Health check": Aspetta che il server sia pronto
    timeout = 5
    start_time = time.time()
    while True:
        try:
            # Tenta di chiamare l'health check o la root
            requests.get("http://localhost:9000/")
            break
        except requests.ConnectionError:
            if time.time() - start_time > timeout:
                proc.terminate()
                raise RuntimeError("Il server del catalogo non si è avviato in tempo")
            time.sleep(0.5)

    yield  # Qui vengono eseguiti i test
    
    # 3. Shutdown: Spegne il server dopo tutti i test
    proc.terminate()
    proc.wait()
