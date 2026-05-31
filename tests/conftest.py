import subprocess
import time
import pytest
import requests
import os
import tempfile
import shutil

from waluigi.sdk.catalog import CatalogClient

CATALOG_PORT   = 19000
BOSS_PORT   = 18082
WORKER_PORT = 15001
CATALOG_URL    = f"http://localhost:{CATALOG_PORT}"
BOSS_URL    = f"http://localhost:{BOSS_PORT}"
WORKER_URL  = f"http://localhost:{WORKER_PORT}"

@pytest.fixture(scope="session")
def catalog_url():
    return CATALOG_URL

@pytest.fixture(scope="session")
def boss_url():
    return BOSS_URL

@pytest.fixture(scope="session")
def worker_url():
    return WORKER_URL

TEST_NAMESPACE = "test"

@pytest.fixture(scope="module")
def catalog(catalog_url):
    print(catalog_url)
    return CatalogClient(url=catalog_url, namespace=TEST_NAMESPACE)

def _wait_ready(url, proc, name, timeout=10):
    deadline = time.time() + timeout
    while True:
        try:
            requests.get(url, timeout=1)
            return
        except requests.ConnectionError:
            if time.time() > deadline:
                proc.terminate()
                raise RuntimeError(f"{name} did not start within {timeout} s")
            time.sleep(0.3)

@pytest.fixture(scope="session", autouse=True)
def start_catalog_server():
    test_dir = tempfile.mkdtemp()
    test_db = os.path.join(test_dir, "test_catalog.db")
    test_data = os.path.join(test_dir, "test_data")
    os.makedirs(test_data, exist_ok=True)

    env = os.environ.copy()
    env["WALUIGI_CATALOG_PORT"]    = str(CATALOG_PORT)
    env["WALUIGI_CATALOG_DB_URL"]   = f"sqlite:///{test_db}"
    env["WALUIGI_CATALOG_DATA_PATH"] = test_data

    proc = subprocess.Popen(
        ["wlcatalog"], 
        stdout=subprocess.DEVNULL, 
        stderr=subprocess.DEVNULL,
        env=env
    )
    
    _wait_ready(f"{CATALOG_URL}/openapi.json", proc, "Catalog")
    
    yield proc
    
    proc.terminate()
    proc.wait()
    
    # Pulizia: rimuove il database e i file prodotti durante il test
    shutil.rmtree(test_dir)

@pytest.fixture(scope="session", autouse=True)
def boss_server():
    test_dir = tempfile.mkdtemp(prefix="wl_test_boss_")
    db_path  = os.path.join(test_dir, "test_boss.db")

    env = os.environ.copy()
    env["WALUIGI_BOSS_PORT"]   = str(BOSS_PORT)
    env["WALUIGI_BOSS_DB_URL"] = f"sqlite:///{db_path}"

    proc = subprocess.Popen(
        ["wlboss"], env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_ready(f"{BOSS_URL}/openapi.json", proc, "Boss")
    yield proc
    proc.terminate()
    proc.wait()
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def worker_server(boss_server):
    env = os.environ.copy()
    env["WALUIGI_WORKER_PORT"]      = str(WORKER_PORT)
    env["WALUIGI_WORKER_BOSS_URL"]  = BOSS_URL
    env["WALUIGI_WORKER_SLOTS"]     = "2"
    env["WALUIGI_WORKER_HEARTBEAT"] = "3600"

    proc = subprocess.Popen(
        ["wlworker"], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    _wait_ready(f"{WORKER_URL}/openapi.json", proc, "Worker")
    yield proc
    proc.terminate()
    proc.wait()
