import sys
import importlib
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)
BOSS_URL = "http://localhost:8082" # Assicurati che l'indirizzo sia corretto

def log(msg):
    print(f"👷 [WORKER] {msg}", flush=True)

@app.route('/execute', methods=['POST'])
def execute():
    data = request.json
    # Il payload contiene: module, class, params, attributes
    mod_name = data.get('module')
    cls_name = data.get('class')
    
    log(f"Ricevuto ordine: {mod_name}.{cls_name}")
    
    try:
        # 1. Caricamento dinamico del flusso
        mod = importlib.import_module(mod_name)
        cls = getattr(mod, cls_name)
        
        # 2. Istanziazione del Task
        task = cls(params=data.get('params'), attributes=data.get('attributes'))
        
        # 3. Agganciamo un "finto" engine per permettere a task.complete() 
        # di sapere a chi inviare l'aggiornamento
        class SimpleEngine:
            def __init__(self, url): self.server_url = url
        task.engine = SimpleEngine(BOSS_URL)
        
        # 4. ESECUZIONE
        log(f"Esecuzione run() per {task.id}...")
        task.run()
        
        # 5. NOTIFICA SUCCESS
        # Usiamo il metodo .complete() originale del tuo Task
        task.complete()
        log(f"✅ Task {task.id} completato e notificato al Boss.")
        
        return jsonify({"status": "success", "id": task.id}), 200

    except Exception as e:
        log(f"❌ ERRORE durante l'esecuzione: {e}")
        # Notifica il fallimento al Boss
        try:
            requests.post(f"{BOSS_URL}/update", json={
                "id": data.get('id'),
                "status": "FAILED",
                "params": data.get('params') # Semplificato per brevità
            })
        except: pass
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Prendi la porta dagli argomenti (es: python worker.py 5001)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    
    log(f"Avvio Worker sulla porta {port}...")
    
    # Registrazione automatica al Boss
    try:
        requests.post(f"{BOSS_URL}/worker/register", json={"url": f"http://localhost:{port}"})
        log("Registrato con successo al Boss.")
    except:
        log("Attenzione: Impossibile registrarsi al Boss. Assicurati che sia acceso.")

    app.run(port=port, host='0.0.0.0', debug=False, threaded=True)
