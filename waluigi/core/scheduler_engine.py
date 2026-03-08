from waluigi.core.db import WaluigiDB
import requests
import os
    
class WaluigiSchedulerEngine:
    
    workers = []
    
    def __init__(self, db, server_url="http://localhost:8082"):
        self.db = db
        self.server_url = server_url

    def _register(self, parent_id, task):
        id = task.id
        params_hash = task.hash(task.params)
        attributes_hash = task.hash(task.attributes)
        status = self.db.get_task_status(id, params_hash)
        print(status)
        if status == "SUCCESS":
            return "already_done"
        if status == "RUNNING":
            return "locked"
        self.db.register_task(id, task.namespace, parent_id, params_hash, attributes_hash)
        return "ok"
            
    def _update_boss(self, parent_id, task, status):
        print("*")
        id = task.id
        if status == "RUNNING":
            # Tentativo di acquisizione lock atomico
            if not self.db.try_to_lock(id):
                return "locked"
        # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
        self.db.update_task(id, task.namespace, parent_id, task.hash(task.params), task.hash(task.attributes), status)
        return "updated"
    
    def _is_complete(self, task):
        id = task.id
        params_hash = task.hash(task.params)
        status = self.db.get_task_status(id, params_hash)
        return status == "SUCCESS"
      
    def _dispatch(self, task, module_name):
        if not self.workers:
            print("⚠️ [Boss] Nessun worker disponibile")
            return False
            
        payload = {
            "module": module_name,
            "class": task.__class__.__name__,
            "id": task.id,
            "tags": task.tags, 
            "params": vars(task.params),
            "params_hash": task.hash(task.params),
            "attributes": vars(task.attributes)
        }
        
        for w_url in self.workers:
            try:
                # Timeout generoso per permettere al worker di caricare i moduli
                r = requests.post(f"{w_url}/execute", json=payload, timeout=10)
                if r.status_code == 202:
                    print(f"🚀 [Boss] Inviato a {w_url}: {task.id}")
                    return True
                elif r.status_code == 429:
                    print(f"⏳ [Boss] Workers {w_url} occupato per {task.id}")                
            except Exception as e:
                print(f"❌ [Boss] Worker {w_url} non ha risposto correttamente")
                continue
        
        return False
    
    def registerWorker(self, url):
        if url and url not in self.workers:
            self.workers.append(url)
        print(f"👷 [Boss] Nuovo worker registrato: {url}")
        
    def build__(self, task, module_name, parent_id=None):
        task.engine = self
        
        # 1. Chiedi lo stato attuale
        r = self._register(parent_id, task)
        
        # Se sta già girando altrove, questo ramo muore qui.
        if r == "locked":
            print(f"⚠️ [Waluigi] {task.id} locked")
            return False
        
        # 2. Check di completamento reale (Idempotenza)
        if self._is_complete(task):
            # Se a DB non era SUCCESS, allinealo per la dashboard
            print(f"✅ [Boss] {task.id} is complete")
            if r != "already_done":
                self._update_boss(parent_id, task, "SUCCESS")
            return True
        
        print(f"📌 [Boss] {task.id} is not complete")
        
        # 3. Se non è completo, risolvi le dipendenze.
        # Se anche una sola dipendenza non è True (è in corso o fallita), il padre si ferma.
        all_deps_ready = True
        for dep in task.requires():
            if not self.build(dep, module_name, parent_id=task.id):
                # Se il figlio non è ancora SUCCESS, il padre resta in PENDING
                self._update_boss(parent_id, task, "PENDING")
                return False # BLOCCANTE
                
        if all_deps_ready:
            # 4. Check finale prima di lanciare
            status = self.db.get_task_status(task.id, task.hash(task.params))
            if status in ["RUNNING", "READY"]:
                return False # Già in carico al worker, aspetta.

            try:
                # 5. ESECUZIONE
                r_lock = self._update_boss(parent_id, task, "READY")
                if r_lock == "locked": return False

                print(f"🚀 [Boss] {task.id} submitted")
                success = self._dispatch(task, module_name)
                
                if not success:
                    print(f"❌ [Boss] {task.id} cannot be submitted")
                    self._update_boss(parent_id, task, "UNSUMBITTED")
            except Exception as e:
                print(f"❌ [Boss] {task.id} error: {e}")
                self._update_boss(parent_id, task, "UNSUMBITTED")
                
            # IMPORTANTE: Restituiamo False. 
            # Il padre saprà che il figlio è "partito" ma non è "finito".
            return False
            
    def build(self, task, module_name, parent_id=None):
        task.engine = self
        
        # Recupero dello stato attuale dal DB
        status = self.db.get_task_status(task.id, task.hash(task.params))
        # SE IL TASK È FALLITO: Blocchiamo questo ramo.
        # Ritorna False per fermare il padre, ma non aggiorniamo il padre a FAILED.
        if status == "FAILED":
            print(f"🛑 [Boss] {task.id} is FAILED. Blocking parent execution.")
            return None
        
        # Chiedi lo stato attuale
        r = self._register(parent_id, task)
        # Se sta già girando altrove, questo ramo muore qui.
        if r == "locked":
            print(f"⚠️ [Boss] {task.id} locked")
            return False
        # Check di completamento (Idempotenza)
        if status == "SUCCESS":
            print(f"✅ [Boss] {task.id} is complete")
            if r != "already_done":
                self._update_boss(parent_id, task, "SUCCESS")
            return True
        
        print(f"📌 [Boss] {task.id} is not complete")
            
        all_deps_ready = True
        
        for dep in task.requires():
            res = self.build(dep, module_name, parent_id=task.id)
            if res is None: 
                return None # Propaga lo stop al padre senza fare update
            if res is False:
                all_deps_ready = False
        
        # Se anche un solo figlio non è SUCCESS (perché è READY, RUNNING o appena lanciato)
        # il padre deve fermarsi qui.
        if not all_deps_ready:
            self._update_boss(parent_id, task, "PENDING")
            return False
                
        # Esecuzione (Tutte le deps sono SUCCESS)
        status = self.db.get_task_status(task.id, task.hash(task.params))
        if status in ["RUNNING", "READY"]:
            return False
        try:
            r_lock = self._update_boss(parent_id, task, "READY")
            if r_lock == "locked":
                return False

            print(f"🚀 [Boss] {task.id} submitted")
            success = self._dispatch(task, module_name)
            
            if not success:
                print(f"❌ [Boss] {task.id} cannot be submitted")
                self._update_boss(parent_id, task, "PENDING")
        except Exception as e:
            print(f"❌ [Boss] {task.id} error: {e}")
            self._update_boss(parent_id, task, "PENDING")
            
        return False
       
       