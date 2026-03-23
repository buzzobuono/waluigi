import requests

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)

class WaluigiSchedulerEngine:
    
    def __init__(self, db):
        self.db = db
        
    def _register(self, parent_id, task, job_id):
        id = task.id
        params_hash = task.hash(task.params)
        attributes_hash = task.hash(task.attributes)
        status = self.db.get_task_status(id, params_hash)
        print(status)
        if status == "SUCCESS":
            return "already_done"
        if status == "RUNNING":
            return "locked"
        self.db.register_task(id, task.namespace, parent_id, params_hash, attributes_hash, job_id=job_id)
        return "ok"
            
    def _update_task(self, task, status):
        id = task.id
        if status == "RUNNING":
            # Tentativo di acquisizione lock atomico
            if not self.db.try_to_lock(id):
                return "locked"
        # Se non è RUNNING (è SUCCESS/FAILED/PENDING), aggiorna normalmente
        self.db.update_task(id, task.namespace, task.hash(task.params), task.hash(task.attributes), status)
        return "updated"
    
    def _is_complete(self, task):
        id = task.id
        params_hash = task.hash(task.params)
        status = self.db.get_task_status(id, params_hash)
        return status == "SUCCESS"
        
    def _allocate(self, task):
        task_resources = getattr(task, 'resources', {'coin': 1.0})
        return self.db.acquire_resources(task_resources)
        
    def _deallocate(self, task):
        task_resources = getattr(task, 'resources', {'coin': 1.0})
        self.db.release_resources(task_resources)
            
    def _dispatch(self, job_metadata, task):
        payload = {
            "workdir": job_metadata['workdir'],
            "sourcedir": job_metadata['sourcedir'],
            "command": task.command,
            "id": task.id,
            "params": vars(task.params),
            "params_hash": task.hash(task.params),
            "attributes": vars(task.attributes),
            "resources": task.resources,
            "namespace": task.namespace
        }
        
        workers = self.db.get_available_workers()
        for worker in workers:
            try:
                # Timeout generoso per permettere al worker di caricare i moduli
                r = requests.post(f"{worker['url']}/execute", json=payload, timeout=10)
                if r.status_code == 202:
                    log(f"🚀 Inviato a {worker['url']}: {task.id}")
                    return True
                elif r.status_code == 429:
                    log(f"⏳ Workers {worker['url']} occupato per {task.id}")
                else:
                    log(f"⏳ Workers {worker['url']} errore per {task.id}")
            except Exception as e:
                log(f"❌ Worker {worker['url']} non ha risposto correttamente. Lo rimuovo dai worker registrati.")
                self.db.delete_worker(worker['url'])
                continue
                
        return False
        
    def registerWorker(self, worker):
        log(f"👷 Contattato dal worker: {worker['url']}")
        self.db.register_worker(worker['url'], worker['max_slots'], worker['free_slots'])
        
    def build(self, job_metadata, task, parent_id):
        task.engine = self
        
        # Recupero dello stato attuale dal DB
        status = self.db.get_task_status(task.id, task.hash(task.params))
        # SE IL TASK È FALLITO: Blocchiamo questo ramo.
        # Ritorna False per fermare il padre, ma non aggiorniamo il padre a FAILED.
        if status == "FAILED":
            log(f"🛑 {task.id} is FAILED. Blocking parent execution.")
            return None
        
        # Chiedi lo stato attuale
        r = self._register(parent_id, task, job_metadata['job_id'])
        # Se sta già girando altrove, questo ramo muore qui.
        if r == "locked":
            log(f"⚠️ {task.id} locked")
            return False
            
        # Check di completamento (Idempotenza)
        if status == "SUCCESS":
            log(f"✅ {task.id} is complete")
            if r != "already_done":
                self._update_task(task, "SUCCESS")
            return True
        
        log(f"📌 {task.id} is not complete")
            
        all_deps_ready = True
        for dep in task.requires():
            res = self.build(job_metadata=job_metadata,
                task=dep,
                parent_id=task.id
                )
            if res is None: 
                return None # Propaga lo stop al padre senza fare update
            if res is False:
                all_deps_ready = False
        
        # Se anche un solo figlio non è SUCCESS (perché è READY, RUNNING o appena lanciato)
        # il padre deve fermarsi qui.
        if not all_deps_ready:
            self._update_task(task, "PENDING")
            return False
                
        # Esecuzione (Tutte le deps sono SUCCESS)
        status = self.db.get_task_status(task.id, task.hash(task.params))
        if status in ["RUNNING", "READY"]:
            return False
        try:
            if not self._allocate(task):
                log(f"⏳ {task.id} in attesa di risorse (limite raggiunto)")
                return False
                
            r_lock = self._update_task(task, "READY")
            if r_lock == "locked":
                self._deallocate(task)
                return False
            
            log(f"🚀 {task.id} submitted")
            success = self._dispatch(job_metadata, task)
            if not success:
                log(f"❌ {task.id} cannot be submitted")
                self._deallocate(task)    
                self._update_task(task, "PENDING")
        
        except Exception as e:
            self._deallocate(task)
            log(f"❌ {task.id} error: {e}")
            self._update_task(task, "PENDING")
            
        return False
        