import requests

def log(msg):
    print(f"[Boss 🐢] {msg}", flush=True)

class WaluigiEngine:
    
    def __init__(self, db):
        self.db = db
        
    def registerJob(self, job_id, task, parent_id):
        params_hash = task.hash(task.params)
        attributes_hash = task.hash(task.attributes)
        self.db.register_task(task.id, task.namespace, parent_id, params_hash, attributes_hash, job_id=job_id)
        for dep in task.requires():
            self.registerJob(job_id, dep, task.id)
            
    def _register(self, parent_id, task, job_id):
        id = task.id
        params_hash = task.hash(task.params)
        attributes_hash = task.hash(task.attributes)
        status = self.db.get_task_status(id, params_hash)
        if status == "RUNNING":
            return status
        self.db.register_task(id, task.namespace, parent_id, params_hash, attributes_hash, job_id=job_id)
        if status == "SUCCESS":
            return status
        return status
            
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
            "type":    task.type,
            "command": task.command,
            "script":  task.script,
            "id": task.id,
            "job_id": job_metadata['name'],
            "params": vars(task.params),
            "params_hash": task.hash(task.params),
            "attributes": vars(task.attributes),
            "config": task.config,
            "resources": task.resources,
            "namespace": task.namespace
        }
        
        workers = self.db.get_available_workers()
        if not workers:
            return "WORKERS_SATURATED"
        all_busy = True
        for worker in workers:
            log(self.db.get_worker_slots(worker['url']))
            if not self.db.acquire_worker_slot(worker['url']):
                log(f"💥 Errore fatale (400) dal worker {worker['url']} per {task.id}")
                continue
            log(f"⏳ Slot acquired by worker {worker['url']} for {task.id}")
            log(self.db.get_worker_slots(worker['url']))  
            try:
                r = requests.post(f"{worker['url']}/execute", json=payload, timeout=10)
                if r.status_code == 202:
                    log(f"🚀 Inviato a {worker['url']}: {task.id}")
                    return "SUCCESS"
                    
                self.db.release_worker_slot(worker['url'])
                
                if r.status_code == 400:
                    log(f"💥 Errore fatale (400) dal worker {worker['url']} per {task.id}")
                    return "FATAL_ERROR"
                elif r.status_code == 429:
                    log(f"⏳ Workers {worker['url']} occupato per {task.id}")
                else:
                    log(f"⏳ Workers {worker['url']} errore {r.status_code} per {task.id}")
                    all_busy = False
            except Exception as e:
                log(f"❌ Worker {worker['url']} non ha risposto. Rimozione in corso.")
                self.db.delete_worker(worker['url'])
                all_busy = False
                continue
                
        if all_busy:
            return "WORKERS_SATURATED"
        return "RETRY"
        
    def registerWorker(self, worker):
        log(f"👷 Contattato dal worker: {worker['url']}")
        self.db.register_worker(worker['url'], worker['max_slots'], worker['free_slots'])
        
    def build(self, job_metadata, task, parent_id):
        status = self.db.get_task_status(task.id, task.hash(task.params))
        if status == "FAILED":
            log(f"🛑 {task.id} is fail. Fail job processing.")
            return None
        elif status == "RUNNING":
            log(f"⚠️ {task.id} is running")
            return False
        elif status == "SUCCESS":
            log(f"✅ {task.id} is complete")
            return True
        
        log(f"📌 {task.id} is not complete")
            
        all_deps_ready = True
        for dep in task.requires():
            res = self.build(job_metadata=job_metadata,
                task=dep,
                parent_id=task.id
                )
            if res == "PAUSE":
                return "PAUSE"
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
            dispatch_status = self._dispatch(job_metadata, task)
            
            if dispatch_status == "WORKERS_SATURATED":
                self._deallocate(task)
                self._update_task(task, "PENDING")
                return "PAUSE"
                
            if dispatch_status == "FATAL_ERROR":
                log(f"💀 {task.id} fallimento definitivo durante il dispatch")
                self._deallocate(task)
                self._update_task(task, "FAILED")
                return None # Ferma la propagazione al padre
                
            if dispatch_status == "RETRY":
                log(f"❌ {task.id} cannot be submitted (no workers available)")
                self._deallocate(task)    
                self._update_task(task, "PENDING")
                return False
        
        except Exception as e:
            self._deallocate(task)
            log(f"❌ {task.id} error: {e}")
            self._update_task(task, "PENDING")
            
        return False
