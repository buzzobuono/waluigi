import time
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog

class RawDataExtract(Task):
    def run(self):
        source = self.params.source.lower()
        dataset_id = f"raw_{source}"
        
        print(f"📥 Estrazione dati da sorgente: {self.params.source}...")
        
        # Usiamo produce per riservare la versione nel catalogo
        with catalog.produce(dataset_id, 
                             namespace=f"analytics/{source}/raw", 
                             format="out") as ctx:
            steps = 5 # Ridotto per brevità
            for step in range(steps):
                print(f"Estrazione Step {step+1}/{steps}")
                time.sleep(1)

            with open(ctx.path, "w") as f:
                f.write(f"Dati grezzi {self.params.source} per data {self.params.date}")
            
            ctx.rows = 1 # Esempio

if __name__ == "__main__":
    RawDataExtract().start()