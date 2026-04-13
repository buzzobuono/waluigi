import time
import random
from waluigi.sdk.task import Task
from waluigi.sdk.catalog_v2 import catalog

class CleanDataTask(Task):
    def run(self):
        source = self.params.source.lower()
        input_id = f"raw_{source}"
        output_id = f"clean_{source}"

        if random.random() < float(self.attributes.fail_prob):
            raise Exception(f"💥 Errore durante la pulizia di {self.params.source}!")

        # Risolviamo il path dell'input (ultima versione disponibile)
        input_path = catalog.resolve(f"analytics/{source}/raw/{input_id}").path
        input_ver = catalog.last_version(f"analytics/{source}/raw/{input_id}")
        
        print(f"🧹 Pulizia dati da: {input_path}")

        # Produciamo il dato pulito dichiarando l'input per la lineage
        with catalog.produce(f"analytics/{source}/clean/{output_id}", 
                             format="out",
                             inputs=[catalog.ref(f"analytics/{source}/raw/{input_id}", input_ver)]) as ctx:
            
            with open(input_path, "r") as f_in:
                data = f_in.read()

            time.sleep(2) # Simulazione lavoro
            
            with open(ctx.path, "w") as f_out:
                f_out.write(f"PULITO: {data}")
            
            ctx.rows = 1

if __name__ == "__main__":
    CleanDataTask().start()