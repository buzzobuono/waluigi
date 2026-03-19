import time
import random
from waluigi.sdk.task import Task

class CleanDataTask(Task):

    def run(self):
        if random.random() < float(self.attributes.fail_prob):
            raise Exception(f"💥 Errore imprevisto durante la pulizia di {self.params.source}!")
        print(f"🧹 Pulizia dati sorgente: {self.params.source}...")
        time.sleep(2)
        with open(f"clean_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati puliti {self.params.source}")

if __name__ == "__main__":
    CleanDataTask().start()