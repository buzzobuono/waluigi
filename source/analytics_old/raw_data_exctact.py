import time
from waluigi.sdk.task import Task

class RawDataExtract(Task):

    def run(self):
        print(f"📥 Estrazione dati da sorgente: {self.params.source}...")
        
        steps = 20
        for step in range(steps):
            print(f"Estrazione Step {step+1}/{steps}")
            time.sleep(1)

        with open(f"raw_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati grezzi {self.params.source}")

if __name__ == "__main__":
    RawDataExtract().start()