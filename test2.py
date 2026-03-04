import argparse
import time
import random
from waluigi.core.task import Task
from waluigi.core.engine import WaluigiEngine

NAMESPACE="analytics"

# --- TASK DI BASE: ESTRAZIONE DATI ---
class RawDataExtract(Task):
    id = "raw_data_extract"
    #namespace = "Extract Processes"
    namespace = NAMESPACE
    # L'ID sarà generato come RawDataExtract_source-X_date-YYYY-MM-DD
    
    def run(self):
        print(f"📥 Estrazione dati da sorgente: {self.params.source}...")
        time.sleep(3)
        with open(f"raw_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati grezzi {self.params.source}")

# --- TASK INTERMEDIO: PULIZIA (Uno per ogni sorgente) ---
class CleanDataTask(Task):
    id = "clean_data"
    #namespace = "Clean Processes"
    namespace = NAMESPACE
    
    def requires(self):
        # Richiede l'estrazione specifica per la sua sorgente
        return [RawDataExtract(tags=self.tags, params={"date": self.params.date, "source" : self.params.source})]
        
    def run(self):
        # Simuliamo un possibile errore casuale per testare la robustezza
        if random.random() < float(self.attributes.fail_prob):
            raise Exception(f"💥 Errore imprevisto durante la pulizia di {self.params.source}!")
            
        print(f"🧹 Pulizia dati sorgente: {self.params.source}...")
        time.sleep(2)
        with open(f"clean_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati puliti {self.params.source}")

# --- TASK DI AGGREGAZIONE: JOIN DI PIÙ SORGENTI ---
class GlobalReport(Task):
    id = "final_report"
    #namespace = "Main Processes"
    namespace = NAMESPACE
    
    def is_complete(self):
        return False

    def requires(self):
        # Questo task fa convergere 3 rami differenti
        return [
            CleanDataTask(tags=["ERP"], params={"date": self.params.date, "source" : "ERP"}, attributes= { "fail_prob" : "0.1" }),
            CleanDataTask(tags=["WEB"], params={"date": self.params.date, "source" : "WEB"}, attributes= { "fail_prob" : "0.2" }),
            CleanDataTask(tags=["SOCIAL"], params={"date": self.params.date, "source" : "SOCIAL"}, attributes= { "fail_prob" : "0.3" })
        ]
        
    def run(self):
        print("📊 Generazione Global Report in corso...")
        time.sleep(5)
        results = []
        for s in ["ERP", "WEB", "SOCIAL"]:
            with open(f"clean_{s}_{self.params.date}.out", "r") as f:
                results.append(f.read())
        
        with open(f"REPORT_{self.params.date}.out", "w") as f:
            f.write("=== WALUIGI GLOBAL REPORT ===\n")
            f.write("\n".join(results))
        print("✅ Report Finale Creato!")

# --- MAIN ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="YYYY-MM-DD")
    args = parser.parse_args()

    engine = WaluigiEngine()
    
    # Lanciamo il task di aggregazione che scatenerà tutta la piramide
    report = GlobalReport(params= {"date": args.date})
    engine.build(task=report)
