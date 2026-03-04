import argparse
import time
import random
from waluigi.core.task import Task
from waluigi.core.engine import WaluigiEngine

# --- TASK DI BASE: ESTRAZIONE DATI ---
class RawDataExtract(Task):
    namespace = "analytics"
    # L'ID sarà generato come RawDataExtract_source-X_date-YYYY-MM-DD
    
    def run(self):
        print(f"📥 Estrazione dati da sorgente: {self.source}...")
        time.sleep(3)
        with open(f"raw_{self.source}_{self.date}.tmp", "w") as f:
            f.write(f"Dati grezzi {self.source}")

# --- TASK INTERMEDIO: PULIZIA (Uno per ogni sorgente) ---
class CleanDataTask(Task):
    namespace = "analytics"
    
    def requires(self):
        # Richiede l'estrazione specifica per la sua sorgente
        return [RawDataExtract(tag=self.tag, date=self.date, source=self.source)]
        
    def run(self):
        # Simuliamo un possibile errore casuale per testare la robustezza
        if hasattr(self, 'fail_prob') and random.random() < float(self.fail_prob):
            raise Exception(f"💥 Errore imprevisto durante la pulizia di {self.source}!")
            
        print(f"🧹 Pulizia dati sorgente: {self.source}...")
        time.sleep(2)
        with open(f"clean_{self.source}_{self.date}.tmp", "w") as f:
            f.write(f"Dati puliti {self.source}")

# --- TASK DI AGGREGAZIONE: JOIN DI PIÙ SORGENTI ---
class GlobalReport(Task):
    id = "final_report" # Override dell'ID per vederlo bene in dashboard
    namespace = "analytics"
    
    def requires(self):
        # Questo task fa convergere 3 rami differenti
        return [
            CleanDataTask(tag="ERP", date=self.date, source="ERP", fail_prob="0.1"),
            CleanDataTask(tag="WEB", date=self.date, source="WEB", fail_prob="0.0"),
            CleanDataTask(tag="SOCIAL", date=self.date, source="SOCIAL", fail_prob="0.2")
        ]
        
    def run(self):
        print("📊 Generazione Global Report in corso...")
        time.sleep(5)
        results = []
        for s in ["ERP", "WEB", "SOCIAL"]:
            with open(f"clean_{s}_{self.date}.tmp", "r") as f:
                results.append(f.read())
        
        with open(f"REPORT_{self.date}.final", "w") as f:
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
    report = GlobalReport(date=args.date)
    engine.build(task=report)
