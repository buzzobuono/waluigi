
import argparse
import time
from waluigi.core.task import Task
from waluigi.core.engine import WaluigiEngine

class Dep1Task(Task):
    
    def run(self):
        print(f"   [Dep1Task] Running")
        time.sleep(5)
        with open(f"dep1_{self.date}.out", "w") as f:
            f.write(f"Dati estratti per il giorno {self.date}")
        print(f"   [Dep1Task] Done")
        
class MainTask(Task):
    def requires(self):
        return [Dep1Task(date=self.date)]
    
    def run(self):
        print(f"   [MainTask] Running")
        time.sleep(5)
        input_file = f"dep1_{self.date}.out"
        
        with open(input_file, "r") as f:
            data = f.read()
        with open(f"main_{self.date}.out", "w") as f:
            f.write(f"{data} e processati da Waluigi con successo!")
        print(f"   [MainTask] Done")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Waluigi Single Task Runner")
    parser.add_argument(
        "date", 
        help="La data da processare (formato YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    engine = WaluigiEngine()

    try:
        task = MainTask(date=args.date)
        engine.build(job_id=f"job_{args.date}", task=task)
    except Exception as e:
        print(f"\n❌ Errore fatale per gli args {args}: {e}")
        exit(1) # Esci con errore per segnalarlo al sistema (es. Bash o CI/CD)
