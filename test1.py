
import argparse
import time
from waluigi.core.task import Task
from waluigi.core.engine import WaluigiEngine

class Dep4Task(Task):
    
    id = "dep4"
    namespace = "test1"
    
    #def is_complete(self):
    #    return False
        
    def run(self):
        time.sleep(5)
        with open(f"dep4_{self.date}.out", "w") as f:
            f.write(f"Dati estratti per il giorno {self.date}")

class Dep3Task(Task):
    
    id = "dep3"
    namespace = "test1"
    
    #def is_complete(self):
    #    return False
        
    def run(self):
        time.sleep(5)
        with open(f"dep3_{self.date}.out", "w") as f:
            f.write(f"Dati estratti per il giorno {self.date}")

class Dep2Task(Task):
    
    id = "dep2"
    namespace = "test1"
    
    #def is_complete(self):
    #    return False
    def requires(self):
        return [Dep4Task(date=self.date, p="4")]

    def run(self):
        time.sleep(5)
        with open(f"dep2_{self.date}.out", "w") as f:
            f.write(f"Dati estratti per il giorno {self.date}")
  
class Dep1Task(Task):
    
    id = "dep1"
    namespace = "test1"
    
    #def is_complete(self):
    #   return False
    
    def requires(self):
        return [Dep2Task(date=self.date, p="4"), Dep3Task(date=self.date, p="43")]
    
    def run(self):
        time.sleep(5)
        with open(f"dep1_{self.date}.out", "w") as f:
            f.write(f"Dati estratti per il giorno {self.date}")
        
class MainTask(Task):
    
    id = "main"
    namespace = "test1"
    
    def requires(self):
        return [Dep1Task(date=self.date, p="45")]
        #return []
    
    def is_complete(self):
        return False
        
    def run(self):
        time.sleep(5)
        input_file = f"dep1_{self.date}.out"
        
        with open(input_file, "r") as f:
            data = f.read()
        with open(f"main_{self.date}.out", "w") as f:
            f.write(f"{data} e processati da Waluigi con successo!")
        

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
        engine.build(task=task)
    except Exception as e:
        print(f"\n❌ Errore fatale per gli args {args}: {e}")
        exit(1) # Esci con errore per segnalarlo al sistema (es. Bash o CI/CD)
