import time
from waluigi.sdk.task import Task

class GlobalReport(Task):

    def run(self):
        print(self.attributes.var)
        print("📊 Generazione Global Report in corso...")

        results = []
        for s in ["ERP", "WEB", "SOCIAL"]:
            with open(f"clean_{s}_{self.params.date}.out", "r") as f:
                print(f"Esecuzione File {s}")
                results.append(f.read())
                time.sleep(7)
        
        with open(f"REPORT_{self.params.date}.out", "w") as f:
            f.write("=== WALUIGI GLOBAL REPORT ===\n")
            f.write("\n".join(results))
        print("✅ Report Finale Creato!")

if __name__ == "__main__":
    GlobalReport().start()