import argparse
import time
import random
from waluigi.core.task import Task

NAMESPACE="analytics"

class RawDataExtract(Task):
    id = "raw_data_extract"
    namespace = NAMESPACE
    
    def run(self):
        print(f"📥 Estrazione dati da sorgente: {self.params.source}...")
        time.sleep(3)
        with open(f"raw_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati grezzi {self.params.source}")

class CleanDataTask(Task):
    id = "clean_data"
    namespace = NAMESPACE
    resources = {
       "pdc": 1
    }
    
    def requires(self):
        return [RawDataExtract(tags=self.tags, params={"date": self.params.date, "source" : self.params.source})]
        
    def run(self):
        if random.random() < float(self.attributes.fail_prob):
            raise Exception(f"💥 Errore imprevisto durante la pulizia di {self.params.source}!")
            
        print(f"🧹 Pulizia dati sorgente: {self.params.source}...")
        time.sleep(2)
        with open(f"clean_{self.params.source}_{self.params.date}.out", "w") as f:
            f.write(f"Dati puliti {self.params.source}")

class GlobalReport(Task):
    id = "final_report"
    namespace = NAMESPACE

    def requires(self):
        self.attributes.var="pippo"
        return [
            CleanDataTask(tags=["ERP"], params={"date": self.params.date, "source" : "ERP"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["WEB"], params={"date": self.params.date, "source" : "WEB"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["SOCIAL"], params={"date": self.params.date, "source" : "SOCIAL"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["CONTABILITA"], params={"date": self.params.date, "source" : "CONTABILITA"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["MDM"], params={"date": self.params.date, "source" : "MDM"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["ANALYTICS"], params={"date": self.params.date, "source" : "ANALYTICS"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["MOBILE"], params={"date": self.params.date, "source" : "MOBILE"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["AGENTIC"], params={"date": self.params.date, "source" : "AGENTIC"}, attributes= { "fail_prob" : "0.0" }),
            CleanDataTask(tags=["PROXIMITY"], params={"date": self.params.date, "source" : "PROXIMITY"}, attributes= { "fail_prob" : "0.0" })
        ]
        
    def run(self):
        print(self.attributes.var)
        print("📊 Generazione Global Report in corso...")
        time.sleep(5)
        results = []
        for s in ["ERP", "WEB", "SOCIAL", "CONTABILITA", "MDM", "ANALYTICS", "MOBILE", "AGENTIC", "PROXIMITY"]:
            with open(f"clean_{s}_{self.params.date}.out", "r") as f:
                results.append(f.read())
        
        with open(f"REPORT_{self.params.date}.out", "w") as f:
            f.write("=== WALUIGI GLOBAL REPORT ===\n")
            f.write("\n".join(results))
        print("✅ Report Finale Creato!")
        