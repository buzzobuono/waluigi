import time
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog

class GlobalReport(Task):
    def run(self):
        print(f"Variabile: {self.attributes.var}")
        print("📊 Generazione Global Report...")

        sources = ["erp", "web", "social"]
        inputs_for_lineage = []
        results = []

        # Raccogliamo path e versioni dei 3 input
        for s in sources:
            ds_id = f"clean_{s}"
            path = catalog.resolve(f"analytics/{s}/clean/{ds_id}").path
            ver = catalog.last_version(f"analytics/{s}/clean/{ds_id}")
            
            inputs_for_lineage.append(catalog.ref(f"analytics/{s}/clean/{ds_id}", ver))
            
            with open(path, "r") as f:
                results.append(f.read())
            print(f"Letto dataset: {ds_id} (v: {ver})")

        # Produciamo il report finale con lineage completa
        with catalog.produce("analytics/reports/global_report",
                             format="out",
                             inputs=inputs_for_lineage) as ctx:
            
            with open(ctx.path, "w") as f:
                f.write("=== WALUIGI GLOBAL REPORT ===\n")
                f.write("\n".join(results))
            
            print(f"✅ Report Finale Creato in: {ctx.path}")

if __name__ == "__main__":
    GlobalReport().start()