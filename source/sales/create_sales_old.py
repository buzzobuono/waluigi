import csv
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog

NAMESPACE = "sales/raw"

class CreateSalesDataset(Task):

    def run(self):
        print(f"📊 Creazione dataset vendite per data: {self.params.date}")

        rows = [
            {"date": self.params.date, "product": "A", "quantity": 10, "revenue": 100.0},
            {"date": self.params.date, "product": "B", "quantity": 25, "revenue": 250.0},
            {"date": self.params.date, "product": "C", "quantity":  7, "revenue":  70.0},
            {"date": self.params.date, "product": "D", "quantity": 42, "revenue": 420.0},
            {"date": self.params.date, "product": "E", "quantity":  3, "revenue":  30.0},
            {"date": self.params.date, "product": "F", "quantity":  9, "revenue":  350.0}
        ]

        # 1. Produzione del dataset
        with catalog.produce(NAMESPACE, "sales_raw", format="csv") as ctx:
            with open(ctx.path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            ctx.rows = len(rows)
        
        # Una volta usciti dal blocco 'with', la versione è committata.
        # Ora ctx.committed_version contiene la versione definitiva (o quella nuova o quella recuperata)

        # 2. Aggiunta dei CUSTOM METADATA associati alla versione specifica
        ver = ctx.committed_version
        
        catalog.set_metadata(NAMESPACE, "sales_raw", "source", ver, "SAP_EXTRACT")
        catalog.set_metadata(NAMESPACE, "sales_raw", "owner", ver, "sales_team")
        catalog.set_metadata(NAMESPACE, "sales_raw", "date_ref", ver, self.params.date)

        print(f"✅ Dataset {NAMESPACE}/sales_raw@{ver} scritto con metadati storicizzati.")

if __name__ == "__main__":
    CreateSalesDataset().start()
