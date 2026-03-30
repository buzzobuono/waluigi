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
        ]

        with catalog.produce(NAMESPACE, "sales_raw", format="csv") as ctx:
            with open(ctx.path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            ctx.rows = len(rows)

        print(f"✅ Dataset {NAMESPACE}/sales_raw scritto, righe: {ctx.rows}")


if __name__ == "__main__":
    CreateSalesDataset().start()
