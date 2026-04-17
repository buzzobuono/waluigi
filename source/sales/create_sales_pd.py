import pandas as pd
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog

class CreateSalesDataset(Task):

    def run(self):
        print(f"📊 Creazione dataset vendite per data: {self.params.date}")

        rows = [
            {"date": self.params.date, "product": "A", "quantity": 10, "revenue": 100.0},
            {"date": self.params.date, "product": "B", "quantity": 25, "revenue": 250.0},
            {"date": self.params.date, "product": "C", "quantity": 7,  "revenue": 70.0},
            {"date": self.params.date, "product": "D", "quantity": 42, "revenue": 420.0},
            {"date": self.params.date, "product": "E", "quantity": 3,  "revenue": 30.0},
        ]

        df = pd.DataFrame(rows)

        with catalog.produce(
            "sales/raw/sales_raw_pd",
            format="parquet"
        ) as ctx:

            df.to_parquet(ctx.path, index=False)

            ctx.rows = len(df)

        print(f"✅ Dataset sales_raw_pd scritto in Parquet, righe: {ctx.rows}")


if __name__ == "__main__":
    CreateSalesDataset().start()