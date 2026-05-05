import pandas as pd
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


class CreateSalesDataset(Task):

    def run(self):
        date = self.params.date

        catalog.create_source(SourceCreateRequest(
            id="local",
            type=SourceType.LOCAL,
            config={},
            description="Local Source",
        ))

        print(f"Creating sales parquet dataset for date: {date}")

        df = pd.DataFrame([
            {"date": date, "product": "A", "quantity": 10, "revenue": 100.0},
            {"date": date, "product": "B", "quantity": 25, "revenue": 250.0},
            {"date": date, "product": "C", "quantity":  7, "revenue":  70.0},
            {"date": date, "product": "D", "quantity": 42, "revenue": 420.0},
            {"date": date, "product": "E", "quantity":  3, "revenue":  30.0},
        ])

        dataset = DatasetCreateRequest(
            id="sales/raw/sales_raw_pd",
            format=DatasetFormat.PARQUET,
            description="Sales raw data (parquet)",
            source_id="local",
        )

        with catalog.produce(dataset, metadata={"date": date, "source": "SAP_EXTRACT"}) as writer:
            writer.write(df)

        if writer.skipped:
            print(f"Skipped — same metadata, existing version: {writer.version}")
            return

        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    CreateSalesDataset().start()
