import random
import pandas as pd
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


class RawDataExtract(Task):

    def run(self):
        source = self.params.source.lower()
        date   = self.params.date

        catalog.create_source(SourceCreateRequest(
            id="analytics-local",
            type=SourceType.LOCAL,
            config={},
            description="Local storage for analytics pipeline",
        ))

        dataset = DatasetCreateRequest(
            id=f"analytics/{source}/raw/raw_{source}",
            format=DatasetFormat.PARQUET,
            description=f"Raw data extracted from {self.params.source}",
            source_id="analytics-local",
        )

        rows = [
            {"date": date, "source": source, "metric": f"m_{i}", "value": round(random.uniform(10, 1000), 2)}
            for i in range(random.randint(50, 200))
        ]
        df = pd.DataFrame(rows)

        print(f"Extracting {len(df)} rows from {self.params.source} for {date} ...")

        with catalog.produce(dataset, metadata={"date": date, "source": source}) as writer:
            writer.write(df)

        if writer.skipped:
            print(f"Skipped — same metadata, existing version: {writer.version}")
            return

        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    RawDataExtract().start()
