import random
import pandas as pd
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType

METRICS = {
    "erp":    [("revenue",     "finance"),     ("costs",       "finance"),
               ("orders",      "operations"),  ("refunds",     "finance")],
    "web":    [("sessions",    "traffic"),     ("pageviews",   "traffic"),
               ("conversions", "acquisition"), ("bounce_rate", "engagement")],
    "social": [("followers",   "audience"),    ("impressions", "reach"),
               ("engagements", "interaction"), ("shares",      "viral")],
}


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

        print(f"Extracting raw data for source: {source}, date: {date}")

        rows = [
            {"date": date, "source": source, "metric": metric,
             "value": round(random.uniform(100, 10_000), 2), "category": category}
            for metric, category in METRICS[source]
        ]
        df = pd.DataFrame(rows)

        dataset = DatasetCreateRequest(
            id=f"analytics/{source}/raw/raw_{source}",
            format=DatasetFormat.PARQUET,
            description=f"Raw extracted data for {source}",
            source_id="analytics-local",
        )

        with catalog.produce(dataset, metadata={"date": date, "source": source}) as writer:
            writer.write(df)

        if writer.skipped:
            print(f"Skipped — same metadata, existing version: {writer.version}")
            return

        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    RawDataExtract().start()
