import random
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


class CleanDataTask(Task):

    def run(self):
        source     = self.params.source.lower()
        date       = self.params.date
        fail_prob  = float(self.attributes.fail_prob)

        if random.random() < fail_prob:
            raise RuntimeError(f"Simulated failure while cleaning {self.params.source}")

        catalog.create_source(SourceCreateRequest(
            id="analytics-local",
            type=SourceType.LOCAL,
            config={},
            description="Local storage for analytics pipeline",
        ))

        raw_id = f"analytics/{source}/raw/raw_{source}"
        reader = catalog.resolve(raw_id)
        df     = reader.read()

        print(f"Read {len(df)} rows from {raw_id} @ {reader.version}")

        # Basic cleaning: drop nulls, normalize metric names
        df = df.dropna()
        df["metric"] = df["metric"].str.strip().str.lower()

        print(f"After cleaning: {len(df)} rows")

        dataset = DatasetCreateRequest(
            id=f"analytics/{source}/clean/clean_{source}",
            format=DatasetFormat.PARQUET,
            description=f"Cleaned data for {self.params.source}",
            source_id="analytics-local",
        )

        lineage = [{"dataset_id": reader.dataset_id, "version": reader.version}]

        with catalog.produce(dataset, metadata={"date": date, "source": source},
                             inputs=lineage) as writer:
            writer.write(df)

        if writer.skipped:
            print(f"Skipped — same metadata, existing version: {writer.version}")
            return

        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(df)} rows)")


if __name__ == "__main__":
    CleanDataTask().start()
