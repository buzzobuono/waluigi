import pandas as pd
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import DatasetCreateRequest, DatasetFormat, SourceCreateRequest, SourceType


class GlobalReport(Task):

    def run(self):
        date = self.params.date
        print(f"Building global report for {date} (var={self.attributes.var}) ...")

        catalog.create_source(SourceCreateRequest(
            id="analytics-local",
            type=SourceType.LOCAL,
            config={},
            description="Local storage for analytics pipeline",
        ))

        sources  = ["erp", "web", "social"]
        frames   = []
        lineage  = []

        for source in sources:
            reader = catalog.resolve(f"analytics/{source}/clean/clean_{source}")
            df     = reader.read()
            df["pipeline_source"] = source
            frames.append(df)
            lineage.append({"dataset_id": reader.dataset_id, "version": reader.version})
            print(f"  {source}: {len(df)} rows @ {reader.version}")

        report_df = pd.concat(frames, ignore_index=True)
        print(f"Total rows in report: {len(report_df)}")

        dataset = DatasetCreateRequest(
            id="analytics/reports/global_report",
            format=DatasetFormat.PARQUET,
            description="Global consolidated report across all sources",
            source_id="analytics-local",
        )

        with catalog.produce(dataset, metadata={"date": date}, inputs=lineage) as writer:
            writer.write(report_df)

        if writer.skipped:
            print(f"Skipped — same metadata, existing version: {writer.version}")
            return

        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(report_df)} rows)")


if __name__ == "__main__":
    GlobalReport().start()
