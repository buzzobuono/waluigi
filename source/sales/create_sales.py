import csv
from waluigi.sdk.task import Task
from waluigi.sdk.catalog import catalog
from waluigi.catalog.models import * 
   
DATASET_ID = "sales/raw/sales_raw2"

class CreateSalesDataset(Task):

    def run(self):
        source_id="local"
        
        print(f"📊 Creazione local source: {source_id}")
        
        source = SourceCreateRequest(
                id=source_id,
                type=SourceType.LOCAL,
                config={},
                description="Local Source"
        )
    
        catalog.create_source(source)
   
        print(f"📊 Creazione dataset vendite per data: {self.params.date}")
        
        rows = [
            {"date": self.params.date, "product": "A", "quantity": 10, "revenue": 100.0},
            {"date": self.params.date, "product": "B", "quantity": 25, "revenue": 250.0},
            {"date": self.params.date, "product": "C", "quantity":  7, "revenue":  70.0},
            {"date": self.params.date, "product": "D", "quantity": 42, "revenue": 420.0},
            {"date": self.params.date, "product": "E", "quantity":  3, "revenue":  30.0},
            {"date": self.params.date, "product": "F", "quantity":  9, "revenue": 350.0},
        ]

        dataset = DatasetCreateRequest(
            id=DATASET_ID,
            format=DatasetFormat.CSV,
            description="Sales raw",
            source_id=source_id
        )
        with catalog.produce(dataset) as ctx:
            ctx.write(rows, source="SAP_EXTRACT", date_ref=self.params.date)
            
        if ctx.skipped:
            print(f"⏭️  Contenuto invariato — versione: {ctx.version}")
            return

        print(f"✅ {ctx.dataset_id}@{ctx.version} scritto.")


if __name__ == "__main__":
    CreateSalesDataset().start()
