import logging
import yaml
import uvicorn
from fastapi import FastAPI

from waluigi.catalog.config.args import args
from waluigi.catalog.config.dependencies import init_db

from waluigi.catalog.api.routes.browser_router import browser_router
from waluigi.catalog.api.routes.source_router import source_router
from waluigi.catalog.api.routes.metadata_router import metadata_router
from waluigi.catalog.api.routes.schema_router import schema_router
from waluigi.catalog.api.routes.dq_router import dq_dataset_router, dq_global_router
from waluigi.catalog.api.routes.chart_router import chart_router
from waluigi.catalog.api.routes.lineage_router import lineage_router
from waluigi.catalog.api.routes.version_router import version_router
from waluigi.catalog.api.routes.dataset_router import dataset_router
from waluigi.catalog.api.routes.materialize_router import materialize_router

logger = logging.getLogger("waluigi")

app = FastAPI(
    title="Waluigi Catalog",
    description="Data Catalog service: manages source, datasets, versions, schema, lineage and metadata.",
    version="2.0.0",
)

app.include_router(browser_router)
app.include_router(source_router)
app.include_router(metadata_router)
app.include_router(schema_router)
app.include_router(dq_dataset_router)
app.include_router(dq_global_router)
app.include_router(chart_router)
app.include_router(lineage_router)
app.include_router(version_router)
app.include_router(dataset_router)
app.include_router(materialize_router)


def main():
    try:
        with open("logging.yaml") as f:
            logging.config.dictConfig(yaml.safe_load(f))
    except Exception:
        logging.basicConfig(level=logging.INFO)
        logger.warning("File logging.yaml non trovato, uso configurazione base.")

    logger.info("Waluigi Catalog v2")
    logger.info(f"  Binding : {args.bind_address}:{args.port}")
    logger.info(f"  URL     : http://{args.host}:{args.port}")
    logger.info(f"  DB      : {args.db_url}")
    logger.info(f"  Data    : {args.data_path}")

    init_db(args.db_url)

    uvicorn.run(app, host=args.bind_address, port=args.port, log_config=None)


if __name__ == "__main__":
    main()
