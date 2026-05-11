import logging

from waluigi.catalog.db import CatalogDB

logger = logging.getLogger("waluigi")


class CatalogBrowserService:

    def __init__(self, db: CatalogDB):
        self.db = db

    # ── Folders ───────────────────────────────────────────────────────────────

    def list_folders(self, prefix: str) -> list:
        return self.db.list_folders(prefix)

