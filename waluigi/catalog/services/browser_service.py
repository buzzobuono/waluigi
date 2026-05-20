import logging

from waluigi.catalog.repositories.folder_repo import FolderRepository

logger = logging.getLogger("waluigi")


class CatalogBrowserService:

    def __init__(self, repo: FolderRepository):
        self.repo = repo

    def list_folders(self, prefix: str) -> list:
        return self.repo.list_folders(prefix)
