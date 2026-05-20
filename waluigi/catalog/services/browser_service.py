import logging

from waluigi.catalog.repositories.folder_repo import FolderRepository

logger = logging.getLogger("waluigi")


class CatalogBrowserService:

    def __init__(self, folders_repository: FolderRepository):
        self.folders_repository = folders_repository

    def list_folders(self, prefix: str) -> list:
        return self.folders_repository.list_folders(prefix)
