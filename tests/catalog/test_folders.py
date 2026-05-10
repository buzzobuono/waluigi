import pytest
from waluigi.sdk.catalog import catalog

def test_get_root_folders():
    result = catalog.list_folders("")
    assert isinstance(result, dict), "Il risultato deve essere un dizionario"
    