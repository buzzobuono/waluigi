import json

def _source(row) -> dict | None:
    if row is None:
        return None
    d= dict(row)
    d["config"] = json.loads(d.get("config") or "{}")
    return {
            "id":              d["id"],
            "description":     d.get("description"),
            "type":            d.get("type"),
            "config":          d["config"],
            "username":        d.get("username"),
            "createdate":      d.get("createdate"),
            "updatedate":      d.get("updatedate"),    
        }
        
def _dataset(row) -> dict | None:
    if row is None:
        return None
    d= dict(row)
    return {
            "id":              d.get("id"),
            "format":          d.get("format"),
            "description":     d.get("description"),
            "status":          d.get("status"),
            "source_id":       d.get("source_id"),
            "username":        d.get("username"),
            "createdate":      d.get("createdate"),
            "updatedate":      d.get("updatedate"),
        }
        
def _version(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    return {
            "dataset_id":     d.get("dataset_id"),
            "version":        d.get("version"),
            "location":       d.get("location"),
            "hash":           d.get("hash"),
            "status":         d.get("status"),
            "username":       d.get("username"),
            "createdate":     d.get("createdate"),
            "updatedate":     d.get("updatedate"),
        }