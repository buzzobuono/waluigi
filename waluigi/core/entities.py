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
    d["tags"] = json.loads(d.get("tags") or "[]")
    return {
            "id":              d["id"],
            "description":     d.get("description"),
            "tags":            d["tags"],
            "owner":           d.get("owner"),
            "status":          d.get("status"),
            "username":        d.get("username"),
            "createdate":      d.get("createdate"),
            "updatedate":      d.get("updatedate"),
        }
        
def _version(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    d["schema_snapshot"] = json.loads(d.get("schema_snapshot") or "[]")
    return {
            "dataset_id":     d["dataset_id"],
            "version":        d.get("version"),
            "source_id":      d.get("source_id"),
            "location":       d.get("location"),
            "format":         d.get("format"),
            "hash":           d.get("hash"),
            "rows":           d.get("rows"),
            "job_id":         d.get("job_id"),
            "task_id":        d.get("task_id"),
            "status":         d.get("status"),
            "username":       d.get("username"),
            "createdate":     d.get("createdate"),
            "updatedate":     d.get("updatedate"),
        }