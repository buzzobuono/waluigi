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
            "dq_suite":        d.get("dq_suite"),
            "username":        d.get("username"),
            "createdate":      d.get("createdate"),
            "updatedate":      d.get("updatedate"),
        }
        
def _version(row) -> dict | None:
    if row is None:
        return None
    d = dict(row)
    return {
            "dataset_id":       d.get("dataset_id"),
            "version":          d.get("version"),
            "location":         d.get("location"),
            "produced_by_task": d.get("produced_by_task"),
            "produced_by_job":  d.get("produced_by_job"),
            "status":           d.get("status"),
            "username":         d.get("username"),
            "createdate":       d.get("createdate"),
            "updatedate":       d.get("updatedate"),
        }

def _expectation(row) -> dict | None:
    if row is None:
        return None
    d = dict(row) if not isinstance(row, dict) else row
    return {
        "id":         d.get("id"),
        "dataset_id": d.get("dataset_id"),
        "rule_id":    d.get("rule_id"),
        "inputs":     d.get("inputs") if isinstance(d.get("inputs"), dict) else json.loads(d.get("inputs") or "{}"),
        "params":     d.get("params") if isinstance(d.get("params"), dict) else json.loads(d.get("params") or "{}"),
        "tolerance":  d.get("tolerance", 1.0),
        "position":   d.get("position", 0),
        "username":   d.get("username"),
        "createdate": d.get("createdate"),
        "updatedate": d.get("updatedate"),
    }