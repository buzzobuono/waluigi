import json
import getpass
import yaml

from waluigi.cli.services.session import WaluigiSession


def apply(session: WaluigiSession, descriptor_path: str,
          namespace_override: str | None = None) -> None:
    try:
        with open(descriptor_path) as f:
            docs = [d for d in yaml.safe_load_all(f) if d is not None]
    except Exception as e:
        print(f"Error: {e}")
        return

    for doc in docs:
        _apply_one(session, doc, namespace_override)


def _apply_one(session: WaluigiSession, doc: dict,
               namespace_override: str | None) -> None:
    kind  = doc.get("kind")
    meta  = doc.get("metadata") or {}
    _ns   = ""
    _name = ""
    try:
        if kind == "Namespace":
            r = session.http.post("/boss/namespaces", json=doc, headers=session.headers())

        elif kind == "User":
            spec     = doc.get("spec", {})
            uid      = meta.get("name", "").strip()
            if not uid:
                print("Error: metadata.name (userid) is required"); return
            _name = uid
            password = spec.get("password") or None
            if not password:
                password = getpass.getpass(
                    f"Password for '{uid}' (leave blank to keep unchanged): "
                ) or None
            body = {
                "username":   meta.get("displayName", ""),
                "namespaces": spec.get("namespaces", []),
            }
            if password:
                body["password"] = password
            r = session.http.put(f"/auth/users/{uid}", json=body, headers=session.headers())

        elif kind == "Job":
            _ns = namespace_override or meta.get("namespace") \
                  or session.resolve_namespace(None)
            if not _ns: return
            r = session.http.post(f"/boss/namespaces/{_ns}/jobs",
                                  json=doc, headers=session.headers())

        elif kind == "CronJob":
            _ns   = namespace_override or meta.get("namespace") \
                    or session.resolve_namespace(None)
            _name = meta.get("name", "")
            if not _ns: return
            r = session.http.post(f"/boss/namespaces/{_ns}/cron-jobs",
                                  json=doc, headers=session.headers())

        elif kind == "JobDefinition":
            _ns   = namespace_override or meta.get("namespace") \
                    or session.resolve_namespace(None)
            _name = meta.get("name", "")
            if not _ns: return
            r = session.http.post(f"/boss/namespaces/{_ns}/job-definitions",
                                  json=doc, headers=session.headers())

        elif kind == "TaskDefinition":
            _ns   = namespace_override or meta.get("namespace") \
                    or session.resolve_namespace(None)
            _name = meta.get("name", "")
            if not _ns: return
            r = session.http.post(f"/boss/namespaces/{_ns}/task-definitions",
                                  json=doc, headers=session.headers())

        elif kind in ("NamespaceResources", "ClusterResources"):
            _ns = namespace_override or meta.get("namespace") \
                  or session.resolve_namespace(None)
            if not _ns: return
            r = session.http.post(f"/boss/namespaces/{_ns}/resources",
                                  json=doc, headers=session.headers())

        elif kind == "Secret":
            _ns   = namespace_override or meta.get("namespace") \
                    or session.resolve_namespace(None)
            _name = meta.get("name", "").strip()
            if not _ns:   return
            if not _name:
                print("Error: metadata.name (secret group name) is required"); return
            spec = doc.get("spec", {})
            if not isinstance(spec, dict):
                print("Error: spec must be a dict of KEY: value pairs"); return
            r = session.http.post(f"/boss/namespaces/{_ns}/secrets/{_name}",
                                  json=spec, headers=session.headers())

        elif kind == "Source":
            _ns   = namespace_override or meta.get("namespace") \
                    or session.resolve_namespace(None)
            _name = meta.get("name", "").strip()
            if not _ns:   return
            if not _name:
                print("Error: metadata.name (source id) is required"); return
            spec = doc.get("spec", {})
            body = {
                "id":          _name,
                "type":        spec.get("type", "local"),
                "config":      spec.get("config", {}),
                "description": spec.get("description"),
            }
            r = session.http.post(f"/catalog/namespaces/{_ns}/sources",
                                  json=body, headers=session.headers())

        elif kind == "Chart":
            _ns        = namespace_override or meta.get("namespace") \
                         or session.resolve_namespace(None)
            dataset_id = meta.get("dataset", "").strip()
            if not _ns:        return
            if not dataset_id:
                print("Error: metadata.dataset is required"); return
            charts  = (doc.get("spec") or {}).get("charts") or []
            base    = f"/catalog/namespaces/{_ns}/datasets/{dataset_id}/charts"
            headers = session.headers()
            # full replace: delete all existing, then add new ones
            existing = session.http.get(base, headers=headers)
            if existing.status_code == 200:
                for c in (existing.json().get("data") or []):
                    session.http.delete(f"{base}/{c['id']}", headers=headers)
            for i, chart in enumerate(charts):
                body = {"key": chart["key"], "title": chart["title"],
                        "spec": chart.get("spec", {}), "position": i}
                session.http.post(base, json=body, headers=headers)
            _name = dataset_id
            r = type("R", (), {"status_code": 200,
                               "json": lambda self: {"data": {"id": dataset_id}}})()

        else:
            print(f"Error: kind '{kind}' not supported"); return

        _print_applied(kind, doc, r, ns=_ns, name=_name)

    except Exception as e:
        print(f"Error applying {kind}: {e}")


def _print_applied(kind: str, doc: dict, r, ns: str = "", name: str = "") -> None:
    from waluigi.cli.output import ok
    if not ok(r):
        return
    body = r.json()
    d    = body.get("data", body) or {}
    verb = "created" if r.status_code == 201 else "configured"
    meta = doc.get("metadata") or {}
    name = name or meta.get("name", "")
    ns   = ns   or meta.get("namespace", "")

    if kind == "Namespace":
        ref = d.get("namespace") or name
        print(f"namespace/{ref} {verb}")
    elif kind == "User":
        print(f"user/{name} {verb}")
    elif kind == "Job":
        ref = d.get("job_id") or name
        print(f"job/{ref} {verb}")
    elif kind == "CronJob":
        ref = d.get("id") or name
        print(f"cron-job/{ref} {verb}")
    elif kind == "JobDefinition":
        ref = d.get("id") or name
        print(f"job-definition/{ref} {verb}")
    elif kind == "TaskDefinition":
        ref = d.get("id") or name
        print(f"task-definition/{ref} {verb}")
    elif kind in ("NamespaceResources", "ClusterResources"):
        print(f"namespaceresources/{ns} {verb}")
    elif kind == "Secret":
        print(f"secret/{ns}/{name} {verb}")
    elif kind == "Source":
        ref = d.get("id") or name
        print(f"source/{ns}/{ref} {verb}")
    elif kind == "Chart":
        charts = (doc.get("spec") or {}).get("charts") or []
        print(f"chart/{ns}/{name} {verb} ({len(charts)} chart(s))")
    else:
        print(f"{kind.lower()}/{name} {verb}")
