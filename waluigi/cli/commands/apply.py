import json
import getpass
import yaml

from waluigi.cli.services.session import WaluigiSession


def apply(session: WaluigiSession, descriptor_path: str,
          namespace_override: str | None = None) -> None:
    try:
        with open(descriptor_path) as f:
            doc = yaml.safe_load(f)
        kind = doc.get("kind")

        if kind == "Namespace":
            r = session.http.post("/boss/namespaces", json=doc, headers=session.headers())

        elif kind == "User":
            meta     = doc.get("metadata", {})
            spec     = doc.get("spec", {})
            uid      = meta.get("name", "").strip()
            if not uid:
                print("Error: metadata.name (userid) is required"); return
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

        elif kind in ("StatefulJob", "Job"):
            ns = namespace_override or doc.get("metadata", {}).get("namespace") \
                 or session.resolve_namespace(None)
            if not ns: return
            r = session.http.post(f"/boss/namespaces/{ns}/jobs",
                                  json=doc, headers=session.headers())

        elif kind == "TaskDefinition":
            ns = namespace_override or doc.get("metadata", {}).get("namespace") \
                 or session.resolve_namespace(None)
            if not ns: return
            r = session.http.post(f"/boss/namespaces/{ns}/task-definitions",
                                  json=doc, headers=session.headers())

        elif kind in ("NamespaceResources", "ClusterResources"):
            ns = namespace_override or doc.get("metadata", {}).get("namespace") \
                 or session.resolve_namespace(None)
            if not ns: return
            r = session.http.post(f"/boss/namespaces/{ns}/resources",
                                  json=doc, headers=session.headers())

        else:
            print(f"Error: kind '{kind}' not supported"); return

        print(json.dumps(r.json(), indent=2))

    except Exception as e:
        print(f"Error: {e}")
