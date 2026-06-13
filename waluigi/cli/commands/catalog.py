import json as _json

from waluigi.cli.services.session import WaluigiSession
from waluigi.cli.output import ok, data, table, fmt_dt


# ── helpers ───────────────────────────────────────────────────────────────────

def _cat(session: WaluigiSession, path: str, params: dict | None = None):
    return session.http.get(
        f"/catalog/{path}", params=params or {}, headers=session.headers()
    )


def _latest_version(session: WaluigiSession, ns: str, dataset_id: str) -> str | None:
    r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/versions")
    if not ok(r):
        return None
    versions = data(r)
    committed = [v for v in versions if v.get("status") == "committed"]
    if not committed:
        print("No committed versions found.")
        return None
    return committed[0]["version"]


# ── get sources ───────────────────────────────────────────────────────────────

def get_sources(session: WaluigiSession, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = _cat(session, f"namespaces/{ns}/sources")
        if not ok(r):
            return
        rows = data(r)
        table(
            [[s.get("id"), s.get("type"), s.get("description") or "—",
              fmt_dt(s.get("createdate"))] for s in rows],
            headers=["ID", "TYPE", "DESCRIPTION", "CREATED"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


# ── get datasets ──────────────────────────────────────────────────────────────

def get_datasets(session: WaluigiSession, namespace=None, status=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        params = {"status": status} if status else {}
        r = _cat(session, f"namespaces/{ns}/datasets", params)
        if not ok(r):
            return
        rows = data(r)
        table(
            [[d.get("id"), d.get("format"), d.get("source_id"),
              d.get("status"), d.get("description") or "—",
              fmt_dt(d.get("updatedate"))] for d in rows],
            headers=["ID", "FORMAT", "SOURCE", "STATUS", "DESCRIPTION", "UPDATED"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


# ── get versions ──────────────────────────────────────────────────────────────

def get_versions(session: WaluigiSession, dataset_id: str, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/versions")
        if not ok(r):
            return
        rows = data(r)
        table(
            [[str(v.get("version")), v.get("status"), v.get("row_count", "—"),
              v.get("size_bytes", "—"), fmt_dt(v.get("createdate"))] for v in rows],
            headers=["VERSION", "STATUS", "ROWS", "BYTES", "CREATED"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


# ── get schema ────────────────────────────────────────────────────────────────

def get_schema(session: WaluigiSession, dataset_id: str, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/schema")
        if not ok(r):
            return
        rows = data(r)
        table(
            [[c.get("column_name"), c.get("column_type"), c.get("pii", False),
              c.get("status"), c.get("description") or "—"] for c in rows],
            headers=["COLUMN", "TYPE", "PII", "STATUS", "DESCRIPTION"],
            output_arg=output, raw=rows,
        )
    except Exception as e:
        print(f"Error: {e}")


# ── describe dataset ──────────────────────────────────────────────────────────

def describe_dataset(session: WaluigiSession, dataset_id: str, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}")
        if not ok(r):
            return
        d = data(r)
        if output == "json":
            print(_json.dumps(d, indent=2))
            return
        rows = [
            ["id",          d.get("id")],
            ["namespace",   ns],
            ["format",      d.get("format")],
            ["source",      d.get("source_id")],
            ["status",      d.get("status")],
            ["dq_suite",    d.get("dq_suite") or "—"],
            ["description", d.get("description") or "—"],
            ["created",     fmt_dt(d.get("createdate"))],
            ["updated",     fmt_dt(d.get("updatedate"))],
        ]
        from tabulate import tabulate
        print(tabulate(rows, tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


# ── describe source ───────────────────────────────────────────────────────────

def describe_source(session: WaluigiSession, source_id: str, namespace=None, output=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = _cat(session, f"namespaces/{ns}/sources/{source_id}")
        if not ok(r):
            return
        s = data(r)
        if output == "json":
            print(_json.dumps(s, indent=2))
            return
        cfg = s.get("config") or {}
        if isinstance(cfg, str):
            try:
                cfg = _json.loads(cfg)
            except Exception:
                pass
        rows = [
            ["id",          s.get("id")],
            ["namespace",   ns],
            ["type",        s.get("type")],
            ["description", s.get("description") or "—"],
            ["created",     fmt_dt(s.get("createdate"))],
            ["updated",     fmt_dt(s.get("updatedate"))],
        ]
        for k, v in cfg.items():
            rows.append([f"config.{k}", v])
        from tabulate import tabulate
        print(tabulate(rows, tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


# ── delete dataset ────────────────────────────────────────────────────────────

def delete_dataset(session: WaluigiSession, dataset_id: str, namespace=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = session.http.delete(
            f"/catalog/namespaces/{ns}/datasets/{dataset_id}",
            headers=session.headers(),
        )
        if ok(r):
            print(f"dataset/{dataset_id} deleted")
    except Exception as e:
        print(f"Error: {e}")


def delete_version(session: WaluigiSession, dataset_id: str, version: str,
                   namespace=None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        r = session.http.delete(
            f"/catalog/namespaces/{ns}/datasets/{dataset_id}/versions/{version}",
            headers=session.headers(),
        )
        if ok(r):
            print(f"dataset/{dataset_id}@{version} deleted")
    except Exception as e:
        print(f"Error: {e}")


# ── preview ───────────────────────────────────────────────────────────────────

def preview(session: WaluigiSession, dataset_id: str, namespace=None,
            version: str | None = None, lines: int = 10) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        v = version or _latest_version(session, ns, dataset_id)
        if not v:
            return
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/_preview/{v}",
                 params={"limit": lines})
        if not ok(r):
            return
        result = data(r)
        columns = result.get("columns", [])
        rows    = result.get("rows", [])
        if not rows:
            print("No rows returned.")
            return
        from tabulate import tabulate
        print(f"Dataset : {dataset_id}")
        print(f"Version : {v}")
        print(f"Rows    : {len(rows)} (showing up to {lines})")
        print()
        row_lists = [[row.get(c) for c in columns] for row in rows]
        print(tabulate(row_lists, headers=columns, tablefmt="plain"))
    except Exception as e:
        print(f"Error: {e}")


# ── lineage ───────────────────────────────────────────────────────────────────

def lineage(session: WaluigiSession, dataset_id: str, namespace=None,
            version: str | None = None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        v = version or _latest_version(session, ns, dataset_id)
        if not v:
            return
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/lineage/{v}")
        if not ok(r):
            return
        result = data(r)
        print(f"Dataset : {dataset_id}")
        print(f"Version : {v}")
        print()

        upstream = result.get("upstream") or []
        if upstream:
            print("Upstream (inputs):")
            for u in upstream:
                print(f"  {u.get('dataset_id')}  @  {u.get('version', '—')}")
        else:
            print("Upstream: (none)")

        print()
        downstream = result.get("downstream") or []
        if downstream:
            print("Downstream (consumers):")
            for d in downstream:
                print(f"  {d.get('dataset_id')}  @  {d.get('version', '—')}")
        else:
            print("Downstream: (none)")
    except Exception as e:
        print(f"Error: {e}")


# ── dq ────────────────────────────────────────────────────────────────────────

def dq(session: WaluigiSession, dataset_id: str, namespace=None,
       version: str | None = None) -> None:
    ns = session.resolve_namespace(namespace)
    if not ns:
        return
    try:
        v = version or _latest_version(session, ns, dataset_id)
        if not v:
            return
        r = _cat(session, f"namespaces/{ns}/datasets/{dataset_id}/dq/{v}")
        if not ok(r):
            return
        result = data(r)
        score  = result.get("score")
        checks = result.get("details") or []

        print(f"Dataset : {dataset_id}")
        print(f"Version : {v}")
        print(f"Score   : {f'{score:.2%}' if score is not None else '—'}")
        print()
        if checks:
            table(
                [[c.get("rule_id"), "PASS" if c.get("success") else "FAIL",
                  f"{c.get('score', 0):.2%}" if c.get('score') is not None else "—",
                  c.get("error") or ""] for c in checks],
                headers=["RULE", "RESULT", "SCORE", "MESSAGE"],
            )
        else:
            print("No DQ checks recorded.")
    except Exception as e:
        print(f"Error: {e}")
