import argparse
import atexit
import os
import pathlib
import socket
import subprocess
import sys
import time

from waluigi.cli.commands.run import run_task


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def _start_local_catalog(data_dir: str) -> tuple[subprocess.Popen, str]:
    """
    Start an embedded wlcatalog subprocess pointing to data_dir.
    Returns (proc, catalog_url). The process is registered for atexit cleanup.
    """
    path = pathlib.Path(data_dir).resolve()
    path.mkdir(parents=True, exist_ok=True)

    port   = _find_free_port()
    db_url = f"sqlite:///{path}/catalog.db"

    proc = subprocess.Popen(
        [
            "wlcatalog",
            "--port",      str(port),
            "--data-path", str(path),
            "--db-url",    db_url,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    atexit.register(proc.terminate)

    # Wait until the TCP port is open (max 10 s)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            conn = socket.create_connection(("localhost", port), timeout=0.3)
            conn.close()
            break
        except OSError:
            time.sleep(0.1)
    else:
        proc.terminate()
        print(f"[wlrun] ERROR: local catalog failed to start on port {port}", file=sys.stderr)
        sys.exit(1)

    return proc, f"http://localhost:{port}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wlrun",
        description=(
            "Run a Waluigi task or job locally — no Boss, no Worker, no cluster required.\n\n"
            "Catalog calls are intercepted and written to --data-dir (default: ./wlrun-data).\n\n"
            "Examples:\n"
            "  wlrun -f pipeline.yaml                       # run full job\n"
            "  wlrun -f pipeline.yaml -t extract            # single task\n"
            "  wlrun -f pipeline.yaml -t extract -p date=2026-06-15\n"
            "  wlrun 'python script.py' -p date=2026-06-15 -n analytics\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "cmd",
        nargs="?",
        default=None,
        help="Shell command to run directly (omit when using --file)",
    )
    parser.add_argument(
        "-f", "--file",
        help="YAML descriptor (Job or JobDefinition); can include TaskDefinition docs",
    )
    parser.add_argument(
        "-t", "--task",
        help="Task ID to run from --file (omit to run the full job in DAG order)",
    )
    parser.add_argument(
        "-p", "--params",
        nargs="*",
        metavar="KEY=VALUE",
        help="Task params — override or extend job-level defaults (repeatable)",
    )
    parser.add_argument(
        "-n", "--namespace",
        default=os.environ.get("WALUIGI_CATALOG_NAMESPACE", "local"),
        help="Catalog namespace (env: WALUIGI_CATALOG_NAMESPACE, default: local)",
    )
    parser.add_argument(
        "-d", "--data-dir",
        default=os.environ.get("WALUIGI_RUN_DATA_DIR", "./wlrun-data"),
        metavar="DIR",
        help="Local folder for catalog data (env: WALUIGI_RUN_DATA_DIR, default: ./wlrun-data)",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    if args.cmd is None and args.file is None:
        parser.print_help()
        return

    _proc, catalog_url = _start_local_catalog(args.data_dir)

    data_path = pathlib.Path(args.data_dir).resolve()
    print(f"[wlrun] catalog       : {catalog_url}")
    print(f"[wlrun] data dir      : {data_path}")
    print(f"[wlrun] namespace     : {args.namespace}")
    print()

    run_task(
        cmd=args.cmd,
        file=args.file,
        task_id=args.task,
        params=args.params,
        namespace=args.namespace,
        catalog_url=catalog_url,
    )


if __name__ == "__main__":
    main()
