import argparse
import os

from waluigi.cli.commands.run import run_task


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wlrun",
        description=(
            "Run a Waluigi task or job locally — no Boss, no Worker, no cluster required.\n\n"
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
        default=os.environ.get("WALUIGI_CATALOG_NAMESPACE"),
        help="Catalog namespace (env: WALUIGI_CATALOG_NAMESPACE)",
    )
    parser.add_argument(
        "--catalog-url",
        default=os.environ.get("WALUIGI_CATALOG_URL"),
        help="Catalog URL (env: WALUIGI_CATALOG_URL)",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    if args.cmd is None and args.file is None:
        parser.print_help()
        return

    run_task(
        cmd=args.cmd,
        file=args.file,
        task_id=args.task,
        params=args.params,
        namespace=args.namespace,
        catalog_url=args.catalog_url,
    )


if __name__ == "__main__":
    main()
