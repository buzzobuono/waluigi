import argparse
import os

from waluigi.cli.services.session  import WaluigiSession
from waluigi.cli.commands.auth      import login, logout
from waluigi.cli.commands.apply     import apply
from waluigi.cli.commands.get       import (
    get_namespaces, get_jobs, get_tasks, get_resources,
    get_workers, get_task_definitions, get_job_definitions, get_cron_jobs, get_users,
    get_secrets,
)
from waluigi.cli.commands.describe  import (
    describe_job, describe_task, describe_task_definition,
    describe_job_definition, describe_cron_job, describe_namespace, describe_secret,
)
from waluigi.cli.commands.lifecycle import (
    pause, resume, cancel, reset, delete, enable_cron_job, disable_cron_job,
)
from waluigi.cli.commands.logs      import get_logs
from waluigi.cli.commands.catalog   import (
    get_sources, get_datasets, get_versions, get_schema, get_metadata,
    describe_dataset, describe_source,
    preview, lineage, dq,
    delete_dataset, delete_version,
)
from waluigi.cli.commands.prune     import prune_workers, prune_prepare
from waluigi.cli.commands.builtins  import apply_builtins


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wlctl", description="Waluigi CLI")
    parser.add_argument(
        "--url",
        default=os.environ.get("WALUIGI_CTL_URL", "http://localhost:8080"),
        help="Console base URL (env: WALUIGI_CTL_URL, default: http://localhost:8080)",
    )
    sub = parser.add_subparsers(dest="command")

    # login / logout
    p = sub.add_parser("login",  help="Authenticate and save token")
    p.add_argument("-u", "--username", required=True)
    p.add_argument("-p", "--password", default=None, help="Password (prompted if omitted)")
    sub.add_parser("logout", help="Remove saved token")

    # apply
    p = sub.add_parser("apply", help="Apply a YAML descriptor (Namespace, Job, CronJob, JobDefinition, TaskDefinition, NamespaceResources, Secret, Source, Dataset, Chart, User)")
    p.add_argument("-f", "--file",      required=True, help="Path to YAML file")
    p.add_argument("-n", "--namespace", help="Override namespace from descriptor metadata")

    # get
    p = sub.add_parser("get", help="List resources")
    p.add_argument("type", choices=["namespaces", "jobs", "tasks", "resources",
                                    "workers", "task-definitions", "job-definitions",
                                    "cron-jobs", "users", "secrets",
                                    "sources", "datasets", "versions", "schema", "metadata"])
    p.add_argument("-n", "--namespace", help="Namespace (auto-detected if token has one)")
    p.add_argument("-j", "--job_id",    help="Filter tasks by job ID")
    p.add_argument("-s", "--status",    help="Filter jobs or datasets by status")
    p.add_argument("-d", "--dataset",   help="Dataset ID (required for versions, schema, metadata)")
    p.add_argument("-v", "--version",   help="Version (required for metadata; default: latest for others)")
    p.add_argument("-o", "--output",    choices=["json"])

    # describe
    p = sub.add_parser("describe", help="Show full details of a resource")
    p.add_argument("type",   choices=["namespace", "job", "task", "task-definition",
                                      "job-definition", "cron-job", "dataset", "source", "secret"])
    p.add_argument("target", help="Resource ID or name")
    p.add_argument("-n", "--namespace", help="Namespace (auto-detected if token has one)")
    p.add_argument("-o", "--output",    choices=["json"])

    # preview
    p = sub.add_parser("preview", help="Preview rows of a Catalog resource")
    p.add_argument("type",              choices=["dataset"], help="Resource type")
    p.add_argument("target",            help="Dataset ID (e.g. web/raw/raw_web)")
    p.add_argument("-n", "--namespace", help="Namespace")
    p.add_argument("-v", "--version",   help="Version (default: latest committed)")
    p.add_argument("-l", "--lines",     type=int, default=10, help="Number of rows (default: 10)")

    # lineage
    p = sub.add_parser("lineage", help="Show upstream/downstream lineage of a Catalog resource")
    p.add_argument("type",              choices=["dataset"], help="Resource type")
    p.add_argument("target",            help="Dataset ID")
    p.add_argument("-n", "--namespace", help="Namespace")
    p.add_argument("-v", "--version",   help="Version (default: latest committed)")

    # dq
    p = sub.add_parser("dq", help="Show data quality results for a Catalog resource")
    p.add_argument("type",              choices=["dataset"], help="Resource type")
    p.add_argument("target",            help="Dataset ID")
    p.add_argument("-n", "--namespace", help="Namespace")
    p.add_argument("-v", "--version",   help="Version (default: latest committed)")

    # cancel / pause / resume
    for cmd in ("cancel", "pause", "resume"):
        p = sub.add_parser(cmd, help=f"{cmd.capitalize()} a job")
        p.add_argument("type",   choices=["job"])
        p.add_argument("target", help="Job ID")
        p.add_argument("-n", "--namespace")

    # reset
    p = sub.add_parser("reset", help="Reset a task, job, or namespace to PENDING")
    p.add_argument("type",   choices=["task", "job", "namespace"])
    p.add_argument("target", help="Task/job ID, or namespace name")
    p.add_argument("-n", "--namespace", help="Namespace for task/job")

    # enable / disable
    for cmd, help_text in (("enable", "Enable a CronJob"), ("disable", "Disable a CronJob")):
        p = sub.add_parser(cmd, help=help_text)
        p.add_argument("type",   choices=["cron-job"])
        p.add_argument("target", help="CronJob name")
        p.add_argument("-n", "--namespace")

    # delete
    p = sub.add_parser("delete", help="Delete a resource")
    p.add_argument("type",   choices=["job", "cron-job", "task-definition", "job-definition",
                                      "namespace", "secret", "dataset", "version"])
    p.add_argument("target", help="Resource ID / version / namespace name")
    p.add_argument("-n", "--namespace", help="Namespace")
    p.add_argument("-d", "--dataset",   help="Dataset ID (required for version)")

    # apply-builtins
    p = sub.add_parser("apply-builtins",
                       help="Apply built-in TaskDefinitions to a namespace")
    p.add_argument("-n", "--namespace", required=True,
                   help="Target namespace")
    p.add_argument("vendor", nargs="?", default=None,
                   help="Vendor/category name (e.g. google). Omit for core built-ins.")

    # prune
    p = sub.add_parser("prune", help="Remove ghost workers or clear prepare directories")
    p.add_argument("type", choices=["workers", "prepare"],
                   help="'workers' removes ghost workers from DB; 'prepare' wipes prepare dirs")
    p.add_argument("-w", "--worker", metavar="URL",
                   help="Target worker URL (for prepare; default: all workers)")

    # logs
    p = sub.add_parser("logs", help="Fetch task logs")
    p.add_argument("task_id")
    p.add_argument("-n", "--namespace")
    p.add_argument("-l", "--lines",  type=int, default=20, help="Number of lines (default: 20)")
    p.add_argument("-f", "--follow", action="store_true",  help="Stream logs in real time")

    return parser


def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()
    if not args.command:
        parser.print_help(); return

    session = WaluigiSession(args.url)
    out     = getattr(args, "output",    None)
    ns      = getattr(args, "namespace", None)

    if args.command == "login":
        login(session, args.username, args.password)
    elif args.command == "logout":
        logout(session)
    elif args.command == "apply":
        apply(session, args.file, namespace_override=ns)
    elif args.command == "get":
        dataset_id = getattr(args, "dataset", None)
        version    = getattr(args, "version",  None)
        {
            "namespaces":      lambda: get_namespaces(session, output=out),
            "jobs":            lambda: get_jobs(session, namespace=ns, status=args.status, output=out),
            "tasks":           lambda: get_tasks(session, namespace=ns, job_id=args.job_id, output=out),
            "resources":       lambda: get_resources(session, namespace=ns, output=out),
            "workers":         lambda: get_workers(session, output=out),
            "task-definitions": lambda: get_task_definitions(session, namespace=ns, output=out),
            "job-definitions":  lambda: get_job_definitions(session, namespace=ns, output=out),
            "cron-jobs":        lambda: get_cron_jobs(session, namespace=ns, output=out),
            "users":           lambda: get_users(session, output=out),
            "secrets":         lambda: get_secrets(session, namespace=ns, output=out),
            "sources":         lambda: get_sources(session, namespace=ns, output=out),
            "datasets":        lambda: get_datasets(session, namespace=ns, status=args.status, output=out),
            "versions":        lambda: get_versions(session, dataset_id, namespace=ns, output=out),
            "schema":          lambda: get_schema(session, dataset_id, namespace=ns, output=out),
            "metadata":        lambda: get_metadata(session, dataset_id, version, namespace=ns, output=out),
        }[args.type]()
    elif args.command == "describe":
        {
            "namespace":      lambda: describe_namespace(session, namespace=args.target, output=out),
            "job":            lambda: describe_job(session, namespace=ns, job_id=args.target, output=out),
            "task":           lambda: describe_task(session, namespace=ns, task_id=args.target, output=out),
            "task-definition": lambda: describe_task_definition(session, namespace=ns, defn_id=args.target, output=out),
            "job-definition":  lambda: describe_job_definition(session, namespace=ns, defn_id=args.target, output=out),
            "cron-job":        lambda: describe_cron_job(session, namespace=ns, cron_id=args.target, output=out),
            "dataset":        lambda: describe_dataset(session, args.target, namespace=ns, output=out),
            "source":         lambda: describe_source(session, args.target, namespace=ns, output=out),
            "secret":         lambda: describe_secret(session, namespace=ns, name=args.target, output=out),
        }[args.type]()
    elif args.command == "enable":
        enable_cron_job(session, namespace=ns, cron_id=args.target)
    elif args.command == "disable":
        disable_cron_job(session, namespace=ns, cron_id=args.target)
    elif args.command == "cancel":
        cancel(session, namespace=ns, job_id=args.target)
    elif args.command == "pause":
        pause(session, namespace=ns, job_id=args.target)
    elif args.command == "resume":
        resume(session, namespace=ns, job_id=args.target)
    elif args.command == "reset":
        reset(session, args.type, args.target, namespace=ns)
    elif args.command == "delete":
        if args.type == "dataset":
            delete_dataset(session, args.target, namespace=ns)
        elif args.type == "version":
            dataset_id = getattr(args, "dataset", None)
            if not dataset_id:
                print("Error: --dataset is required for version")
            else:
                delete_version(session, dataset_id, args.target, namespace=ns)
        else:
            delete(session, args.type, args.target, namespace=ns)
    elif args.command == "apply-builtins":
        apply_builtins(session, namespace=ns, vendor=getattr(args, "vendor", None))
    elif args.command == "prune":
        if args.type == "workers":
            prune_workers(session)
        elif args.type == "prepare":
            prune_prepare(session, worker_url=getattr(args, "worker", None))
    elif args.command == "logs":
        get_logs(session, namespace=ns, task_id=args.task_id,
                 limit=args.lines, follow=args.follow)
    elif args.command == "preview":
        preview(session, args.target, namespace=ns,
                version=args.version, lines=args.lines)
    elif args.command == "lineage":
        lineage(session, args.target, namespace=ns, version=args.version)
    elif args.command == "dq":
        dq(session, args.target, namespace=ns, version=args.version)


if __name__ == "__main__":
    main()
