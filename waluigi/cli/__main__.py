import argparse
import os

from waluigi.cli.services.session  import WaluigiSession
from waluigi.cli.commands.auth      import login, logout
from waluigi.cli.commands.apply     import apply
from waluigi.cli.commands.get       import (
    get_namespaces, get_jobs, get_tasks, get_resources,
    get_workers, get_task_definitions, get_job_definitions, get_cron_jobs, get_users,
)
from waluigi.cli.commands.describe  import (
    describe_job, describe_task, describe_task_definition,
    describe_job_definition, describe_cron_job,
)
from waluigi.cli.commands.lifecycle import (
    pause, resume, cancel, reset, delete, enable_cron_job, disable_cron_job,
)
from waluigi.cli.commands.logs      import get_logs


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
    p = sub.add_parser("apply", help="Apply a YAML descriptor (Namespace, Job, CronJob, JobDefinition, TaskDefinition, NamespaceResources, User)")
    p.add_argument("-f", "--file",      required=True, help="Path to YAML file")
    p.add_argument("-n", "--namespace", help="Override namespace from descriptor metadata")

    # get
    p = sub.add_parser("get", help="List resources")
    p.add_argument("type", choices=["namespaces", "jobs", "tasks", "resources",
                                    "workers", "taskdefinitions", "jobdefinitions",
                                    "cronjobs", "users"])
    p.add_argument("-n", "--namespace", help="Namespace (auto-detected if token has one)")
    p.add_argument("-j", "--job_id",    help="Filter tasks by job ID")
    p.add_argument("-s", "--status",    help="Filter jobs by status")
    p.add_argument("-o", "--output",    choices=["json"])

    # describe
    p = sub.add_parser("describe", help="Show full details of a job, task, or task definition")
    p.add_argument("type",   choices=["job", "task", "taskdefinition", "jobdefinition", "cronjob"])
    p.add_argument("target", help="Resource ID or name")
    p.add_argument("-n", "--namespace", help="Namespace (auto-detected if token has one)")
    p.add_argument("-o", "--output",    choices=["json"])

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
        p.add_argument("type",   choices=["cronjob"])
        p.add_argument("target", help="CronJob name")
        p.add_argument("-n", "--namespace")

    # delete
    p = sub.add_parser("delete", help="Delete a resource")
    p.add_argument("type",   choices=["task", "job", "cronjob", "taskdefinition", "jobdefinition", "namespace"])
    p.add_argument("target", help="Resource ID or namespace name")
    p.add_argument("-n", "--namespace", help="Namespace for task/job")

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
        {
            "namespaces":      lambda: get_namespaces(session, output=out),
            "jobs":            lambda: get_jobs(session, namespace=ns, status=args.status, output=out),
            "tasks":           lambda: get_tasks(session, namespace=ns, job_id=args.job_id, output=out),
            "resources":       lambda: get_resources(session, namespace=ns, output=out),
            "workers":         lambda: get_workers(session, output=out),
            "taskdefinitions": lambda: get_task_definitions(session, namespace=ns, output=out),
            "jobdefinitions":  lambda: get_job_definitions(session, namespace=ns, output=out),
            "cronjobs":        lambda: get_cron_jobs(session, namespace=ns, output=out),
            "users":           lambda: get_users(session, output=out),
        }[args.type]()
    elif args.command == "describe":
        {
            "job":            lambda: describe_job(session, namespace=ns, job_id=args.target, output=out),
            "task":           lambda: describe_task(session, namespace=ns, task_id=args.target, output=out),
            "taskdefinition": lambda: describe_task_definition(session, namespace=ns, defn_id=args.target, output=out),
            "jobdefinition":  lambda: describe_job_definition(session, namespace=ns, defn_id=args.target, output=out),
            "cronjob":        lambda: describe_cron_job(session, namespace=ns, cron_id=args.target, output=out),
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
        delete(session, args.type, args.target, namespace=ns)
    elif args.command == "logs":
        get_logs(session, namespace=ns, task_id=args.task_id,
                 limit=args.lines, follow=args.follow)


if __name__ == "__main__":
    main()
