import subprocess
from waluigi.core.task import Task

NAMESPACE = "security_audit"

class FetchRemoteLogs(Task):
    id = "fetch_logs"
    namespace = NAMESPACE

    def run(self):
        log_file = f"logs_{self.params.server_id}.log"
        cmd = ["touch", log_file]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Errore creazione log: {result.stderr}")

class ShellSecurityScanner(Task):
    id = "security_scan"
    namespace = NAMESPACE
    resources = {"cpu_intense": 1}

    def requires(self):
        return [FetchRemoteLogs(
            tags=[self.params.server_id], 
            params={"ip": self.params.ip, "server_id": self.params.server_id},
            attributes={"command": f"ls -l logs_{self.params.server_id}.log"}
        )]

    def run(self):
        report_file = f"report_{self.params.server_id}.json"
        cmd = f"echo '{{\"status\": \"secure\", \"server\": \"{self.params.server_id}\"}}' > {report_file}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Errore generazione report: {result.stderr}")

class NotifyAuditResult(Task):
    id = "audit_notification"
    namespace = NAMESPACE

    def requires(self):
        return [
            ShellSecurityScanner(
                tags=["SRV-01"], 
                params={"ip": "127.0.0.1", "server_id": "SRV-01", "level": "deep"},
                attributes={"command": "cat report_SRV-01.json"}
            ),
            ShellSecurityScanner(
                tags=["SRV-02"], 
                params={"ip": "127.0.0.1", "server_id": "SRV-02", "level": "fast"},
                attributes={"command": "cat report_SRV-02.json"}
            )
        ]

    def run(self):
        cmd = "tar -cvf final_audit.tar report_SRV-01.json report_SRV-02.json"
        result = subprocess.run(cmd.split(), capture_output=True, text=True)
        if result.returncode == 0:
            print("Audit finale impacchettato con successo.")
