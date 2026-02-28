import argparse
import os
from waluigi.core.db import WaluigiDB

def main():
    parser = argparse.ArgumentParser(description="🟣 Waluigi CLI - Gestione Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Comandi disponibili")

    # Comando LIST
    subparsers.add_parser("list", help="Elenca tutti i task e il loro stato")

    # Comando RESET
    reset_parser = subparsers.add_parser("reset", help="Elimina un task dal DB per rifarlo")
    reset_parser.add_argument("task_id", help="L'ID del task da resettare (es. Dep1Task_id-aaa)")
    job_reset_parser = subparsers.add_parser("reset-job", help="Resetta tutti i task di un Job")
    job_reset_parser.add_argument("job_id", help="L'ID del job (es. job_171000000)")
    args = parser.parse_args()
    
    # Inizializza DB (cerca waluigi.db nella cartella corrente)
    db_path = os.path.join(os.getcwd(), "waluigi.db")
    if not os.path.exists(db_path):
        print("🚨 Database waluigi.db non trovato in questa cartella.")
        return
    
    db = WaluigiDB(db_path)

       
    if args.command == "list":
        tasks = db.list_tasks()
        print(f"{'JOB ID':<15} | {'TASK ID':<35} | {'STATO'}")
        print("-" * 80)
        for t in tasks:
            color = "\033[92m" if t[2] == "SUCCESS" else "\033[93m" if t[2] == "RUNNING" else "\033[91m"
            reset = "\033[0m"
            print(f"{t[0]:<40} | {color}{t[2]:<10}{reset} | {t[3]}")

    elif args.command == "reset":
        if db.reset_task(args.task_id):
            print(f"✅ Task {args.task_id} resettato. Ora puoi rieseguirlo.")
        else:
            print(f"❓ Task {args.task_id} non trovato nel database.")
    
    elif args.command == "reset-job":
        # Chiamata al metodo di cancellazione del DB
        count = db.reset_tasks_by_job(args.job_id)
        print(f"💣 Eliminati {count} task relativi al job {args.job_id}.")
        
if __name__ == "__main__":
    main()
