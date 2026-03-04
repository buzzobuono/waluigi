import argparse
import os
from waluigi.core.db import WaluigiDB

def main():
    parser = argparse.ArgumentParser(description="🟣 Waluigi CLI - Gestione Pipeline")
    subparsers = parser.add_subparsers(dest="command", help="Comandi disponibili")

    # Comando LIST
    subparsers.add_parser("list", help="Elenca tutti i task e il loro stato")

    # Comando RESET (Singolo Task)
    reset_parser = subparsers.add_parser("reset", help="Elimina un task dal DB per rifarlo")
    reset_parser.add_argument("id", help="L'ID del task (es. Dep1Task_id-aaa)")

    # Comando RESET-JOB (Intero Gruppo)
    job_reset_parser = subparsers.add_parser("reset-job", help="Resetta tutti i task di un Job")
    job_reset_parser.add_argument("namespace", help="L'ID del job (es. job_174000000)")

    args = parser.parse_args()
    
    # Inizializza DB
    db_path = os.path.join(os.getcwd(), "waluigi.db")
    if not os.path.exists(db_path):
        print("🚨 Database waluigi.db non trovato in questa cartella.")
        return
    
    db = WaluigiDB(db_path)

    if args.command == "list":
        # list_tasks() restituisce: (id, namespace, name, status, last_update)
        tasks = db.list_tasks()
        
        print(f"\n{'JOB ID':<18} | {'TASK NAME':<25} | {'STATUS':<10} | {'TASK ID'} | {'PARENT TASK ID'}")
        print("-" * 100)
        
        for t in tasks:
            t_id, j_id, name, status, last_update, parent_id = t
            
            # Colori ANSI
            color = "\033[92m" if status == "SUCCESS" else "\033[93m" if status == "RUNNING" else "\033[91m"
            reset = "\033[0m"
            
            # Gestione Job ID nullo
            display_job = j_id if j_id else "None"
            
            print(f"{display_job:<18} | {name:<25} | {color}{status:<10}{reset} | {t_id} | {parent_id}")
        print("-" * 100 + "\n")

    elif args.command == "reset":
        if db.reset_task(args.id):
            print(f"✅ Task {args.id} resettato con successo.")
        else:
            print(f"❓ Task {args.id} non trovato.")
    
    elif args.command == "reset-job":
        count = db.reset_tasks_by_job(args.namespace)
        if count > 0:
            print(f"💣 Boom! Eliminati {count} task per il job {args.namespace}.")
        else:
            print(f"❓ Nessun task trovato per il job {args.namespace}.")

if __name__ == "__main__":
    main()
