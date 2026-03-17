import os
import subprocess
import time
from waluigi.core.task import Task

NAMESPACE = "file_pipeline"

class FileSourceReader(Task):
    id = "file_reader"
    namespace = NAMESPACE

    def run(self):
        source_file = self.params.filename
        output_file = f"read_{source_file}.tmp"
        cmd = f"cat {source_file} > {output_file}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Errore lettura {source_file}: {result.stderr}")

class FileDiscoveryTask(Task):
    id = "file_discovery"
    namespace = NAMESPACE

    def requires(self):
        target_files = ["data_1.txt", "data_2.txt", "data_3.txt"]
        dependencies = []
        
        for fname in target_files:
            print(fname)
            if os.path.exists(fname):
                mtime = time.ctime(os.path.getmtime(fname))
                dependencies.append(
                    FileSourceReader(
                        tags=[fname],
                        params={
                            "filename": fname,
                            "last_modified": mtime
                        }
                    )
                )
        return dependencies

    def run(self):
        print(f"Discovery completata. Processati {len(self.params.files)} file.")

class FinalAggregator(Task):
    id = "final_aggregator"
    namespace = NAMESPACE

    def requires(self):
        return [
            FileDiscoveryTask(
                params={"files": ["data_1.txt", "data_2.txt", "data_3.txt"]}
            )
        ]

    def run(self):
        output_final = "collection_final.out"
        cmd = "cat read_*.tmp > " + output_final
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Aggregazione completata in {output_final}")
