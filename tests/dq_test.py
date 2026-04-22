import pandas as pd
import json
from waluigi.sdk.dataquality import DQManager # Assumi che il codice del manager sia in manager.py

def _model_dump(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    else:
        return obj.dict(exclude_none=True)
        
df_ordini = pd.DataFrame({
    "id_prodotto": [1, 2, 3, 4, 5],
    "prezzo":      [150, 1000, 300, 250, 50],
    "stato":       ["SPEDITO", "IN_CORSO", "ERROR", "SPEDITO", "ANNULLATO"],
})
df_catalogo = pd.DataFrame({
    "id":   [1, 2, 3, 4],
    "nome": ["Prodotto A", "Prodotto B", "Prodotto C", "Prodotto D" ],
})

datasets = {"ordini": df_ordini, "catalogo": df_catalogo}
    
dq = DQManager(rules_path="./rules")
report = dq.run_suite("./tests/dq_test.yaml", datasets)
dq.print_report(report)

#print(json.dumps(_model_dump(report), indent=2))
