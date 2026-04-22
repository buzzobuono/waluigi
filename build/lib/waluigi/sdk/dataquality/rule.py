import pandas as pd
from pydantic import BaseModel
from typing import List, Dict, Any

class Rule(BaseModel):
    name: str
    formula: str                  # Esempio: "x + y > z"
    inputs: Dict[str, str]        # Mappa placeholder -> "dataset.colonna"
    tolerance: float = 1.0

    def execute(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        # 1. Creiamo il namespace usando i placeholder come chiavi
        # Questo rende la formula indipendente dai nomi dei dataset originali
        env = {}
        
        for placeholder, path in self.inputs.items():
            ds_name, col_name = path.split(".")
            
            if ds_name not in datasets:
                raise KeyError(f"Dataset '{ds_name}' non trovato.")
            
            # Assegniamo la Series del dataset al nome del placeholder (x, y, ecc.)
            env[placeholder] = datasets[ds_name][col_name]

        # 2. Valutazione dell'espressione
        try:
            # L'espressione vede solo 'x', 'y', ecc.
            mask = pd.eval(self.formula, local_dict=env, engine='python')
            
            # Gestione check aggregativo o row-level
            if isinstance(mask, (bool, int, float, complex)):
                score = 1.0 if mask else 0.0
                is_row_level = False
            else:
                score = mask.mean()
                is_row_level = True
                
        except Exception as e:
            return {"name": self.name, "success": False, "error": f"Errore formula: {e}"}

        return {
            "name": self.name,
            "success": bool(score >= self.tolerance),
            "score": float(score),
            "failed_indices": mask[~mask].index.tolist() if is_row_level and score < 1.0 else []
        }
