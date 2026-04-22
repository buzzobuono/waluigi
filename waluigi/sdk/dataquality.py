import os
import re
import ast
import yaml
import pandas as pd
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional


# ── Models ────────────────────────────────────────────────────────────────────

class RuleDefinition(BaseModel):
    formula: str
    inputs_schema: Dict[str, str]
    params_schema: Optional[Dict[str, str]] = {}
    description: str = ""

class RuleExecution(BaseModel):
    rule_id: str
    inputs: Dict[str, str]
    params: Optional[Dict[str, Any]] = {}
    tolerance: float = Field(default=1.0, ge=0.0, le=1.0)

class RuleResult(BaseModel):
    rule_id: str
    success: bool
    score: Optional[float] = None
    failed_indices: List[Any] = []
    error: Optional[str] = None

class SuiteResult(BaseModel):
    suite_path: str
    total: int
    passed: int
    failed: int
    success: bool                  # True solo se TUTTI passano
    results: List[RuleResult]

    @property
    def score(self) -> float:
        return round(self.passed / self.total, 4) if self.total else 0.0
        
# ── Formula Safety ────────────────────────────────────────────────────────────

_SAFE_BUILTINS = {
    "int", "float", "str", "bool",
    "abs", "round", "len", "min", "max",
}

_SAFE_AST_NODES = {
    # Struttura
    ast.Expression, ast.Expr,
    # Operatori booleani e comparatori
    ast.BoolOp, ast.And, ast.Or, ast.Not,
    ast.Compare, ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
    ast.In, ast.NotIn,
    # Operatori aritmetici
    ast.BinOp, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.FloorDiv,
    ast.UnaryOp, ast.USub, ast.UAdd,
    # Operatori bitwise — usati da pandas per & | ^ tra Series booleane
    ast.BitAnd, ast.BitOr, ast.BitXor, ast.Invert,
    # Literals e nomi
    ast.Constant, ast.Name, ast.Load,
    # Accesso attributi e chiamate
    ast.Attribute, ast.Call,
    # Strutture dati
    ast.List, ast.Tuple, ast.Dict,
    # Subscript e slicing
    ast.Subscript, ast.Index, ast.Slice,
}

def _check_formula_safety(formula: str, allowed_names: set) -> None:
    try:
        tree = ast.parse(formula, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Sintassi non valida nella formula: {e}")

    for node in ast.walk(tree):
        if type(node) not in _SAFE_AST_NODES:
            raise ValueError(
                f"Costrutto non permesso nella formula: {type(node).__name__}"
            )
        if isinstance(node, ast.Name) and node.id not in allowed_names | _SAFE_BUILTINS:
            raise ValueError(
                f"Nome '{node.id}' non dichiarato in inputs_schema o params_schema"
            )

# ── Engine ────────────────────────────────────────────────────────────────────

class DQManager:
    def __init__(self, rules_path: str):
        self.rules_path = rules_path
        self.catalogue: Dict[str, RuleDefinition] = {}
        self._startup()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def _startup(self):
        for filename in os.listdir(self.rules_path):
            if filename.endswith(".yaml"):
                rule_id = os.path.splitext(filename)[0]
                filepath = os.path.join(self.rules_path, filename)
                with open(filepath, "r") as f:
                    data = yaml.safe_load(f)
                try:
                    self.catalogue[rule_id] = RuleDefinition(**data)
                except Exception as e:
                    print(f"⚠️  Skipping {filename}: {e}")
        
    # ── Public API ────────────────────────────────────────────────────────────

    def run_suite(
        self,
        suite_path: str,
        datasets: Dict[str, pd.DataFrame],
    ) -> SuiteResult:
        with open(suite_path, "r") as f:
            raw = yaml.safe_load(f)
    
        results = []
        for item in raw:
            try:
                exec_r = RuleExecution(**item)
            except Exception as e:
                results.append(
                    RuleResult(rule_id=item.get("rule_id", "?"), success=False, error=str(e))
                )
                continue
            results.append(self._execute(exec_r, datasets))
    
        passed = sum(1 for r in results if r.success)
    
        return SuiteResult(
            suite_path=suite_path,
            total=len(results),
            passed=passed,
            failed=len(results) - passed,
            success=passed == len(results),
            results=results,
        )

    def get_rule(self, rule_id: str) -> RuleDefinition:
        """Recupera la definizione completa di una regola dal catalogo."""
        rule = self.catalogue.get(rule_id)
        if not rule:
            raise KeyError(f"Regola '{rule_id}' non trovata nel catalogo")
        return rule
    
    def describe_rule(self, rule_id: str) -> None:
        """Stampa in modo leggibile la definizione di una regola."""
        rule = self.get_rule(rule_id)
        print(f"Rule ID           : {rule_id}")
        print(f"Rule Description  : {rule.description or '—'}")
        print(f"Rule Formula      : {rule.formula.strip()}")
        print(f"Input Schema      :")
        for name, desc in rule.inputs_schema.items():
            print(f"     {name:<15} → {desc}")
        if rule.params_schema:
            print(f"Rule Parameters    :")
            for name, desc in rule.params_schema.items():
                print(f"     {name:<15} → {desc}")
    
    def list_rules(self) -> None:
        """Elenca tutte le regole disponibili nel catalogo."""
        print(f"Available Rules ({len(self.catalogue)})\n")
        for rule_id, rule in self.catalogue.items():
            print(f"- {rule_id}: {rule.description or '—'}\n")
                
    def print_report(self, report) -> None:
        overall = f"✅ PASSED" if report.success else "❌ FAILED"
        print(f"\nSuite Status: {overall}\nScore: {report.score*100:.1f}% ({report.passed}/{report.total} passed rules)")
        
        # Dettaglio per regola
        print("Rule details:")
        for r in report.results:
            status = "✅ PASSED" if r.success else "❌ FAILED"
            score  = (r.score or 0) * 100
            print(f"- {r.rule_id}\n\tStatus: {status}\n\tScore: {score:>6.2f}%")
            if not r.success:
                if r.error:
                    print(f"\tError: {r.error}")
                else:
                    print(f"\tFailed indices: {r.failed_indices}")
        print("")        


    # ── Internals ─────────────────────────────────────────────────────────────

    def _bind_inputs(
        self,
        exec_r: RuleExecution,
        definition: RuleDefinition,
        datasets: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        env: Dict[str, Any] = {}

        for placeholder in definition.inputs_schema:
            mapping = exec_r.inputs.get(placeholder)
            if not mapping:
                raise KeyError(f"Nessun mapping per il placeholder '{placeholder}'")
            parts = mapping.split(".", 1)
            if len(parts) != 2:
                raise ValueError(f"Input '{mapping}' deve essere nel formato 'dataset.colonna'")
            ds_name, col_name = parts
            if ds_name not in datasets:
                raise KeyError(f"Dataset '{ds_name}' non trovato (disponibili: {list(datasets)})")
            df = datasets[ds_name]
            if col_name not in df.columns:
                raise KeyError(f"Colonna '{col_name}' non trovata nel dataset '{ds_name}'")
            env[placeholder] = df[col_name]

        for p_name in (definition.params_schema or {}):
            if p_name not in (exec_r.params or {}):
                raise KeyError(f"Parametro obbligatorio mancante: '{p_name}'")
            env[p_name] = exec_r.params[p_name]

        return env

    def _eval_formula(self, formula, env, definition):
        declared_names = set(definition.inputs_schema) | set(definition.params_schema or {})
        _check_formula_safety(formula, declared_names)
    
        safe_env = {
            # builtin sicuri resi disponibili all'eval
            "int": int, "float": float, "str": str, "bool": bool,
            "abs": abs, "round": round, "len": len, "min": min, "max": max,
            **env,
        }
    
        return eval(
            compile(formula.strip(), "<dq_formula>", "eval"),
            {"__builtins__": {}},
            safe_env,
        )

    def _execute(
        self,
        exec_r: RuleExecution,
        datasets: Dict[str, pd.DataFrame],
    ) -> RuleResult:
        definition = self.catalogue.get(exec_r.rule_id)
        if not definition:
            return RuleResult(
                rule_id=exec_r.rule_id,
                success=False,
                error=f"Regola '{exec_r.rule_id}' non trovata nel catalogo",
            )

        try:
            env = self._bind_inputs(exec_r, definition, datasets)
        except (KeyError, ValueError) as e:
            return RuleResult(rule_id=exec_r.rule_id, success=False, error=str(e))

        try:
            mask = self._eval_formula(definition.formula, env, definition)
        except Exception as e:
            return RuleResult(rule_id=exec_r.rule_id, success=False, error=f"Errore formula: {e}")

        is_series = isinstance(mask, pd.Series)
        score = float(mask.mean()) if is_series else (1.0 if bool(mask) else 0.0)
        failed = mask[~mask].index.tolist() if is_series and score < 1.0 else []

        return RuleResult(
            rule_id=exec_r.rule_id,
            success=score >= exec_r.tolerance,
            score=round(score, 4),
            failed_indices=failed,
        )