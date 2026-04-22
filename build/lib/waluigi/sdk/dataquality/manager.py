class Manager:
    def __init__(self, rules_path: str):
        self.catalogue: Dict[str, RuleDefinition] = {}
        self._load_catalogue(rules_path)

    def _load_catalogue(self, path: str):
        """Carica le regole e i loro schemi di input."""
        for filename in os.listdir(path):
            if filename.endswith(".yaml"):
                rule_id = filename.replace(".yaml", "")
                with open(os.path.join(path, filename), 'r') as f:
                    self.catalogue[rule_id] = RuleDefinition(**yaml.safe_load(f))

    def get_rule_details(self, rule_id: str) -> Dict[str, Any]:
        """Metodo per l'utente: restituisce lo schema della regola."""
        rule = self.catalogue.get(rule_id)
        if not rule: return {"error": "Non trovata"}
        return {
            "id": rule_id,
            "description": rule.description,
            "required_inputs": rule.inputs_schema
        }

    def _execute_single_rule(self, exec_rule: RuleExecution, datasets: Dict[str, pd.DataFrame]):
        # 1. Recupero definizione
        definition = self.catalogue.get(exec_rule.rule_id)
        if not definition:
            raise ValueError(f"Regola {exec_rule.rule_id} assente nel catalogo.")

        # 2. VALIDAZIONE SCHEMA: La suite ha passato tutti gli input richiesti?
        missing = set(definition.inputs_schema.keys()) - set(exec_rule.inputs.keys())
        if missing:
            raise ValueError(f"Input mancanti per {exec_rule.rule_id}: {missing}")

        # 3. Binding e Mapping
        env = {}
        for placeholder in definition.inputs_schema.keys():
            path = exec_rule.inputs[placeholder]
            ds_name, col_name = path.split(".")
            env[placeholder] = datasets[ds_name][col_name]

        # 4. Esecuzione (Pandas Eval)
        mask = pd.eval(definition.formula, local_dict=env, engine='python')
        
        # ... (logica di calcolo score e failed_indices come visto prima)
        return mask
