import os
import yaml
import logging

from waluigi.catalog.repositories.dataset_repo import DatasetRepository
from waluigi.catalog.repositories.dq_result_repo import DQResultRepository
from waluigi.catalog.repositories.expectation_repo import ExpectationRepository
from waluigi.sdk.dataquality import DQManager

logger = logging.getLogger("waluigi")


class DQService:

    def __init__(self, datasets: DatasetRepository,
                 dq_results: DQResultRepository,
                 expectations: ExpectationRepository,
                 dq_manager: DQManager):
        self.datasets      = datasets
        self.dq_results    = dq_results
        self.expectations  = expectations
        self.dq_manager    = dq_manager

    def run_on_commit(self, dataset_id: str, version: str,
                      connector, location: str, fmt: str,
                      expectations: list) -> dict | None:
        try:
            if not expectations:
                return None
            df       = connector.read(location, fmt)
            exp_data = [e.to_dict() if hasattr(e, "to_dict") else e
                        for e in expectations]
            result   = self.dq_manager.run_from_db(exp_data, {"this": df})
            details = [
                {"rule_id": r.rule_id, "success": r.success,
                 "score": r.score, "error": r.error}
                for r in result.results
            ]
            row = self.dq_results.save(
                dataset_id, version,
                score=result.score, passed=result.passed,
                total=result.total, success=result.success,
                details=details,
            )
            logger.info(
                f"DQ {dataset_id}@{version}: "
                f"score={result.score:.2%} ({result.passed}/{result.total})"
            )
            return row
        except Exception as e:
            logger.warning(f"DQ run skipped for {dataset_id}@{version}: {e}")
            self.dq_results.save(
                dataset_id, version,
                score=0.0, passed=0, total=0, success=False,
                details=[], error=str(e),
            )
            return None

    def get_suite(self, path: str) -> list:
        if not os.path.isfile(path):
            raise ValueError(f"Suite file not found: {path}")
        try:
            with open(path, "r") as f:
                raw = yaml.safe_load(f) or []
        except Exception as e:
            raise ValueError(f"Cannot read suite file: {e}")
        enriched = []
        for item in raw:
            rule_id = item.get("rule_id", "?")
            defn    = self.dq_manager.catalogue.get(rule_id)
            enriched.append({
                "rule_id":     rule_id,
                "inputs":      item.get("inputs", {}),
                "params":      item.get("params", {}),
                "tolerance":   item.get("tolerance", 1.0),
                "description": defn.description if defn else None,
                "formula":     defn.formula.strip() if defn else None,
                "found":       defn is not None,
            })
        return enriched

    def list_rules(self) -> list:
        self.dq_manager._startup()
        return [
            {
                "id":            rule_id,
                "description":   rule.description,
                "formula":       rule.formula.strip(),
                "inputs_schema": rule.inputs_schema,
                "params_schema": rule.params_schema or {},
            }
            for rule_id, rule in sorted(self.dq_manager.catalogue.items())
        ]

    def list_results(self, dataset_id: str) -> list:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        return self.dq_results.list(dataset_id)

    def get_result(self, dataset_id: str, version: str) -> dict:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        row = self.dq_results.get(dataset_id, version)
        if not row:
            raise ValueError("No DQ result for this version")
        return row

    def list_expectations(self, dataset_id: str) -> list:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        return self.expectations.list(dataset_id)

    def add_expectation(self, dataset_id: str, rule_id: str, inputs: dict,
                        params: dict, tolerance: float, position: int) -> dict:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        return self.expectations.add(dataset_id, rule_id, inputs, params,
                                     tolerance, position)

    def update_expectation(self, dataset_id: str, exp_id: int, **updates) -> dict:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        if not self.expectations.update(dataset_id, exp_id, **updates):
            raise ValueError("Expectation not found")
        return self.expectations.get(dataset_id, exp_id)

    def delete_expectation(self, dataset_id: str, exp_id: int) -> dict:
        if not self.datasets.exists(dataset_id):
            raise ValueError("Dataset not found")
        if not self.expectations.delete(dataset_id, exp_id):
            raise ValueError("Expectation not found")
        return {"deleted": exp_id}
