import os
import yaml
import logging

from waluigi.catalog.db import CatalogDB
from waluigi.sdk.dataquality import DQManager

logger = logging.getLogger("waluigi")


class DQService:

    def __init__(self, db: CatalogDB, dq_manager: DQManager):
        self.db         = db
        self.dq_manager = dq_manager

    def run_on_commit(self, dataset_id: str, version: str,
                      connector, location: str, fmt: str,
                      expectations: list) -> dict | None:
        """Run DQ expectations against a committed dataset version.

        Never raises — writes the result (or the error) to dq_results and
        returns the saved row, or None when there are no expectations.
        """
        try:
            if not expectations:
                return None
            df     = connector.read(location, fmt)
            result = self.dq_manager.run_from_db(expectations, {"this": df})
            details = [
                {"rule_id": r.rule_id, "success": r.success,
                 "score": r.score, "error": r.error}
                for r in result.results
            ]
            row = self.db.save_dq_result(
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
            self.db.save_dq_result(
                dataset_id, version,
                score=0.0, passed=0, total=0, success=False,
                details=[], error=str(e),
            )
            return None

    def get_suite(self, path: str) -> list:
        """Read a DQ suite YAML and enrich with catalogue definitions.

        Raises ValueError if the file is not found or cannot be parsed.
        """
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
        """Reload the rules catalogue from disk and return a serialisable list."""
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
