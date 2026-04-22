from waluigi.sdk.dataquality import DQManager

def _model_dump(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    else:
        return obj.dict(exclude_none=True)

dq = DQManager(rules_path="./rules")

dq.describe_rule("expect_cf_birthdate_coherence")

dq.describe_rule("expect_column_values_to_be_in_set")