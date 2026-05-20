from waluigi.sdk.dataquality import DQManager

dq = DQManager(rules_path="./rules")

dq.describe_rule("expect_cf_birthdate_coherence")

dq.describe_rule("expect_column_values_to_be_in_set")