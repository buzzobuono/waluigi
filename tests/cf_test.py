import pandas as pd
from waluigi.sdk.dataquality import DQManager
import json

def _model_dump(obj):
    if hasattr(obj, "model_dump"):
        return obj.model_dump(exclude_none=True)
    else:
        return obj.dict(exclude_none=True)

data = {
    "cf": [
        "MRNRSS85M01H501Z",
        "RSSMRA85A41H501W",
        "LGRGSS90A01H501X",
        "NNNRSS85T41H501Y"
    ],
    "nascita": ["1985-08-01", "1985-01-01", "1990-01-15", "1985-05-01"],
    "genere": ["M", "F", "M", "F"]
}

df = pd.DataFrame(data)
df["nascita"] = pd.to_datetime(df["nascita"])

datasets = {"utenti": df}

dq = DQManager(rules_path="./rules")
report = dq.run_suite("./test/cf_test.yaml", datasets)
dq.print_report(report)


#print(json.dumps(_model_dump(report), indent=2))
