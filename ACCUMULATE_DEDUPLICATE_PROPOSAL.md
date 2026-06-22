# Proposta: `AccumulateDeduplicateDataset`
## Nuovo built-in Waluigi — fact table con dedup cross-day per stato

**Autore:** Francesco Delli Paoli — Do Different  
**Data:** 2026-06-22  
**Contesto:** Emerso dalla pipeline InRecruiting (namespace `universo`) dopo l'introduzione di `AccumulateDataset` e `UpsertDataset`

---

## Differenza da `AccumulateDataset`

| | `AccumulateDataset` | `AccumulateDeduplicateDataset` |
|---|---|---|
| Idempotenza stesso giorno | drop righe con `date==oggi` + `force=False` | dedup cross-day (copre anche questo caso) |
| Righe invariate tra un giorno e l'altro | rimangono duplicate nel gold | eliminate — una sola riga con la data della prima osservazione |
| Crescita del dataset | lineare (N righe/giorno) | cresce solo su cambiamenti reali di stato |
| Caso d'uso | snapshot giornaliero puro | funnel operativo / storico stati |

---

## Logica

```
df_today  = read_latest_version(input.dataset)
df_prev   = read_latest_version(output.dataset)   # None se primo run

df_all    = concat([df_prev, df_today])            # prev prima, oggi dopo

key_cols  = [c for c in df_all.columns if c != date_column]
df_gold   = df_all
              .sort_values(date_column)             # ordine cronologico
              .drop_duplicates(subset=key_cols, keep="first")
              .reset_index(drop=True)

write_version(output.dataset,
              metadata={date_column: today},
              inputs=[reader_today, reader_prev],
              force=False)
```

---

## Config YAML

```yaml
- id: accumulate_applications
  taskRef:
      name: AccumulateDeduplicateDataset
  config:
      input:
          dataset: "bronze/universo/applications_raw"
          source: *local
      output:
          dataset: "gold/universo/applications_all"
          source: *local
          format: "parquet"
          description: "Candidature con storico passaggi di stato"
      date_column: "date"    # default: "date"
      date_param:  "date"    # default: "date"
  resources:
      coin: 2
```

---

## Implementazione di riferimento (in produzione)

```python
import datetime
import pandas as pd
from waluigi.sdk.context import context
from waluigi.sdk.catalog import CatalogClient

DATE_STR = getattr(context.params, "date", None) or datetime.datetime.now().strftime("%Y-%m-%d")
catalog  = CatalogClient()

reader_today = catalog.read_dataset("bronze/universo/applications_raw")
df_today = reader_today.read()
print(f"Bronze oggi ({DATE_STR}): {len(df_today)} righe")

frames = [df_today]
reader_prev = None
try:
    reader_prev = catalog.read_dataset("gold/universo/applications_all")
    df_prev = reader_prev.read()
    frames = [df_prev, df_today]
    print(f"Gold precedente: {len(df_prev)} righe")
except Exception as e:
    print(f"Nessun gold precedente (primo run): {e}")

df_all = pd.concat(frames, ignore_index=True)
before = len(df_all)

# prev prima di oggi → sort garantisce che keep="first" conservi la data più antica
key_cols = [c for c in df_all.columns if c != "date"]
df_all = (df_all
          .sort_values("date")
          .drop_duplicates(subset=key_cols, keep="first")
          .reset_index(drop=True))

print(f"Dopo dedup: {before} → {len(df_all)} righe ({before - len(df_all)} duplicate rimosse)")

input_lineage = [{"dataset_id": r.dataset_id, "version": r.version}
                 for r in [reader_today, reader_prev] if r is not None]
handle = catalog.create_dataset(
    "gold/universo/applications_all", format="parquet", source_id="local",
    description="Candidature con storico passaggi di stato — una riga per stato unico"
)
with handle.create_version(metadata={"date": DATE_STR}, force=False, inputs=input_lineage) as w:
    w.write(df_all)
print("skipped" if w.skipped else f"Gold applications written: {len(df_all)} righe")
```

---

## Note implementative

1. `date_column` e `date_param` devono avere default `"date"` — compatibile con job che non li specificano
2. Il `concat` deve mettere **prev prima, oggi dopo** — così `sort_values` + `keep="first"` conserva sempre la data più antica per ogni stato
3. Il `try/except` sul read del gold precedente è normale al primo run — non è un errore
4. Il lineage deve registrare entrambi gli input: `reader_today` e `reader_prev` (quando esiste)
5. Il formato `inputs` nel commit è `[{"dataset_id": ..., "version": ...}]` — non oggetti reader (il SDK `0.1.0` non li serializza correttamente in JSON)
