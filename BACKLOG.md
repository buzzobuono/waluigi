# Waluigi — Backlog

## Fatto

- [x] `kind: Source` supportato da `wlctl apply`
- [x] Coercizione automatica tipi misti in scrittura Parquet (warning)
- [x] `sort` e `sort_field` sull'asse X dei grafici
- [x] Grafici `combo` (barre + linea, asse Y duale)
- [x] `inputs=[reader]` in `create_version` accetta `DatasetReader`
- [x] `ReindexTimeSeries` con `group_by` opzionale (cross-product gruppo × periodi)
- [x] `wlctl apply-builtins -n <ns>` — registra built-in da bundle interno al pacchetto
- [x] `AddDerivedColumns`: expr pandas completo (`x`=DataFrame) + mapping dizionario
- [x] `TransformDataset`: eval block su `df` con I/O e lineage automatici
- [x] `LastPerGroup` / `FirstPerGroup`
- [x] `wl-cluster.sh` nella skill per gestione cluster locale
- [x] `SendGmail` built-in Google vendor + `wlctl apply-builtins -n <ns> google`
- [x] `SharePointExport` spostato da core a vendor Microsoft (`wlctl apply-builtins -n <ns> microsoft`)
- [x] `kind: Chart` in `wlctl apply` — full replace chart definitions su un dataset da YAML
- [x] `IngestGoogleSheet` built-in Google vendor (parametric: skip_rows, key_column, coercion, multi-sheet)
  - **limite**: solo fogli pubblici ("chiunque con il link") — fogli privati → OAuth2 Device Flow (vedi backlog lungo termine)
  - **limite**: nessun supporto range nominato o notazione A1
  - **limite**: nessuna idempotency reale quando `force=true` — scrive sempre una nuova versione

## Da fare

### Medio termine

- [ ] `wlctl reset task --cascade` — resetta task + tutti i downstream senza toccare il job
- [ ] Uniformare nomi CLI vs API (`taskdefinitions` → `task-definitions`)

### Lungo termine

- [ ] Validazione params con JSON Schema a livello `JobDefinition`
- [ ] Connettore Google Sheets con OAuth2 Device Flow (headless/mobile)
- [ ] Rotazione automatica Secret (integrazione vault esterni)
- [ ] Interattività grafici — filtro serie nel browser
