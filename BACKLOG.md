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

## Da fare

### Medio termine

- [ ] `wlctl reset task --cascade` — resetta task + tutti i downstream senza toccare il job
- [ ] Uniformare nomi CLI vs API (`taskdefinitions` → `task-definitions`)

### Lungo termine

- [ ] Validazione params con JSON Schema a livello `JobDefinition`
- [ ] Connettore Google Sheets con OAuth2 Device Flow (headless/mobile)
- [ ] Rotazione automatica Secret (integrazione vault esterni)
- [ ] Interattività grafici — filtro serie nel browser
