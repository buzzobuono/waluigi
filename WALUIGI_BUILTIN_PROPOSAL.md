# Proposta: Nuovi Built-in Task per Waluigi
## Pattern Incrementali per Architettura Medallion

**Autore:** Francesco Delli Paoli — Do Different  
**Data:** 2026-06-18  
**Contesto:** Emerso dalla produzione della pipeline InRecruiting (namespace `universo`)

---

## Motivazione

Nel costruire una pipeline Bronze→Silver→Gold con esecuzione giornaliera su CronJob, si sono manifestati due pattern che si ripetono identici in ogni pipeline incrementale, ma che non trovano copertura nei built-in attuali di Waluigi:

1. **Tabelle di fatto** (eventi, transazioni, candidature): ogni giorno si aggiunge un nuovo snapshot al dataset accumulato. La logica è sempre la stessa: leggi il gold precedente, rimuovi le righe di oggi (idempotenza), appendi il bronze di oggi, scrivi la nuova versione gold.

2. **Tabelle dimensione** (anagrafica clienti, prodotti, team): ogni giorno i dati possono aggiornare record esistenti. La logica è: leggi il gold precedente, concat con i nuovi dati, deduplicazione per chiave business (`keep="last"`), scrivi la nuova versione gold.

Entrambi i pattern sono stati implementati come `TaskDefinition` custom (script Python inline). Sono corretti e in produzione, ma ogni team che costruisce una pipeline giornaliera li riscriverà da zero, con rischio di regressioni sull'idempotenza e sul lineage.

L'obiettivo di questa proposta è formalizzarli come **built-in di prima classe**, affiancando `MergeDatasets`, `DeduplicateDataset` e `AggregateDataset` che già esistono.

Un terzo built-in, `IngestRestOAuth2`, chiude un gap nell'ingestion: `IngestRest` esiste, ma non gestisce il flusso OAuth2 client_credentials che è lo standard de-facto delle API B2B moderne.

---

## Proposta 1 — `AccumulateDataset`

### Descrizione

Implementa il pattern **append-only con idempotenza per data**: ogni run aggiunge le righe del giorno corrente al dataset accumulato, garantendo che eseguire il job due volte sullo stesso giorno non duplichi i dati.

È il built-in naturale per tutte le tabelle di fatto in architettura medallion con trigger giornaliero.

### Logica interna (pseudocodice)

```
gold_prev  = read_latest_version(output.dataset)   # None se primo run
df_today   = read_latest_version(input.dataset)

if gold_prev is not None:
    df_prev_clean = gold_prev[ gold_prev[date_column] != param[date_param] ]
else:
    df_prev_clean = empty_dataframe

df_gold = concat([df_prev_clean, df_today])

write_version(output.dataset,
              metadata={date_column: param[date_param]},
              force=False,           # idempotente anche a livello di versione Catalog
              inputs=[reader_today, reader_prev])
```

### Configurazione YAML

```yaml
- id: accumulate_orders
  taskRef:
    name: AccumulateDataset
  config:
    input:
      dataset: "bronze/myapp/orders_raw"
      source: *local
    output:
      dataset: "gold/myapp/orders_all"
      source: *local
      format: "parquet"
      description: "Ordini accumulati — tutti i giorni"
    date_column: "date"     # nome della colonna data nel dataframe (default: "date")
    date_param:  "date"     # nome del param job da usare come valore di oggi (default: "date")
  resources:
    coin: 2
  requires:
    - bronze_ingest
```

### Comportamento dettagliato

| Scenario | Comportamento atteso |
|---|---|
| Primo run (nessun gold precedente) | Scrive solo le righe di `input` come prima versione gold |
| Run normale (gold precedente esiste) | Rimuove righe con `date_column == date_param`, appende `input`, scrive nuova versione |
| Run ripetuto stesso giorno | `force=False` su `create_version` skippa la scrittura (versione con stessa metadata già esistente) |
| `input` dataset vuoto | Scrive comunque una versione gold (con solo storico, senza righe di oggi) — non fallisce |
| `date_column` non presente nell'`input` | Il task aggiunge automaticamente la colonna con il valore di `date_param` |

### Note implementative

- Il lineage deve registrare **entrambi** gli input: `input.dataset` (bronze oggi) e `output.dataset` versione precedente (gold ieri). Il Catalog supporta già input multipli in `create_version(inputs=[...])`.
- Il `force=False` sulla scrittura della versione gold è fondamentale: se il CronJob o un operatore riavvia il job nello stesso giorno, il task deve essere idempotente senza richiedere intervento manuale.
- La gestione del "gold precedente non trovato" deve essere `try/except` silenzioso — è una condizione normale al primo run, non un errore.
- Tipo del `date_column` nel gold accumulato: stringa ISO `YYYY-MM-DD`. Il task non deve forzare conversioni di tipo sull'intero dataframe, solo aggiungere/verificare la colonna di partizione.

### Casi d'uso reali

- Candidature giornaliere (InRecruiting — questo progetto)
- Log di accesso / eventi applicativi
- Ordini e transazioni e-commerce
- Metriche di monitoring (serie temporali a granularità giornaliera)
- Export giornalieri da CRM/ERP

---

## Proposta 2 — `UpsertDataset`

### Descrizione

Implementa il pattern **SCD Type 1** (Slowly Changing Dimension, Type 1): ogni run aggiorna il dataset accumulato con i nuovi dati, mantenendo sempre l'ultima versione nota per ogni chiave business. Le versioni precedenti non vengono storicizzate — vengono sovrascritte.

È il built-in naturale per tutte le tabelle dimensione in architettura medallion.

### Logica interna (pseudocodice)

```
gold_prev = read_latest_version(output.dataset)   # None se primo run
df_today  = read_latest_version(input.dataset)

if gold_prev is not None:
    df_all = concat([gold_prev, df_today])
else:
    df_all = df_today

df_gold = df_all.drop_duplicates(subset=key, keep="last").reset_index(drop=True)

write_version(output.dataset,
              metadata={date_param: param[date_param]},
              force=False,
              inputs=[reader_today, reader_prev])
```

### Configurazione YAML

```yaml
- id: upsert_clienti
  taskRef:
    name: UpsertDataset
  config:
    input:
      dataset: "bronze/myapp/clienti_raw"
      source: *local
    output:
      dataset: "gold/myapp/clienti"
      source: *local
      format: "parquet"
      description: "Anagrafica clienti — versione più recente per cliente"
    key:
      - "IdCliente"        # lista per supportare chiavi composte
    date_param: "date"     # param job usato come metadata di versione (default: "date")
  resources:
    coin: 1
  requires:
    - bronze_ingest
```

### Comportamento dettagliato

| Scenario | Comportamento atteso |
|---|---|
| Primo run | Scrive `input` as-is dopo deduplicazione interna per `key` |
| Run normale | Concat gold precedente + oggi → dedup `keep="last"` → nuova versione gold |
| Chiave non trovata nel dataframe | Il task fallisce con errore esplicito: `KeyError: column 'X' not found for upsert key` |
| Chiave composta (lista di colonne) | `drop_duplicates(subset=["col_a", "col_b"], keep="last")` |
| Run ripetuto stesso giorno | `force=False` skippa la scrittura |
| Nuovo record (chiave non esistente nel gold) | Viene aggiunto normalmente |
| Record esistente con dati aggiornati | Sovrascrive il record precedente (`keep="last"`) |
| Record rimosso dall'API (non più presente nell'`input`) | **Rimane nel gold** — il task non cancella, solo aggiorna/aggiunge |

### Note implementative

- L'ordine del `concat` è deliberato: `[gold_prev, df_today]`. Con `keep="last"`, le righe di `df_today` vincono sempre su quelle di `gold_prev` in caso di chiave duplicata.
- Per supportare "cancellazione logica" futura, si potrebbe aggiungere un parametro opzionale `soft_delete_column` (es. `is_deleted`) — non incluso in questa proposta base.
- Il `key` deve accettare sia stringa singola (per compatibilità con l'uso semplice) sia lista.

### Casi d'uso reali

- Anagrafica clienti / fornitori (questo progetto: `clienti`, `commesse`)
- Catalogo prodotti
- Team e utenti di sistema
- Tabelle di codifica / lookup
- Configurazioni applicative versionabili

---

## Proposta 3 — `IngestRestOAuth2`

### Descrizione

Estensione di `IngestRest` con supporto nativo per autenticazione **OAuth2 client_credentials flow**. Gestisce autonomamente il token refresh e il retry su rate-limit (HTTP 402/429).

`IngestRest` esiste ed è ottimo per API pubbliche. Il gap è che la maggior parte delle API B2B aziendali (Salesforce, HubSpot, qualsiasi ATS/CRM enterprise) usa OAuth2 con client ID e secret. Oggi questo richiede uno script custom di 30+ righe che ogni team replica.

### Configurazione YAML

```yaml
- id: fetch_orders
  taskRef:
    name: IngestRestOAuth2
  config:
    auth:
      token_url:     "${WALUIGI_SECRET_API_BASE_URL}/oauth2/token"
      client_id:     "${WALUIGI_SECRET_API_CLIENT_ID}"
      client_secret: "${WALUIGI_SECRET_API_CLIENT_SECRET}"
      grant_type:    "client_credentials"      # unico grant type supportato in questa proposta
    http:
      url:    "${WALUIGI_SECRET_API_BASE_URL}/api/v3/orders"
      params:
        status: "active"
      response_key: "orders"    # path JSON da cui estrarre la lista (opzionale; null = usa root)
      retry_on:   [402, 429]
      retry_wait: 60
      retries:    10
    output:
      dataset: "bronze/myapp/orders_raw"
      source:  *local
      format:  "parquet"
      description: "Ordini raw dall'API"
      static:  false    # true → force=False con metadata senza date (per dati statici)
  resources:
    coin: 1
  requires:
    - setup_source
```

### Comportamento dettagliato

| Parametro | Descrizione |
|---|---|
| `auth.token_url` | Endpoint POST per ottenere il Bearer token |
| `auth.client_id/secret` | Riferimenti a secret Waluigi (interpolati a runtime) |
| `auth.grant_type` | Solo `client_credentials` in v1 |
| `http.response_key` | Chiave JSON da cui estrarre la lista di oggetti (es. `"orders"`, `"items"`). Se omesso, si usa la root. Supporta dot-notation per path annidati (`"data.results"`) |
| `http.retry_on` | Lista di status code che triggerano un retry con wait (default: `[429]`) |
| `http.retry_wait` | Secondi di attesa prima del retry (default: `60`) |
| `http.retries` | Numero massimo di tentativi (default: `10`) |
| `output.static` | Se `true`, scrive con `force=False` e metadata senza data — per dati di configurazione che non cambiano giornalmente |

### Note implementative

- Il token deve essere ottenuto **una sola volta** per esecuzione e riutilizzato per tutte le chiamate. Non serve refresh mid-execution (i token OAuth2 client_credentials hanno tipicamente TTL 1h+).
- L'autenticazione fallisce con errore esplicito se la risposta del token endpoint non è HTTP 200, riportando status code e body.
- Per API paginate, questa proposta non include la paginazione (che richiederebbe config più complessa). Il caso paginato rimane per uno script custom o una futura `IngestRestOAuth2Paginated`.

---

## Confronto con built-in esistenti

| Built-in esistente | Copre | Gap |
|---|---|---|
| `MergeDatasets` | Concat verticale N dataset | Non legge/scrive dal gold — non gestisce idempotenza per data |
| `DeduplicateDataset` | Dedup su un dataset | Non fa il read-back del gold precedente |
| `MergeDatasets` + `DeduplicateDataset` | Insieme si avvicinano a `UpsertDataset` | Richiedono 2 task + un dataset intermedio + nessuna gestione automatica del lineage gold-prev |
| `IngestRest` | REST API pubblica | Nessun OAuth2, nessun retry su rate-limit |
| `FilterDataset` | Filtra righe | Non aggiunge colonne derivate da param di job |

I nuovi built-in non sostituiscono quelli esistenti: li completano coprendo i pattern incrementali che oggi richiedono script custom.

---

## Priorità suggerita

| Priorità | Built-in | Motivazione |
|---|---|---|
| **1** | `AccumulateDataset` | Usato in ogni pipeline con tabelle di fatto giornaliere — massima ricorrenza |
| **2** | `UpsertDataset` | Usato in ogni pipeline con dimensioni — complementare al precedente |
| **3** | `IngestRestOAuth2` | Alta utilità ma parzialmente sostituibile con script custom di poche righe |

---

## Implementazione di riferimento

Il codice di riferimento per questi pattern è in produzione nel namespace `universo` del cluster Waluigi:

- `AccumulateDataset` → `InrecruitingGoldApplications` in `descriptors/task-definitions.yaml` (righe 422–455)
- `UpsertDataset` → `InrecruitingGoldCommesse` e `InrecruitingGoldClienti` (righe 460–549)
- `IngestRestOAuth2` → `InrecruitingFetchUsers`, `InrecruitingFetchTeams`, `InrecruitingFetchCodeTables` (righe 43–230)

Il codice è funzionante e ha superato un test end-to-end su due giorni consecutivi (2026-06-17 e 2026-06-18), verificando:
- Accumulo corretto delle candidature giornaliere
- Deduplicazione SCD Type 1 su commesse e clienti
- Idempotenza su run ripetuti nello stesso giorno

---

## Cosa NON è incluso in questa proposta

- **SCD Type 2** (storicizzazione con `valid_from`/`valid_to`): pattern più complesso, merita una proposta separata
- **Paginazione REST**: da trattare in `IngestRestOAuth2Paginated` o come parametro opzionale v2
- **`CastColumns`**: utile ma bassa priorità — già parzialmente coperto da `AddDerivedColumns` per i casi semplici
- **Fan-out per lista di ID**: il pattern di `InrecruitingFetchApplications` (itera su lista di ID, chiama N endpoint, scrive M dataset) è potente ma molto complesso da generalizzare in modo dichiarativo senza introdurre un meccanismo di sub-task dinamici
