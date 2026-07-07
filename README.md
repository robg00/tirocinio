# StreamMark — Pipeline E-commerce in Streaming

Pipeline real-time per la generazione, trasmissione, elaborazione e analisi di eventi di vendita e-commerce, basata su **Python**, **Apache Kafka**, **Apache Flink** e **PostgreSQL** (star schema).

## Architettura

```
Generatore Eventi (Python) → Kafka (topic: sales-events) → PyFlink (ETL, window 30s) → PostgreSQL (agg_sales_windowed)
```

## Struttura del progetto

```
Tirocinio/
├── docker-compose.yml           # Orchestrazione (Kafka, PostgreSQL, Flink, Kafka UI)
├── pyproject.toml               # Dipendenze e configurazione tool
├── config/
│   ├── products.json            # Catalogo prodotti (24 item, 4 categorie)
│   └── generator_config.py      # Parametri di generazione eventi
├── scripts/
│   └── event_generator.py       # Generatore di eventi di vendita
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── agg_tables.sql       # DDL tabella aggregata PostgreSQL
│       └── pyflink_etl.py       # Job ETL PyFlink (windowed aggregations)
├── sql/
│   ├── star_schema.sql          # DDL database analitico (dim + fact)
│   ├── seed_dim_product.sql     # Seed prodotti (24)
│   ├── seed_dim_date.sql        # Seed date (730)
│   └── seed_dim_user.sql        # Seed utenti (100)
├── flink/
│   └── Dockerfile               # Flink con Python, PyFlink e connector JARs
├── docs/
│   ├── report.typ               # Report tecnico (Typst)
│   └── report.pdf               # Report compilato
└── tests/
    ├── unit/
    │   └── test_generator.py    # Test unitari (25)
    ├── integration/
    │   ├── test_system.py       # Test di sistema (7)
    │   └── test_integration.py  # Test di integrazione (6)
    └── quality/
        ├── test_performance.py  # Benchmark prestazionali (5)
        └── test_mutation.py     # Test mutation score (1)
```

## Prerequisiti

- **Docker** + **Docker Compose** (per Kafka, PostgreSQL, Flink)
- **Python 3.14+**
- **uv** (gestione dipendenze)

## Avvio rapido

### 1. Avviare i servizi

```bash
docker compose up -d
```

Servizi avviati:
| Servizio | Porta host |
|---|---|
| Kafka Broker | `9092` |
| Zookeeper | `2181` |
| Kafka UI | `8080` |
| PostgreSQL | `5433` |
| Flink JobManager | `8081` |

### 2. Creare i topic

```bash
docker compose exec kafka kafka-topics --create \
  --topic sales-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics --create \
  --topic invalid-sales-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 3. Inizializzare il database (star schema + seed)

```bash
cat sql/star_schema.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_product.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_date.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_user.sql | docker compose exec -T postgres psql -U streammark -d streammark
```

### 4. Avviare il job PyFlink ETL

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink run -py /src/etl/pyflink_etl.py
```

> **Nota**: i connector JAR (`flink-sql-connector-kafka`, `flink-connector-jdbc`, `postgresql-42.7.5`) sono già inclusi nell'immagine Docker personalizzata (`flink/Dockerfile`).

### 5. Creare la tabella aggregata PostgreSQL

```bash
cat src/etl/agg_tables.sql | docker compose exec -T postgres psql -U streammark -d streammark
```

### 6. Avviare il generatore eventi

```bash
source .venv/bin/activate
PYTHONPATH=. python scripts/event_generator.py
```

Il generatore produce eventi JSON sul topic `sales-events` fino a Ctrl+C.

### 7. Verificare i dati

```bash
# Verifica dati aggregati su PostgreSQL
docker compose exec -T postgres psql -U streammark -d streammark -c "SELECT * FROM agg_sales_windowed;"

# Verifica eventi raw su Kafka
docker compose exec kafka kafka-console-consumer \
  --topic sales-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

Oppure via interfaccia web:
- Kafka UI: http://localhost:8080
- Flink Dashboard: http://localhost:8081

## Test

Sono implementate 5 categorie di test. Per tutti i comandi, assicurarsi che il cluster Kafka sia attivo (`docker compose up -d`) e impostare `PYTHONPATH=.`.

### Unit test (25)
```bash
PYTHONPATH=. uv run pytest tests/unit/test_generator.py -v
```

### Test di sistema (7)
```bash
PYTHONPATH=. uv run pytest tests/integration/test_system.py -v
```

### Test di integrazione (6)
```bash
PYTHONPATH=. uv run pytest tests/integration/test_integration.py -v
```

### Benchmark prestazionali (5)
```bash
PYTHONPATH=. uv run pytest tests/quality/test_performance.py -v \
  --benchmark-columns=min,max,mean,stddev,median,iqr,rounds,iterations
```

### Mutation test (1) — richiede ~90 secondi
```bash
PYTHONPATH=. uv run pytest tests/quality/test_mutation.py -v -m mutation
```

### Tutti i test (esclusi mutation e benchmark)
```bash
PYTHONPATH=. uv run pytest -v \
  --ignore=tests/quality/test_performance.py \
  --ignore=tests/quality/test_mutation.py
```

### Tutti i test con coverage
```bash
PYTHONPATH=. uv run pytest --cov=scripts --cov-report=term-missing
```

## Utilizzo CLI

Il generatore accetta parametri opzionali da riga di comando:

```bash
PYTHONPATH=. python scripts/event_generator.py \
  --error-rate 0.15 \
  --max-events 1000 \
  --topic sales-events \
  --bootstrap-servers localhost:9092 \
  --num-users 100
```

Tutti i parametri hanno default dal file `config/generator_config.py`.
Usa `--help` per l'elenco completo.

## Specifiche dei dati

### Evento valido

```json
{
  "sale_id": "S-000001",
  "user_id": "U-0041",
  "product_id": 303,
  "quantity": 2,
  "unit_price": 96.30,
  "total_amount": 192.60,
  "event_timestamp": "2026-06-23 15:30:00"
}
```

### Anomalie iniettate (~10% degli eventi)

| Tipo | Descrizione |
|---|---|
| `quantity_zero` | quantity = 0 |
| `negative_price` | unit_price negativo |
| `missing_field` | sale_id, user_id o product_id rimossi |
| `corrupted_timestamp` | event_timestamp = `"corrupted-timestamp"` |

## Star Schema

Il database analitico segue il modello a star schema con:

- **dim_product** (24 prodotti, 4 categorie: Electronics, Home, Clothing, Sports)
- **dim_user** (100 utenti, 3 segmenti: new, regular, vip)
- **dim_date** (730 giorni, 2025-01-01 — 2026-12-31)
- **fact_sales** (eventi ETL validati, con FK verso le dimensioni)

### Tabella aggregata (PyFlink ETL, finestra 30s)

| Colonna | Descrizione |
|---|---|
| `sale_count` | Numero di vendite nella finestra |
| `total_revenue` | Fatturato totale nella finestra |
| `avg_order_value` | Valore medio ordine nella finestra |
| `window_end` | Timestamp di fine finestra tumbling |

La tabella `agg_sales_windowed` viene popolata dal job PyFlink
che consuma eventi da Kafka, applica una finestra tumbling di 30
secondi e scrive i risultati aggregati in PostgreSQL via JDBC.

## Licenza

Progetto a scopo didattico.
