# StreamMark — Pipeline E-commerce in Streaming

Pipeline real-time per generazione, trasporto, validazione, arricchimento e persistenza di eventi di vendita e-commerce, basata su **Python**, **Apache Kafka**, **Apache Flink (DataStream API pura)** e **PostgreSQL**.

## Architettura

```
Generatore Eventi (Python)
  ↓  Kafka Producer
[sales-events] ──────────────────────────────────────────────────┐
  ↓                                                              │
Job ETL (FlatMapFunction + filter + map)                         │
  ├── [valid-sales] ──→ Job Aggregazione (sliding 30s) ──→ [agg-sales-windowed]
  │                       ↓                                      ↓
  │                       fact_sales_consumer.py              agg_sales_consumer.py
  │                       ↓                                      ↓
  │                       PostgreSQL (fact_sales)             PostgreSQL (agg_sales_windowed)
  │
  └── [invalid-sales-events] ──→ Job Anomalie ──→ [anomaly-alerts]
                                   ↓
                                   anomaly_consumer.py
                                   ↓
                                   PostgreSQL (anomaly_alerts)
```
```

Principio chiave: **Kafka come unico intermediario** — Flink non scrive mai direttamente su PostgreSQL. Consumer Python esterni leggono da Kafka e scrivono sul DB.

## Struttura del progetto

```
Tirocinio/
├── docker-compose.yml               # Orchestrazione (Kafka, Flink, PostgreSQL, Kafka UI, pgAdmin)
├── pyproject.toml                   # Dipendenze e configurazione
├── config/
│   ├── products.json                # Catalogo prodotti (24 item, 4 categorie)
│   └── generator_config.py          # Parametri generazione eventi
├── scripts/
│   ├── event_generator.py           # Generatore eventi vendita
│   └── generate_dashboard.py        # Dashboard HTML (coverage + mutation + test)
├── src/
│   ├── etl/
│   │   ├── sales_splitter.py        # Logica pura split/validazione/arricchimento
│   │   ├── pyflink_etl.py           # Job ETL PyFlink (FlatMapFunction + filter + map)
│   │   ├── pyflink_datastream_anomaly.py  # Job anomalie PyFlink
│   │   ├── pyflink_aggregation.py   # Job finestra 30s PyFlink
│   │   └── etl-job.zip              # Archivio per -pyfs Flink
│   └── consumers/
│       ├── anomaly_consumer.py      # Kafka → PostgreSQL (anomaly_alerts)
│       ├── fact_sales_consumer.py    # Kafka → PostgreSQL (fact_sales)
│       └── agg_sales_consumer.py     # Kafka → PostgreSQL (agg_sales_windowed)
├── sql/
│   ├── star_schema.sql              # DDL star schema (dim + fact)
│   └── seed_*.sql                   # Seed dimensioni
├── flink/
│   └── Dockerfile                   # Flink + Python + PyFlink + connector JAR
├── tests/
│   ├── unit/etl/
│   │   ├── test_sales_splitter.py   # 39 test unitari split_event()
│   │   └── test_split_event_benchmark.py  # Benchmark prestazionali
│   ├── system/
│   │   ├── conftest.py              # Fixture auto job Flink
│   │   └── test_etl_pipeline_system.py  # 5 test di sistema E2E
│   ├── integration/
│   │   ├── test_system.py           # 7 test connettività Kafka
│   │   └── test_integration.py      # 6 test generatore
│   └── quality/
│       └── test_mutation.py         # Mutation score (soglia 70%)
├── htmlcov/
│   └── dashboard.html               # Dashboard risultati test
└── docs/
    ├── report.typ                   # Report tecnico (Typst)
    └── report.pdf                   # Report compilato
```

## Prerequisiti

- **Docker** + **Docker Compose**
- **Python 3.12+**
- **uv** (gestione dipendenze)

## Avvio rapido

### 1. Avviare i servizi

```bash
docker compose up -d
```

| Servizio | Porta host |
|---|---|
| Kafka Broker | `9092` |
| Zookeeper | `2181` |
| Kafka UI | `8080` |
| pgAdmin | `5050` |
| PostgreSQL | `5433` |
| Flink JobManager | `8081` |

### 2. Creare i topic Kafka

```bash
docker compose exec kafka kafka-topics --create --topic sales-events \
  --bootstrap-server localhost:9092 --partitions 3 --if-not-exists
docker compose exec kafka kafka-topics --create --topic valid-sales \
  --bootstrap-server localhost:9092 --partitions 3 --if-not-exists
docker compose exec kafka kafka-topics --create --topic invalid-sales-events \
  --bootstrap-server localhost:9092 --partitions 3 --if-not-exists
docker compose exec kafka kafka-topics --create --topic anomaly-alerts \
  --bootstrap-server localhost:9092 --partitions 3 --if-not-exists
docker compose exec kafka kafka-topics --create --topic agg-sales-windowed \
  --bootstrap-server localhost:9092 --partitions 3 --if-not-exists
```

### 3. Inizializzare il database

```bash
cat sql/star_schema.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_product.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_date.sql | docker compose exec -T postgres psql -U streammark -d streammark
cat sql/seed_dim_user.sql | docker compose exec -T postgres psql -U streammark -d streammark
```

### 4. Submittare i job Flink

```bash
# Job ETL (split + arricchimento)
docker compose exec -e GROUP_ID=etl-group flink-jobmanager /opt/flink/bin/flink run \
  -py /src/etl/pyflink_etl.py \
  -pyfs /src/etl/ \
  -d

# Job anomalie
docker compose exec -e GROUP_ID=anomaly-group flink-jobmanager /opt/flink/bin/flink run \
  -py /src/etl/pyflink_datastream_anomaly.py \
  -pyfs /src/etl/ \
  -d

# Job aggregazione finestre 30s
docker compose exec -e GROUP_ID=agg-group flink-jobmanager /opt/flink/bin/flink run \
  -py /src/etl/pyflink_aggregation.py \
  -pyfs /src/etl/ \
  -d
```

### 5. Avviare i consumer (scrive in PostgreSQL)

```bash
# Consumer anomalie (anomaly-alerts → anomaly_alerts)
uv run python3 src/consumers/anomaly_consumer.py &

# Consumer vendite valide (valid-sales → fact_sales)
uv run python3 src/consumers/fact_sales_consumer.py &

# Consumer aggregati (agg-sales-windowed → agg_sales_windowed)
uv run python3 src/consumers/agg_sales_consumer.py &
```

### 6. Generare eventi

```bash
uv run python3 scripts/event_generator.py
```

### 7. Verificare

```bash
# PostgreSQL
docker compose exec -T postgres psql -U streammark -d streammark \
  -c "SELECT COUNT(*) FROM fact_sales;"

# Kafka
docker compose exec kafka kafka-console-consumer \
  --topic valid-sales --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 3
```

### 8. Esplorare con pgAdmin

Apri http://localhost:5050, login con `admin@tirocinio.it` / `admin`, e registra il server PostgreSQL:

- **Host**: `postgres`, **Port**: `5432`, **Database**: `streammark`, **User**: `streammark`, **Password**: `streammark`

## Test

8 categorie di test (91 totali). Per i test di sistema, i job Flink devono essere RUNNING (la fixture `conftest.py` li submita automaticamente con `-pyfs`).

| Tipologia | Comando | Risultato |
|---|---|---|
| Unit test ETL | `PYTHONPATH=src uv run pytest tests/unit/etl/ -v` | 39/39 passati |
| Unit test generatore | `PYTHONPATH=src:. uv run pytest tests/unit/test_generator.py -v` | 25/25 passati |
| Benchmark ETL | `PYTHONPATH=src uv run pytest tests/unit/etl/test_split_event_benchmark.py --benchmark-only` | ~10μs/evento |
| Benchmark generatore | `PYTHONPATH=src:. uv run pytest tests/quality/test_performance.py -v --benchmark-skip` | 5/5 passati |
| Mutation test | `PYTHONPATH=src:. uv run pytest tests/quality/test_mutation.py -v -m mutation` | 79.4% |
| Test di sistema ETL | `PYTHONPATH=src uv run pytest tests/system/ -v` | 5/5 passati |
| Test integrazione Kafka | `PYTHONPATH=src:. uv run pytest tests/integration/ -v` | 13/13 passati |
| Tutti i test | `PYTHONPATH=src:. uv run coverage run -m pytest tests/ -v` | 91 passati |

### Dashboard

```bash
PYTHONPATH=src uv run python scripts/generate_dashboard.py
```
Aprire `htmlcov/dashboard.html` nel browser.

## Utilizzo CLI (generatore)

```bash
uv run python scripts/event_generator.py \
  --error-rate 0.15 --max-events 1000 \
  --topic sales-events \
  --bootstrap-servers localhost:9092
```

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
  "event_timestamp": "2026-06-23T15:30:00"
}
```

### Arricchimento ETL

| Campo | Descrizione |
|---|---|
| `date_id` | Data estratta dal timestamp (`YYYY-MM-DD`) |
| `value_band` | `low` (< 50), `medium` (50-200), `high` (> 200) |
| `sale_hour` | Ora della vendita |
| `sale_minute` | Minuto della vendita |
| `day_of_week` | Nome del giorno della settimana |

### Anomalie iniettate (~10%)

| Tipo | Condizione |
|---|---|
| `quantity_zero` | quantity = 0 |
| `negative_price` | unit_price ≤ 0 |
| `missing_field` | sale_id, user_id o product_id assenti |
| `corrupted_timestamp` | timestamp non parsabile |

## Star Schema

- **dim_product** — 24 prodotti, 4 categorie
- **dim_user** — 100 utenti, 3 segmenti
- **dim_date** — 730 giorni
- **fact_sales** — eventi validati e arricchiti con FK verso le dimensioni
- **anomaly_alerts** — eventi scartati con tipo errore
- **agg_sales_windowed** — aggregati finestra 30s

## Licenza

Progetto a scopo didattico.