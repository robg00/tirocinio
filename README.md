# Tirocinio - Pipeline E-commerce in Streaming

Pipeline real-time per l'elaborazione di eventi di vendita di un e-commerce, basata su **Apache Kafka** e **Python**.

## Architettura

```
Generatore Eventi (Python) → Kafka (topic: sales-events) → Flink (ETL) → Database Analitico (Star Schema)
```

## Struttura del progetto

```
Tirocinio/
├── docker-compose.yml           # Orchestrazione Kafka + Zookeeper + Kafka UI
├── pyproject.toml               # Dipendenze e configurazione tool
├── config/
│   ├── products.json            # Catalogo prodotti (24 item, 4 categorie)
│   └── generator_config.py      # Parametri di generazione eventi
├── scripts/
│   └── event_generator.py       # Generatore di eventi di vendita
├── docs/
│   ├── report.typ               # Report tecnico (Typst)
│   ├── report.pdf               # Report compilato
│   └── mutation-results.json    # Risultati mutation testing
└── tests/
    ├── test_generator.py        # Test unitari (25)
    ├── test_system.py           # Test di sistema (7)
    ├── test_integration.py      # Test di integrazione (6)
    ├── test_performance.py      # Benchmark prestazionali (5)
    └── test_mutation.py         # Test mutation score (1)
```

## Prerequisiti

- **Docker** + **Docker Compose** (per Kafka)
- **Python 3.14+**
- **uv** (gestione dipendenze)

## Avvio rapido

### 1. Avviare Kafka

```bash
docker compose up -d
```

Servizi avviati:
| Servizio | Porta |
|---|---|
| Kafka Broker | `9092` |
| Zookeeper | `2181` |
| Kafka UI | `8080` |

### 2. Creare il topic (automatico con `KAFKA_NUM_PARTITIONS=3`)

```bash
docker compose exec kafka kafka-topics --create \
  --topic sales-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Attivare l'ambiente virtuale

```bash
source .venv/bin/activate
```

### 4. Avviare il generatore eventi

```bash
PYTHONPATH=. python scripts/event_generator.py
```

Il generatore produce eventi JSON sul topic `sales-events` fino a Ctrl+C.

### 5. Verificare i dati su Kafka

```bash
docker compose exec kafka kafka-console-consumer \
  --topic sales-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

Oppure via interfaccia web: [http://localhost:8080](http://localhost:8080)

## Test

Sono implementate 5 categorie di test, eseguibili separatamente. Per tutti i comandi, assicurarsi che il cluster Kafka sia attivo (`docker compose up -d`) e impostare `PYTHONPATH=.` per la corretta risoluzione dei moduli.

### Unit test (25)
```bash
PYTHONPATH=. uv run pytest tests/test_generator.py -v
```

### Test di sistema (7)
```bash
PYTHONPATH=. uv run pytest tests/test_system.py -v
```

### Test di integrazione (6)
```bash
PYTHONPATH=. uv run pytest tests/test_integration.py -v
```

### Benchmark prestazionali (5)
```bash
PYTHONPATH=. uv run pytest tests/test_performance.py -v \
  --benchmark-columns=min,max,mean,stddev,median,iqr,rounds,iterations
```

### Mutation test (1) — richiede ~60 secondi
```bash
PYTHONPATH=. uv run pytest tests/test_mutation.py -v -m mutation
```

### Tutti i test (esclusi mutation e benchmark)
```bash
PYTHONPATH=. uv run pytest -v \
  --ignore=tests/test_performance.py \
  --ignore=tests/test_mutation.py
```

### Tutti i test con coverage
```bash
PYTHONPATH=. uv run pytest --cov=scripts --cov-report=term-missing
```

### Report dei risultati

Un dashboard HTML con tutti i risultati strutturati è disponibile in `docs/test-report.html`.
I dati grezzi in formato JSON sono salvati nella directory `docs/` per ogni categoria di test.

## Specifiche dei dati

### Evento valido

```json
{
  "sale_id": "S-000001",
  "user_id": "U-0041",
  "product_id": 303,
  "quantity": 2,
  "unit_price": 96.30,
  "event_timestamp": "2026-06-23T15:30:00+00:00"
}
```

### Anomalie iniettate (~10% degli eventi)

| Tipo | Descrizione |
|---|---|---|
| `quantity_zero` | quantity = 0 |
| `negative_price` | unit_price negativo |
| `missing_field` | sale_id, user_id o product_id rimossi |
| `corrupted_timestamp` | event_timestamp = "NOT_A_TIMESTAMP" |

## Licenza

Progetto a scopo didattico.
