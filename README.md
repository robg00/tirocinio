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
├── pyproject.toml               # Dipendenze Python
├── config/
│   ├── products.json            # Catalogo prodotti (25 item, 4 categorie)
│   └── generator_config.py      # Parametri di generazione eventi
├── scripts/
│   └── event_generator.py       # Generatore di eventi di vendita
├── docs/
│   └── report.md                # Report tecnico di progetto
└── tests/
    └── test_generator.py        # Test unitari del generatore
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

### 2. Creare il topic

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
|---|---|
| `quantita_zero` | quantity = 0 |
| `prezzo_negativo` | unit_price negativo |
| `campo_mancante` | sale_id, user_id o product_id rimossi |
| `timestamp_corrotto` | event_timestamp = "NOT_A_TIMESTAMP" |

## Licenza

Progetto a scopo didattico.
