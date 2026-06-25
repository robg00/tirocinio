# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales-events"

# Generazione eventi
NUM_USERS = 100
ERROR_RATE = 0.1
MIN_INTERVAL = 0.1
MAX_INTERVAL = 0.5

# Distribuzione quantità (probabilità cumulativa per np.random.choice)
QUANTITY_DISTRIBUTION = {1: 0.60, 2: 0.25, 3: 0.10, 4: 0.03, 5: 0.02}

# Segmenti utenti
USER_SEGMENTS = ["new", "regular", "vip"]
USER_SEGMENT_WEIGHTS = {"new": 0.3, "regular": 0.5, "vip": 0.2}

# Weather dei prezzi (deviazione standard percentuale)
PRICE_VOLATILITY = 0.05

# Finestra temporale per timestamp simulati (ore indietro rispetto a now)
TIMESTAMP_WINDOW_HOURS = 24
