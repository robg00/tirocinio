# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales-events"

# Event generation
NUM_USERS = 100
ERROR_RATE = 0.1
MIN_INTERVAL = 0.1
MAX_INTERVAL = 0.5

# Quantity distribution (weighted probabilities)
QUANTITY_DISTRIBUTION = {1: 0.60, 2: 0.25, 3: 0.10, 4: 0.03, 5: 0.02}

# User segments
USER_SEGMENTS = {"new": 0.3, "regular": 0.5, "vip": 0.2}

# Price volatility (percentage deviation from base price)
PRICE_VOLATILITY = 0.05

# Time window for simulated timestamps (hours before now)
TIMESTAMP_WINDOW_HOURS = 24
