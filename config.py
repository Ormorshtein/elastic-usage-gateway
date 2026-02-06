"""
Centralized configuration for the ES Usage Gateway.

All settings are read from environment variables with sensible defaults
for local development (docker-compose).
"""

import os

# --- Elasticsearch ---
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# --- Gateway ---
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "9201"))

# --- Usage Events ---
USAGE_INDEX = os.getenv("USAGE_INDEX", ".usage-events")
CLUSTER_ID = os.getenv("CLUSTER_ID", "default")

# --- Heat Thresholds (ops per hour) ---
INDEX_HEAT_HOT = float(os.getenv("INDEX_HEAT_HOT", "100"))
INDEX_HEAT_WARM = float(os.getenv("INDEX_HEAT_WARM", "10"))
INDEX_HEAT_COLD = float(os.getenv("INDEX_HEAT_COLD", "1"))

# --- Field Heat Thresholds (proportion of total field refs) ---
FIELD_HEAT_HOT = float(os.getenv("FIELD_HEAT_HOT", "0.15"))
FIELD_HEAT_WARM = float(os.getenv("FIELD_HEAT_WARM", "0.05"))
FIELD_HEAT_COLD = float(os.getenv("FIELD_HEAT_COLD", "0.01"))

# --- Query Body Storage ---
QUERY_BODY_ENABLED = os.getenv("QUERY_BODY_ENABLED", "true").lower() in ("true", "1", "yes")
QUERY_BODY_SAMPLE_RATE = float(os.getenv("QUERY_BODY_SAMPLE_RATE", "1.0"))
