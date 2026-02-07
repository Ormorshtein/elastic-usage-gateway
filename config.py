"""
Centralized configuration for the ES Usage Gateway.

All settings are read from environment variables with sensible defaults
for local development (docker-compose).

Environment variables:
  ES_HOST                  Elasticsearch URL (default: http://localhost:9200)
  GATEWAY_HOST             Bind address (default: 0.0.0.0)
  GATEWAY_PORT             Bind port (default: 9201)
  USAGE_INDEX              Index for usage events (default: .usage-events)
  CLUSTER_ID               Cluster identifier in events (default: default)
  PROXY_TIMEOUT            Proxy request timeout in seconds (default: 120)
  EVENT_TIMEOUT            Event emission timeout in seconds (default: 10)
  ANALYZER_TIMEOUT         Heat analysis timeout in seconds (default: 30)
  METADATA_REFRESH_INTERVAL  Metadata cache refresh interval (default: 60)
  INDEX_HEAT_HOT/WARM/COLD Heat tier thresholds in ops/hour
  FIELD_HEAT_HOT/WARM/COLD Field heat thresholds as proportions
  QUERY_BODY_ENABLED       Store query bodies in events (default: true)
  QUERY_BODY_SAMPLE_RATE   Fraction of events to store bodies (default: 1.0)
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

# --- Timeouts (seconds) ---
PROXY_TIMEOUT = float(os.getenv("PROXY_TIMEOUT", "120"))
EVENT_TIMEOUT = float(os.getenv("EVENT_TIMEOUT", "10"))
ANALYZER_TIMEOUT = float(os.getenv("ANALYZER_TIMEOUT", "30"))
METADATA_REFRESH_INTERVAL = int(os.getenv("METADATA_REFRESH_INTERVAL", "60"))

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
