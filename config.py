"""
Centralized configuration for the ES Usage Gateway.

All settings are read from environment variables with sensible defaults
for local development (docker-compose).

Environment variables:
  ES_HOST                        Elasticsearch URL (default: http://localhost:9200)
  GATEWAY_HOST                   Bind address (default: 0.0.0.0)
  GATEWAY_PORT                   Bind port (default: 9201)
  USAGE_INDEX                    Index for usage events (default: .usage-events)
  CLUSTER_ID                     Cluster identifier in events (default: default)
  PROXY_TIMEOUT                  Proxy request timeout in seconds (default: 120)
  EVENT_TIMEOUT                  Event emission timeout in seconds (default: 10)
  METADATA_REFRESH_INTERVAL      Metadata cache refresh interval (default: 60)
  MAPPING_DIFF_REFRESH_INTERVAL  Mapping diff refresh interval in seconds (default: 300)
  MAPPING_DIFF_LOOKBACK_HOURS    How far back to look for field usage (default: 168 = 7 days)
  EVENT_SAMPLE_RATE              Fraction of requests that emit events (default: 1.0)
  QUERY_BODY_ENABLED             Store query bodies in events (default: true)
  QUERY_BODY_SAMPLE_RATE         Fraction of events to store bodies (default: 1.0)
"""

import os

# --- Elasticsearch ---
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

# --- Gateway ---
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "9301"))

# --- Usage Events ---
USAGE_INDEX = os.getenv("USAGE_INDEX", ".usage-events")
CLUSTER_ID = os.getenv("CLUSTER_ID", "default")

# --- Timeouts (seconds) ---
PROXY_TIMEOUT = float(os.getenv("PROXY_TIMEOUT", "120"))
EVENT_TIMEOUT = float(os.getenv("EVENT_TIMEOUT", "10"))
METADATA_REFRESH_INTERVAL = int(os.getenv("METADATA_REFRESH_INTERVAL", "60"))
MAPPING_DIFF_REFRESH_INTERVAL = int(os.getenv("MAPPING_DIFF_REFRESH_INTERVAL", "300"))
MAPPING_DIFF_LOOKBACK_HOURS = int(os.getenv("MAPPING_DIFF_LOOKBACK_HOURS", "168"))

# --- Proxy ---
PROXY_BODY_LIMIT = int(os.getenv("PROXY_BODY_LIMIT", str(1024 * 1024)))  # 1MB

# --- Workers ---
GATEWAY_WORKERS = int(os.getenv("GATEWAY_WORKERS", "1"))

# --- Bulk Event Writer ---
BULK_FLUSH_SIZE = int(os.getenv("BULK_FLUSH_SIZE", "100"))
BULK_FLUSH_INTERVAL = float(os.getenv("BULK_FLUSH_INTERVAL", "0.5"))
BULK_QUEUE_SIZE = int(os.getenv("BULK_QUEUE_SIZE", "5000"))

# --- Event Sampling ---
EVENT_SAMPLE_RATE = float(os.getenv("EVENT_SAMPLE_RATE", "1.0"))

# --- Query Body Storage ---
QUERY_BODY_ENABLED = os.getenv("QUERY_BODY_ENABLED", "true").lower() in ("true", "1", "yes")
QUERY_BODY_SAMPLE_RATE = float(os.getenv("QUERY_BODY_SAMPLE_RATE", "1.0"))
