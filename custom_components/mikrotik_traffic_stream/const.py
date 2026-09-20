"""Constants for the MikroTik Traffic Stream integration."""

DOMAIN = "mikrotik_traffic_stream"

CONF_INTERFACES = "interfaces"
CONF_USE_TLS = "use_tls"
CONF_VERIFY_TLS = "verify_tls"

DEFAULT_PORT_PLAIN = 8728
DEFAULT_PORT_TLS = 8729

# Reconnect backoff bounds in seconds when the router connection drops.
RECONNECT_MIN_SECONDS = 5
RECONNECT_MAX_SECONDS = 60

FIELD_RX = "rx-bits-per-second"
FIELD_TX = "tx-bits-per-second"

CONF_INTERVAL = "interval"
DEFAULT_INTERVAL = 5
MIN_INTERVAL = 1
MAX_INTERVAL = 300

STAT_AVERAGE = "average"
STAT_MIN = "min"
STAT_MAX = "max"
STATS = (STAT_AVERAGE, STAT_MIN, STAT_MAX)
