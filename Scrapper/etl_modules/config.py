import os
import sys


def _get_cli_arg(flag: str):
    """Read a CLI argument passed as '--flag value' or '--flag=value'."""
    argv = sys.argv[1:]

    if flag in argv:
        idx = argv.index(flag)
        if idx + 1 < len(argv):
            return argv[idx + 1]

    prefix = f"{flag}="
    for arg in argv:
        if arg.startswith(prefix):
            return arg.split("=", 1)[1]

    return None


# Runtime environment: ENV can come from os.environ or CLI args.
# Defaults to 'dev' to avoid writing into prod by mistake.
ENV = (os.getenv("ENV") or _get_cli_arg("--env") or "dev").strip().lower()
if ENV not in {"dev", "prod"}:
    ENV = "dev"

# Default catalog per environment. Can be overridden with DBX_CATALOG or --catalog.
DEFAULT_CATALOG_BY_ENV = {
    "dev": "supermercadoetl_dev",
    "prod": "supermercadoetl_prod",
}

CATALOG = (os.getenv("DBX_CATALOG") or _get_cli_arg("--catalog") or DEFAULT_CATALOG_BY_ENV[ENV]).strip()

# Logical schemas used by the ETL.
AUDIT_SCHEMA = f"{CATALOG}.audit"
BRONZE_SCHEMA = f"{CATALOG}.bronze"

# Fully-qualified table names. Modules should use these constants instead of hardcoded names.
ETL_LOGS_TABLE = f"{AUDIT_SCHEMA}.etl_logs"
SCRAPING_ERRORS_TABLE = f"{AUDIT_SCHEMA}.scraping_errors"
RUN_METRICS_TABLE = f"{AUDIT_SCHEMA}.run_metrics"
BRONZE_PRICES_TABLE = f"{BRONZE_SCHEMA}.precios_scraping"

# Pipeline metadata. Supports override from env vars or CLI params.
PIPELINE_NAME = (os.getenv("PIPELINE_NAME") or _get_cli_arg("--pipeline-name") or "extract_coto_precios").strip()
SOURCE_NAME = (os.getenv("SOURCE_NAME") or _get_cli_arg("--source-name") or "Coto").strip()

# Request configuration for Coto scraping.
COTO_PARAMS = {
    "Dy": "1",
    "idSucursal": "200",
    "format": "json",
}
COTO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}

# HTTP behavior.
REQUEST_TIMEOUT_SECONDS = 10
SSL_VERIFY_FALLBACK = False
