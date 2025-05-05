import time
from typing import Set

ALLOWED_EXPORT_STATES: Set[str] = {"completed", "aborted", "canceled"}

# current time during the start of the script
CURRENT_TIME = int(time.time())

# 15 days in seconds
MAX_SINCE_SECONDS = 15 * 86400

# path and name of the config file. This should be present in the same directory as the script.
CONFIG_FILE_PATH = "config.toml"

DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT = [
    "Asset UUID",
    "IP Address",
    "Plugin ID",
    "Severity",
    "Protocol",
    "Port",
    "FQDN",
    "Plugin Output",
    "First Found",
    "Last Found",
    "Service",
    "Host Scan Schedule ID",
    "Host Scan ID",
    "HOST"
]

DEFAULT_MAX_BY_COLUMNS = [
    "Host End"
]

DEFAULT_IDENTIFIER_COLUMNS = [
    "Asset UUID",
    "IP Address",
    "Plugin ID",
    "Port",
    "Protocol",
    "Service"
]

DB_EXCLUDE_COLUMNS = ['identifier', 'host_end', 'host']

# These columns should always be added as they're referenced in the duck db query.
ALWAYS_INCLUDE_COLUMNS = ["HOST"]

# batch size for duckdb writes
DEFAULT_WRITE_BATCH_SIZE = 100

# parallelism
DEFAULT_WORKERS = 1

DEFAULT_TVM_URL = "https://cloud.tenable.com"

DOWNLOAD_FOLDER_NAME = "downloads"
DEDUP_FOLDER_NAME = "deduped_scans"

TVM_ACCESS_KEY_ENV_VARIABLE_NAME = "TVM_ACCESS_KEY"
TVM_SECRET_KEY_ENV_VARIABLE_NAME = "TVM_SECRET_KEY"
EXTERNAL_DB_PASSWORD_ENV_VARIABLE_NAME = "EXTERNAL_DB_PASSWORD"

DEFAULT_MAX_RETRIES_FOR_SCAN_EXPORT = 5
DEFAULT_RETRY_DELAY_SECONDS = 10
