import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Tuple, Dict, Union, Optional

from tenable.io import TenableIO

from tvm_multi_scan_exporter.constants import CURRENT_TIME, MAX_SINCE_SECONDS, DEFAULT_WRITE_BATCH_SIZE, \
    DEFAULT_WORKERS, DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT, TVM_ACCESS_KEY_ENV_VARIABLE_NAME, \
    TVM_SECRET_KEY_ENV_VARIABLE_NAME, EXTERNAL_DB_PASSWORD_ENV_VARIABLE_NAME, DEFAULT_TVM_URL
from tvm_multi_scan_exporter.util import read_toml_from_file, check_type


@dataclass
class Window:
    # defaults to current time -24 hours
    since: int = field(default_factory=lambda: CURRENT_TIME - 86400)

    # defaults to current time
    upto: int = field(default_factory=lambda: CURRENT_TIME)

    def __post_init__(self):
        # Ensure `since` is not more than 15 days before `upto`
        if self.upto - self.since > MAX_SINCE_SECONDS:
            raise ValueError("`since` cannot be more than 15 days older than `upto`")


class ExportType(ABC):
    @property
    @abstractmethod
    def value(self) -> str:
        pass


@dataclass(frozen=True)
class Csv(ExportType):
    value: str = "csv"


@dataclass(frozen=True)
class Json(ExportType):
    value: str = "json"


@dataclass(frozen=True)
class Parquet(ExportType):
    value: str = "parquet"


DialectAndDbApi = str
DriverName = str


class DatabaseEngine(Enum):
    MsSqlServer: Tuple[DialectAndDbApi, DriverName] = ("mssql+pyodbc", "ODBC Driver 17 for SQL Server")


@dataclass
class WriteToDatabase(ExportType):
    """
    Takes in the configuration required to write to an external database.
    """
    user: str = field(repr=False)
    password: Optional[str] = field(default=None, init=False, repr=False)
    host_address: str = field(repr=False)
    port: int
    database_name: str
    table_name: str
    engine: DatabaseEngine = field(default_factory=lambda: DatabaseEngine.MsSqlServer)
    write_batch_size: int = field(default_factory=lambda: DEFAULT_WRITE_BATCH_SIZE)

    def __post_init__(self):
        self.password = os.getenv(EXTERNAL_DB_PASSWORD_ENV_VARIABLE_NAME)
        if not self.password:
            raise EnvironmentError(
                f"In order to utilise an external database, environment variable "
                f"'{EXTERNAL_DB_PASSWORD_ENV_VARIABLE_NAME}' is required."
            )

    @property
    def value(self) -> str:
        return "database"


@dataclass
class Config:
    # a `contains` filter will be applied on this; case-insensitive
    scan_name: str

    # max allowed GB to allocate for the Duck DB process.
    allowed_memory_gb: int

    # Number of workers to process this export; defaulted to 1
    workers: int = field(default_factory=lambda: DEFAULT_WORKERS)

    # Time Window for export
    window: Window = field(default_factory=lambda: Window())
    export_type: ExportType = field(default_factory=lambda: Csv())

    # fields to include in the export.
    # These are the field names from a CSV scan export.
    # Example field names: "Asset UUID", ""IP Address""
    columns: List[str] = field(default_factory=lambda: DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT)

    # Optional Download folder to download the file into
    output_location: Optional[str] = None

    def __post_init__(self):
        check_type(self.workers, int)
        if not (1 <= self.workers <= 10):
            raise ValueError("`workers` must be between 1 and 10")
        check_type(self.scan_name, str)
        check_type(self.allowed_memory_gb, int)
        check_type(self.workers, int)
        check_type(self.window.since, int)
        check_type(self.window.upto, int)
        check_type(self.export_type, ExportType)

        if self.output_location:
            self.output_location = self.output_location.strip("/\\")


def get_config_from_file(path: str) -> Config:
    """
    Gets the configuration from file.
    """
    data: Dict = read_toml_from_file(path)

    # Required fields
    scan_name = data.get("scan_name") or (_ for _ in ()) \
        .throw(ValueError(f"`scan_name` is required in the config file: {path}"))

    allowed_memory_gb = data.get("allowed_memory_gb") or (_ for _ in ()) \
        .throw(ValueError(f"`allowed_memory_gb` is required in the config file: {path}"))

    # Optional fields
    workers = data.get("workers", DEFAULT_WORKERS)
    columns = data.get("columns", DEFAULT_COLUMNS_TO_BE_PART_OF_EXPORT)

    window_data = data.get("window", {})
    window = Window(**window_data)
    downloads_folder = data.get("output_location", None)

    export_type_raw = data.get("export_type", "csv")
    export_type = _parse_export_type(export_type_raw)

    return Config(
        scan_name=scan_name,
        allowed_memory_gb=allowed_memory_gb,
        workers=workers,
        window=window,
        export_type=export_type,
        columns=columns,
        output_location=downloads_folder
    )


def _parse_export_type(raw: Union[str, dict]) -> ExportType:
    """
    Parses export type and returns the corresponding object.
    """
    if isinstance(raw, str):
        match raw.lower():
            case "csv":
                return Csv()
            case "json":
                return Json()
            case "parquet":
                return Parquet()
            case _:
                raise ValueError(f"Unknown export_type: {raw}")

    if isinstance(raw, dict):
        export_type = raw.get("type", "write_to_database").lower()
        if export_type != "write_to_database":
            raise ValueError(f"Only 'write_to_database' export_type is supported in table format, got: {export_type}")

        return WriteToDatabase(
            user=raw["user"],
            host_address=raw["host_address"],
            port=raw["port"],
            database_name=raw["database_name"],
            table_name=raw["table_name"],
            engine=DatabaseEngine[raw.get("engine", "MsSqlServer")],
            write_batch_size=raw.get("write_batch_size", DEFAULT_WRITE_BATCH_SIZE),
        )

    raise TypeError("export_type must be a string or a dictionary")


def get_tenable_io_from_config(tvm_url: str) -> TenableIO:
    """
    Constructs a TenableIO object using environment variables.
    """
    access_key = os.getenv(TVM_ACCESS_KEY_ENV_VARIABLE_NAME)
    secret_key = os.getenv(TVM_SECRET_KEY_ENV_VARIABLE_NAME)

    if not access_key or not secret_key:
        raise EnvironmentError(
            f"Environment variables {TVM_ACCESS_KEY_ENV_VARIABLE_NAME} and "
            f"{TVM_SECRET_KEY_ENV_VARIABLE_NAME} must be set."
        )

    return TenableIO(access_key=access_key, secret_key=secret_key, url=tvm_url)
