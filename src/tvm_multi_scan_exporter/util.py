import re
from pathlib import Path
from typing import Dict, Any

import tomli


def snake_case(string: str) -> str:
    """
    Converts a normal string to snake case.

    For example:
    "Asset UUID" to "asset_uuid".
    """
    string = string.strip().lower()

    # Replace spaces & special chars with '_'
    string = re.sub(r'[^a-z0-9]+', '_', string)

    # Remove multiple underscores
    string = re.sub(r'_+', '_', string)

    # Remove leading/trailing underscores if any
    return string.strip('_')


def read_toml_from_file(path: str) -> Dict:
    """
    Reads the provided file path into a dictionary.
    """

    with open(path, "rb") as file:
        data: Dict = tomli.load(file)

    return data


def check_type(obj: Any, expected_type: Any):
    # handle scalar types
    if isinstance(obj, expected_type):
        return
    else:
        raise TypeError(f"The value {obj} should be of type {expected_type}")


def file_size_in_mb(path: Path) -> float:
    """
    Returns the size of the file in MB.
    """
    return round(path.stat().st_size / (1024 ** 2), 3)
