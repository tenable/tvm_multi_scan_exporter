from dataclasses import dataclass
from typing import Optional


@dataclass
class Scan:
    uuid: Optional[str]
    name: str
    schedule_uuid: str
    id: int


@dataclass
class ScanHistory:
    id: str
    status: str
    time_end: int

    # the following fields will be set by the code. They don't come from the API.
    schedule_uuid: Optional[str] = None
    schedule_id: Optional[int] = None
