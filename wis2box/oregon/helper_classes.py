
from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
from typing import ClassVar, List, Literal, Optional, Tuple

import httpx
from wis2box.env import API_BACKEND_URL
from wis2box.oregon.lib import assert_valid_date
from wis2box.oregon.types import FrostBatchRequest, Observation

LOGGER = logging.getLogger(__name__)

@dataclass
class UpdateMetadata():
    """Contains the metadata about a specific crawl"""
    data_start: str
    data_end: str
    failures: list[dict[int, str]]
    successes: list[dict[int, str]]

def load_metadata() -> UpdateMetadata:
    with open("oregon_load_metadata.json", "r") as f:
        metadata = json.load(f)
    return UpdateMetadata(**metadata)

def save_metadata(metadata: UpdateMetadata):
    with open("oregon_load_metadata.json", "w") as f:
        json.dump(asdict(metadata), f)

class CrawlResultStore:
    """Helper class to determine what to download based on a local metadata file"""

    metadata_file: ClassVar[str] = "oregon_load_metadata.json"  # noqa: F821

    def __init__(self):
        # check if metadata.json exists if not create it
        metadata_file_path = Path(self.metadata_file)
        if not metadata_file_path.exists():
            save_metadata(UpdateMetadata("", "", [], []))
            CrawlResultStore.metadata_file = str(metadata_file_path)
        else: # if it exists, make sure the successes and failures are not left over from the previous crawl
            metadata = load_metadata()
            metadata.successes, metadata.failures = [], []
            save_metadata(metadata)

    def get_range(self) -> Tuple[str, str]:
        """Get the range of data that has been downloaded"""
        metadata = load_metadata()
        assert_valid_date(metadata.data_start)
        assert_valid_date(metadata.data_end)
        return (metadata.data_start, metadata.data_end)

    def update_range(self, start: str, end: str):
        """Update the range of dates of data that has been downloaded"""
        # make sure that start and end are valid dates
        assert_valid_date(start)
        assert_valid_date(end)
        metadata = load_metadata()
        metadata.data_start = start
        metadata.data_end = end
        save_metadata(metadata)

    def set_success(self, station: int, message: str, with_log: bool):
        """Store the success message for a station and optionally log it"""
        metadata = load_metadata()
        metadata.successes.append({station: message})
        save_metadata(metadata)
        if with_log:
            LOGGER.info(message)

    def set_failure(self, station: int, message: str, with_log: bool):
        """Store the failure message for a station and optionally log it"""
        metadata = load_metadata()
        metadata.failures.append({station: message})
        save_metadata(metadata)
        if with_log:
            LOGGER.error(message)

@dataclass
class BatchObservation:
    """The body format for a FROST batch POST request"""
    id: str 
    method: Literal["post"]
    url: Literal["Observations"]
    body: Observation

class BatchHelper():
    """Helper for more easily constructing batched requests to the FROST API"""

    session: httpx.AsyncClient
    request: dict[Literal["requests"], list[BatchObservation]]

    def __init__(self, session: httpx.AsyncClient, observation_dataset: list[Observation]):
        self.session = session
        serialized_observations = []
        for observation in observation_dataset:
            request_encoded: FrostBatchRequest = {
                "id": f"{observation['Datastream']['@iot.id']}{id}",
                "method": "post",
                "url": "Observations",
                "body": observation,
            }
            serialized_observations.append(request_encoded)
        self.request = {"requests": serialized_observations}

    async def send(self):
        """Send batch data to the FROST API"""
        return await self.session.post(
            f"{API_BACKEND_URL}/$batch",
            json=self.request,
            headers={"Content-Type": "application/json"},
        )

