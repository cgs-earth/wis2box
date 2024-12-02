import logging
import shelve
from datetime import timedelta
from typing import ClassVar, Optional, Tuple

import requests

HEADERS = {"accept": "application/vnd.api+json"}

LOGGER = logging.getLogger(__name__)


class ShelveCache:
    """
    Helper cache for storing and fetching data from the oregon API. 
    Not used in production, moreso to make testing less intensive on upstream
    """
    db: ClassVar[str] = "oregondb"

    def set(self, url: str, json_data: dict, _ttl: Optional[timedelta] = None):
        with shelve.open(ShelveCache.db, "w") as db:
            db[url] = json_data

    def get_or_fetch(self, url: str, force_fetch: bool = False) -> Tuple[bytes, int]:
        with shelve.open(ShelveCache.db) as db:
            if url in db and not force_fetch:
                LOGGER.info(f"Using cache for {url}")
                return db[url], 200
            else:
                res = requests.get(url, headers=HEADERS)
                db[url] = res.content
                return res.content, res.status_code

    def reset(self):
        with shelve.open(ShelveCache.db, "w") as db:
            for key in db:
                del db[key]

    def clear(self, url: str):
        with shelve.open(ShelveCache.db, "w") as db:
            if url not in db:
                return

            del db[url]

    def contains(self, url: str) -> bool:
        with shelve.open(ShelveCache.db) as db:
            return url in db

    def get(self, url: str):
        with shelve.open(ShelveCache.db) as db:
            return db[url]
