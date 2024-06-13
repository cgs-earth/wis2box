###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import logging

from requests import Session
from typing import Tuple

from wis2box.api.backend.base import BaseBackend
from wis2box.util import url_join, to_json

LOGGER = logging.getLogger(__name__)


class SensorthingsBackend(BaseBackend):
    """SensorthingsBackend API backend"""

    def __init__(self, defs: dict) -> None:
        """
        initializer

        :param defs: `dict` of connection parameters (RFC 1738 URL)
        """

        super().__init__(defs)

        self.type = 'SensorThings'
        self.url = url_join(defs.get('url'))
        self.http = Session()

    def sta_id(self, collection_id: str) -> Tuple[str]:
        """
        Make collection_id STA friendly

        :param collection_id: `str` name of collection

        :returns: `str` of STA index
        """
        entity = collection_id.split('.').pop()
        return url_join(self.url, entity)

    def add_collection(self, collection_id: str) -> dict:
        """
        Add a collection

        :param collection_id: `str` name of collection

        :returns: `bool` of result
        """
        return collection_id != ''

    def delete_collection(self, collection_id: str) -> bool:
        """
        Delete a collection

        :param collection_id: name of collection

        :returns: `bool` of delete result
        """
        return collection_id != ''

    def has_collection(self, collection_id: str) -> bool:
        """
        Checks a collection

        :param collection_id: name of collection

        :returns: `bool` of collection result
        """
        return collection_id != ''

    def upsert_collection_items(self, collection_id: str, items: list,
                                method: str = 'POST') -> str:
        """
        Add or update collection items

        :param collection_id: name of collection
        :param items: list of GeoJSON item data `dict`'s

        :returns: `str` identifier of added item
        """
        sta_index = self.sta_id(collection_id)

        for entity in items:
            if method == 'PATCH':
                item_id = entity['@iot.id']
                url = f'''{sta_index}('{item_id}')'''
                r = self.http.patch(url, data=to_json(entity))
            else:
                r = self.http.post(sta_index, data=to_json(entity))

            if not r.ok:
                LOGGER.error(r.content)
                return False
        return True

    def delete_collection_item(self, collection_id: str, item_id: str) -> str:
        """
        Delete an item from a collection

        :param collection_id: name of collection
        :param item_id: `str` of item identifier

        :returns: `bool` of delete result
        """

        LOGGER.debug(f'Deleting {item_id} from {collection_id}')
        sta_index = self.sta_id(collection_id)
        try:
            item_id = int(item_id)
        except ValueError:
            item_id = f"'{item_id}'"
        try:
            self.http.delete(f'{sta_index}({item_id})')
        except Exception as err:
            msg = f'Item deletion failed: {err}'
            LOGGER.error(msg)
            return False

        return True

    def __repr__(self):
        return f'<SensorthingsBackend> (url={self.url})'
