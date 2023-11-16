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

from base64 import b64encode
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import isodate
import json
import logging
import os
from pathlib import Path
import re
from typing import Iterator, Union
from urllib.parse import urlparse
from uuid import UUID
import yaml

LOGGER = logging.getLogger(__name__)


def get_typed_value(value) -> Union[float, int, str]:
    """
    Derive true type from data value

    :param value: value

    :returns: value as a native Python data type
    """

    try:
        if '.' in value:  # float?
            value2 = float(value)
        elif len(value) > 1 and value.startswith('0'):
            value2 = value
        else:  # int?
            value2 = int(value)
    except ValueError:  # string (default)?
        value2 = value

    return value2


def to_json(dict_: dict, pretty: bool = False) -> str:
    """
    Serialize dict to json

    :param dict_: `dict` of JSON representation
    :param pretty: `bool` of whether to prettify JSON (default is `False`)

    :returns: JSON string representation
    """

    if pretty:
        indent = 4
    else:
        indent = None

    return json.dumps(dict_, default=json_serial, indent=indent,
                      separators=(',', ':'))


def json_serial(obj: object) -> Union[bytes, str, float]:
    """
    helper function to convert to JSON non-default
    types (source: https://stackoverflow.com/a/22238613)

    :param obj: `object` to be evaluated

    :returns: JSON non-default type to `str`
    """

    if isinstance(obj, (datetime, date, time)):
        LOGGER.debug('Returning as ISO 8601 string')
        return obj.isoformat()
    elif isinstance(obj, bytes):
        try:
            LOGGER.debug('Returning as UTF-8 decoded bytes')
            return obj.decode('utf-8')
        except UnicodeDecodeError:
            LOGGER.debug('Returning as base64 encoded JSON object')
            return b64encode(obj)
    elif isinstance(obj, Decimal):
        LOGGER.debug('Returning as float')
        return float(obj)
    elif isinstance(obj, Path):
        LOGGER.debug('Returning as path string')
        return str(obj)
    elif isinstance(obj, UUID):
        LOGGER.debug('Returning as uuid string')
        return str(obj)

    msg = f'{obj} type {type(obj)} not serializable'
    LOGGER.error(msg)
    raise TypeError(msg)


def walk_path(path: Path, regex: str, recursive: bool) -> Iterator[Path]:
    """
    Walks os directory path collecting all files.

    :param path: required, string. os directory.
    :param regex: required, string. regex pattern to match files

    :returns: list. Iterator of file paths.
    """

    reg = re.compile(regex)
    if path.is_dir():
        if recursive:
            pattern = '**/*'
        else:
            pattern = '*'
        for f in path.glob(pattern):
            if f.is_file() and reg.match(f.name):
                yield f
    else:
        yield path


def yaml_load(fh) -> dict:
    """
    serializes a YAML files into a pyyaml object

    :param fh: file handle

    :returns: `dict` representation of YAML
    """

    # support environment variables in config
    # https://stackoverflow.com/a/55301129
    path_matcher = re.compile(r'.*\$\{([^}^{]+)\}.*')

    def path_constructor(loader, node):
        env_var = path_matcher.match(node.value).group(1)
        if env_var not in os.environ:
            msg = f'Undefined environment variable {env_var} in config'
            raise EnvironmentError(msg)
        return get_typed_value(os.path.expandvars(node.value))

    class EnvVarLoader(yaml.SafeLoader):
        pass

    EnvVarLoader.add_implicit_resolver('!path', path_matcher, None)
    EnvVarLoader.add_constructor('!path', path_constructor)

    return yaml.load(fh, Loader=EnvVarLoader)


def yaml_dump(fh: str, content: dict) -> None:
    """
    Writes serialized YAML to file

    :param fh: file handle
    :param content: dict, yaml file content

    :returns: `None`
    """
    return yaml.safe_dump(content, fh, sort_keys=False, indent=4)


def older_than(datetime_: str, days: int) -> bool:
    """
    Calculates whether a given datetime is older than n days

    :param datetime_: `str` of datetime
    :param days: `int` of number of days

    :returns: `bool` of whether datetime_ is older than n days
    """

    today = datetime.utcnow().date()

    LOGGER.debug(f'Datetime string {datetime_}')
    dt = isodate.parse_date(datetime_)

    delta = today - timedelta(days=days)

    return dt < delta


def datetime_days_ago(days: int) -> datetime:
    """
    Calculates datetime given n days ago

    :param days: `int` of number of days

    :returns: `datetime.date` object of date n days ago
    """

    today = datetime.utcnow().date()

    return today - timedelta(days=days)


def is_dataset(collection_id: str) -> bool:
    """
    Check whether the index is a dataset (and thus
    needs daily index management)

    :param collection_id: name of collection

    :returns: `bool` of evaluation
    """

    if '.' in collection_id or collection_id == 'messages':
        return True
    else:
        return False


def remove_auth_from_url(url: str) -> str:
    """
    Removes embedded auth from an RFC 1738 URL

    :param url: string of URL

    :returns: URL with embedded auth removed
    """

    u = urlparse(url)
    auth = f'{u.username}:{u.password}@'

    return url.replace(auth, '')


def clean_word(input: str, delim: str = ' ') -> str:
    """
    helper function to make clean words

    :param input: string of source

    :returns: str of resulting uuid
    """
    return delim.join(re.findall(r'\w+', input))


def url_join(*parts: str) -> str:
    """
    helper function to join a URL from a number of parts/fragments.
    Implemented because urllib.parse.urljoin strips subpaths from
    host urls if they are specified

    Per https://github.com/geopython/pygeoapi/issues/695

    :param parts: list of parts to join

    :returns: str of resulting URL
    """

    return '/'.join([str(p).strip().strip('/') for p in parts]).rstrip('/')


def make_uuid(input: str, raw: bool = False) -> UUID:
    """
    helper function to make uuid

    :param input: string of source
    :param raw: bool of str casting

    :returns: str of resulting uuid
    """
    _uuid = UUID(hex=hashlib.md5(input.encode('utf-8')).hexdigest())
    if raw:
        return _uuid
    else:
        return str(_uuid)


def extract_coord(p):
    """
    helper function to extract coordinate

    :param input: string of coordinate

    :returns: types coordinate value
    """
    return get_typed_value(''.join(re.findall(r'[-\d\.]+', p)))
