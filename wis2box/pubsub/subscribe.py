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

import json
import logging
import multiprocessing as mp
from pathlib import Path
from time import sleep

import click

from wis2box import cli_helpers
# from wis2box.api import setup_collection
from wis2box.env import (BROKER_HOST, BROKER_PORT, BROKER_USERNAME,
                         BROKER_PASSWORD, STORAGE_SOURCE, STORAGE_ARCHIVE)
from wis2box.handler import Handler
from wis2box.plugin import load_plugin, PLUGINS
# from wis2box.pubsub.message import gcm

LOGGER = logging.getLogger(__name__)


def handle(filepath):
    try:
        LOGGER.info(f'Processing {filepath}')
        handler = Handler(filepath)
        if handler.handle():
            LOGGER.info('Data processed')
            for plugin in handler.plugins:
                for filepath in plugin.files():
                    LOGGER.info(f'Public filepath: {filepath}')
    except ValueError as err:
        msg = f'handle() error: {err}'
        LOGGER.error(msg)
    except Exception as err:
        msg = f'handle() error: {err}'
        raise err


def on_message_handler(client, userdata, msg):
    LOGGER.debug(f'Raw message: {msg.payload}')

    message = json.loads(msg.payload)

    if message.get('EventName') == 's3:ObjectCreated:Put':
        LOGGER.debug('Incoming data is an s3 data object')
        key = str(message['Key'])
        filepath = f'{STORAGE_SOURCE}/{key}'
        if key.startswith(STORAGE_ARCHIVE):
            LOGGER.info(f'Do not process archived-data: {key}')
            return
    elif 'relPath' in message:
        LOGGER.debug('Incoming data is a filesystem path')
        filepath = Path(message['relPath'])
    else:
        LOGGER.warning('message payload could not be parsed')
        return

    while len(mp.active_children()) == mp.cpu_count():
        sleep(0.1)

    p = mp.Process(target=handle, args=(filepath,))
    p.start()


@click.command()
@click.pass_context
@click.option('--broker', '-b', help='URL to broker')
@click.option('--topic', '-t', help='topic to subscribe to')
@cli_helpers.OPTION_VERBOSITY
def subscribe(ctx, broker, topic, verbosity):
    """Subscribe to a broker/topic"""

    click.echo(f'Subscribing to broker {broker}, topic {topic}')

    defs = {
        'codepath': PLUGINS['pubsub']['mqtt']['plugin'],
        'url': f'mqtt://{BROKER_USERNAME}:{BROKER_PASSWORD}@{BROKER_HOST}:{BROKER_PORT}',  # noqa
        'client_type': 'subscriber'
    }

    broker = load_plugin('pubsub', defs)

    broker.bind('on_message', on_message_handler)

    broker.sub(topic)
