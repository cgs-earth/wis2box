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

FROM python:3.9.13-slim

LABEL maintainer="tomkralidis@gmail.com"

ARG WIS2BOX_PIP3_EXTRA_PACKAGES
ENV TZ="Etc/UTC" \
    DEBIAN_FRONTEND="noninteractive" \
    DEBIAN_PACKAGES="cron bash vim git libffi-dev libeccodes0 python3-eccodes python3-cryptography libssl-dev libudunits2-0 python3-paho-mqtt python3-dateparser python3-tz python3-setuptools"

RUN if [ "$WIS2BOX_PIP3_EXTRA_PACKAGES" = "None" ]; \
    then export WIS2BOX_PIP3_EXTRA_PACKAGES=echo; \
    else export WIS2BOX_PIP3_EXTRA_PACKAGES=pip3 install ${WIS2BOX_PIP3_EXTRA_PACKAGES}; \
    fi

# We need latest version of BUFR tables, these are only available in bookworm release, add to sources
RUN echo 'deb http://deb.debian.org/debian bookworm main' >> /etc/apt/sources.list

# install dependencies
# FIXME: csv2bufr/bufr2geojson: remove and install from requirements.txt once we have a stable release
# FIXME: pygeometa: remove and install from requirements.txt once we have a stable release
RUN apt-get update -y \
    && apt-get install -y -t bookworm libeccodes-data \
    && apt-get install -y ${DEBIAN_PACKAGES} \
    # install wis2box dependencies
    && pip3 install --no-cache-dir https://github.com/wmo-im/csv2bufr/archive/master.zip \
    && pip3 install --no-cache-dir https://github.com/wmo-im/bufr2geojson/archive/refs/tags/v0.4.1.zip \
    && pip3 install --no-cache-dir https://github.com/geopython/pygeometa/archive/master.zip \
    # install shapely
    && pip3 install cython pygeos==0.13 \
    && pip3 install -U "shapely<2" --no-binary shapely \
    # cleanup
    && apt autoremove -y  \
    && apt-get -q clean \
    && rm -rf /var/lib/apt/lists/*

# copy the app
COPY . /app
# install wis2box
RUN cd /app \
    && python3 setup.py install \
    # install wis2box plugins, if defined
    && $PIP_PLUGIN_PACKAGES \
    # add wis2box user
    && useradd -ms /bin/bash wis2box

WORKDIR /home/wis2box

# add wis2box.cron to crontab
COPY ./docker/wis2box/wis2box.cron /etc/cron.d/wis2box.cron

RUN chmod 0644 /etc/cron.d/wis2box.cron && crontab /etc/cron.d/wis2box.cron

COPY ./docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
