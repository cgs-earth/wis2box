#!/bin/bash
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

# wis2box entry script

echo "START /entrypoint.sh"

set -e

# Optional: Run wis2box-create-config.py
if [ "$1" = "configure" ]; then
    echo "Running configuration steps..."
    exec python3 /opt/wis2box/wis2box-create-config.py
fi

# Ensure wis2box.env exists
if [ -d /opt/wis2box/wis2box.env ]; then
    echo "wis2box.env not found, must be file"
    exit 1
elif [ -f /wis2box.env ]; then
    WIS2BOX_ENV=/wis2box.env
elif [ -f /wis2box/wis2box.env ]; then
    WIS2BOX_ENV=/wis2box/wis2box.env
else
    echo -d /opt/wis2box/wis2box.env 
    echo "wis2box.env not found"
    rm -rf /opt/wis2box/wis2box.env
    python3 /opt/wis2box/wis2box-create-config.py
    WIS2BOX_ENV=/wis2box/wis2box.env
fi

if [ ! -f /opt/wis2box/wis2box.env ]; then
    echo "Using: $WIS2BOX_ENV"
    cp -f $WIS2BOX_ENV /opt/wis2box/wis2box.env
fi

# Copy components to mounted volumes
cd /opt/wis2box
cp -nr /wis2box/data -t /opt/wis2box
cp -rf /opt/wis2box/loki /opt/wis2box/prometheus /opt/wis2box/grafana /opt/wis2box/nginx -t /wis2box/data

# Run wis2box-ctl.py
if [ "$1" == "start" ]||[ "$1" == "up" ]; then
    python3 /opt/wis2box/wis2box-ctl.py config > compose.yml
    sed -i "s|/opt/wis2box|${WIS2BOX_HOST_DIR}/data|g" compose.yml
    exec docker compose -f compose.yml up
elif [ "$1" == "config" ]; then
    python3 /opt/wis2box/wis2box-ctl.py config > compose.yml
    sed -i "s|/opt/wis2box|${WIS2BOX_HOST_DIR}/data|g" compose.yml
    exec cat compose.yml 
else
    exec python3 /opt/wis2box/wis2box-ctl.py ${@}
fi
