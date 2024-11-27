#!/bin/bash

# This file contains a script to update the Oregon ODWR dataset
# It is intended to be ran from a cronjob
# To set this up in cron, run the following command:
# crontab -e
# and add the following line (change to your path):
# 0 0 * * * /home/cloftus/oregon/odwr/pullNewData.sh

# Define the container name
CONTAINER_NAME="wis2box"

# Run the echo command inside the container
docker exec -it "$CONTAINER_NAME" wis2box oregon odwr update
