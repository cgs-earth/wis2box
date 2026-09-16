FROM docker:cli

WORKDIR /wis2box

VOLUME [ "/wis2box" ]
VOLUME [ "/opt/wis2box"]

# Install Python
RUN apk add --no-cache python3

# Bundle the wis2box deployment
COPY . /opt/wis2box

ENTRYPOINT [ "sh", "/opt/wis2box/entrypoint.sh" ]
CMD [ "start" ]
