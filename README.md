# wis2box

[![Tests](https://github.com/wmo-im/wis2box/workflows/tests%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/wmo-im/wis2box/actions/workflows/tests-docker.yml)
[![Docs](https://readthedocs.org/projects/wis2box/badge)](https://docs.wis2box.wis.wmo.int)

## Quickstart

To download wis2box from source:

```
git clone https://github.com/cgs-earth/wis2box.git
```

To run with the ‘quickstart’ configuration, copy this file to demo.env in your working directory:

```
cp demo.env dev.env
```

Build and update wis2box:

```
python3 wis2box-ctl.py build
python3 wis2box-ctl.py update
```

Start wis2box and login to the wis2box-management container:

```
python3 wis2box-ctl.py start
python3 wis2box-ctl.py login
```

Publish stations:

```
wis2box metadata thing publish-collection
```

Publish datastreams collection:

```
wis2box metadata datastream publish-collection
```

Publish observations collection:

```
wis2box data observation publish-collection
```

Ingest some csv files:

```
wis2box data observation ingest -th iow.demo.Observations -b 2022-12-31T
```

Please consult the [documentation](https://docs.wis2box.wis.wmo.int) for installing
and running wis2box.

## Contact

- [Ben Webb](https://github.com/webb-ben)
