# tap-clickhouse

`tap-clickhouse` is a Singer tap for ClickHouse. Unlike 'airbyte' variant this one does not need docker to run.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

<!--
Install from PyPi:

```bash
pipx install tap-clickhouse
```

-->

Install from GitHub:

```bash
pipx install git+https://github.com/akurdyukov/tap-clickhouse.git@main
```

## Configuration

### Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

### Accepted Config Options

| Setting               | Required |  Default  | Description                                                                                                                                   |
|:----------------------|:--------:|:---------:|:----------------------------------------------------------------------------------------------------------------------------------------------|
| driver                |  False   |   http    | Driver type                                                                                                                                   |
| username              |  False   |  default  | Database user                                                                                                                                 |
| password              |   True   |   None    | Username password                                                                                                                             |
| host                  |  False   | localhost | Database host                                                                                                                                 |
| port                  |  False   |   8123    | Database connection port                                                                                                                      |
| database              |  False   |  default  | Database name                                                                                                                                 |
| secure                |  False   |     0     | Should the connection be secure                                                                                                               |
| verify                |  False   |     1     | Should secure connection need to verify SSL/TLS                                                                                               |
| batch_size            |  False   |   10000   | Number of records to read in each batch when syncing data from ClickHouse.                                                                    |
| stream_maps           |  False   |   None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html).   |
| stream_map_config     |  False   |   None    | User-defined config values to be used within map expressions.                                                                                 |
| flattening_enabled    |  False   |   None    | 'True' to enable schema flattening and automatically expand nested properties.                                                                |
| flattening_max_depth  |  False   |   None    | The max depth to flatten schemas.                                                                                                             |
| batch_config          |  False   |   None    |                                                                                                                                               |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-clickhouse --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Set `username` and `password` settings to authorize.

## Usage

You can easily run `tap-clickhouse` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-clickhouse --version
tap-clickhouse --help
tap-clickhouse --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-clickhouse` CLI interface directly using `poetry run`:

```bash
poetry run tap-clickhouse --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Run ClickHouse server with basic database using
```bash
docker-compose -f tests/docker-compose.yml up -d
```

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-clickhouse
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-clickhouse --version
# OR run a test `elt` pipeline:
meltano elt tap-clickhouse target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
