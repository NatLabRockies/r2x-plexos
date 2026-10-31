<img src="./docs/src/assets/r2x-logo-full-square.svg" alt="R2X framework logo" align="left" width="220px" height="220px" hspace="10"/>
<img align="left" alt="" width="0" height="220px" hspace="10"/>

#### r2x-plexos
<p><small>PLEXOS models, parser, and exporter for the R2X ecosystem.</small></p>

[![PyPI](https://img.shields.io/pypi/v/r2x-plexos.svg)](https://pypi.org/project/r2x-plexos/) [![Docs](https://img.shields.io/badge/docs-guides-0079c2)](https://natlabrockies.github.io/r2x-plexos/)

<br clear="left">

`r2x-plexos` reads PLEXOS XML into typed R2X systems and exports systems back to PLEXOS.
Use [r2x-cli](https://github.com/NatLabRockies/r2x-cli) for repeatable pipelines and [R2X translation plugins](https://natlabrockies.github.io/R2X/) for cross-format mappings.

## Quick Start

Use Python 3.11, 3.12, or 3.13. In your active Python environment:

```bash
python -m pip install r2x-plexos
python -c "import r2x_plexos; print(r2x_plexos.__version__)"
```

The command prints the installed package version. For the separately installed CLI, install into its managed environment:

```bash
r2x python install
r2x install r2x-plexos
r2x list r2x-plexos
```

Confirm `plexos-parser` and `plexos-exporter` are discovered, then follow the [round-trip tutorial](https://natlabrockies.github.io/r2x-plexos/tutorials/round-trip/).

A `parser_rules.json` in a configuration override may replace the packaged component rules.
It must retain a rule for each supported PLEXOS class present in the input; missing rules fail
with the class and configuration path instead of silently dropping those objects. Unsupported
classes continue to be skipped with a warning.

Plain, nonconstant component-owned profiles from direct CSV files can use Core time-series rules.
The packaged rules cover generator capacity and fuel price, region and purchaser load, reserve
requirement, fuel price, and storage inflow. Other direct properties can be selected by adding a Core rule with the target component class,
series name, `owner`, `timestamp`, and `value` fields, and a separate rule when a horizon feature
is present. Variable actions, scenario or date dimensions, timeslices, multiple bands, constants,
conflicting mapped aliases, datafile-object references, and collection-property owners keep the
native PLEXOS resolution path. Core's rules require a fixed owner class and series name and do not
select supplemental-attribute owners; these distinctions preserve PLEXOS property behavior.

## Documentation

- [Getting started](https://natlabrockies.github.io/r2x-plexos/getting-started/installation/): installation, environment setup, and first-use checks.
- [Tutorials](https://natlabrockies.github.io/r2x-plexos/tutorials/round-trip/): parse and export a public test model.
- [How-to guides](https://natlabrockies.github.io/r2x-plexos/how-to/python/): Python contexts and CLI pipelines.
- [Reference / API](https://natlabrockies.github.io/r2x-plexos/reference/): exports, configuration, and source links.
- [Explanation](https://natlabrockies.github.io/r2x-plexos/explanation/): data flow and compatibility.
- [Troubleshooting](https://natlabrockies.github.io/r2x-plexos/troubleshooting/): discovery, inputs, and export failures.
- [Changelog](CHANGELOG.md): canonical release history.
- [Contributors](https://natlabrockies.github.io/r2x-plexos/contributors/): development and docs checks.

## Development

```bash
uv sync --all-groups
uv run prek run --all-files --hook-stage pre-push
```

See [docs/README.md](docs/README.md) for the documentation build.

## License

BSD-3-Clause. See [LICENSE.txt](LICENSE.txt).
