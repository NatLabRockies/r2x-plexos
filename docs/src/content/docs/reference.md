---
title: "Public API and configuration"
---

The supported top-level imports are declared in [`r2x_plexos.__all__`](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/__init__.py).
Source links point to current `main`; for an installed release use [tagged source](/r2x-plexos/reference/versions/).

| Export | Contract and source |
| --- | --- |
| `PLEXOSConfig` | Shared parser/exporter configuration; [fields and template mapping](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/plugin_config.py). |
| `PLEXOSParser` | Context-bound XML parser; [implementation](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/parser.py). |
| `PLEXOSExporter` | Context-bound XML/CSV exporter; [implementation](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/exporter.py). |
| `PLEXOSObject`, `PLEXOSProperty`, `PLEXOSPropertyValue`, `PLEXOSRow` | Object/property representations; [definitions](https://github.com/NatLabRockies/r2x-plexos/tree/main/src/r2x_plexos/models). |
| `get_horizon`, `set_horizon`, `horizon` | Read, set, or scope horizon selection; [exports](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/__init__.py). |
| `get_scenario_priority`, `set_scenario_priority`, `scenario_priority`, `scenario_and_horizon` | Scenario ordering and scoped selection; [exports](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/__init__.py). |

## Plugin lifecycle

Both parser and exporter use the core `Plugin.from_context(ctx)` class method and `run(*, ctx=None)` method.
`run()` returns the updated context and raises `PluginError` if a lifecycle hook returns an error.
The parser supplies `context.system`; the exporter consumes it and writes files.
[Core lifecycle source](https://github.com/NatLabRockies/r2x-core/blob/main/src/r2x_core/plugin_base.py) defines ordering and failure handling.

## Configuration fields

These are the package-specific settings. Inherited settings and validation are defined by [R2X Core](https://github.com/NatLabRockies/r2x-core/blob/main/src/r2x_core/plugin_config.py).

| Field | Meaning |
| --- | --- |
| `fpath` | Optional input XML file or directory. |
| `model_name` | Model selection; defaults to `default`. Use an existing XML Model name for parsing. |
| `horizon_year`, `weather_year` | Optional integer years. |
| `timeseries_dir` | Optional existing directory for referenced series. |
| `output_path` | Optional export directory. |
| `template` | Optional bundled version key or custom XML path; exporter falls back to PLEXOS10.0. |
| `simulation_config` | Optional `SimulationConfig`; see [simulation definitions](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/utils_simulation.py). |

Inspect the installed schema rather than assuming current defaults apply to an older release:

```python
from r2x_plexos import PLEXOSConfig

print(PLEXOSConfig.model_json_schema())

```

## Model and helper modules

Component definitions live in [`r2x_plexos.models`](https://github.com/NatLabRockies/r2x-plexos/tree/main/src/r2x_plexos/models).
PLEXOS memberships, collection properties, scenario priority, and horizon selection preserve XML relationships and property selection.
Follow [Python usage](/r2x-plexos/how-to/python/) for a complete context example.
