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
| `fpath` | `str \| None`, default `None`; input XML file or directory. |
| `model_name` | Model selection; defaults to `default`. Use an existing XML Model name for parsing. |
| `horizon_year`, `weather_year` | `int \| None`, each defaults to `None`. |
| `timeseries_dir` | `DirectoryPath \| None`, default `None`; must exist when supplied. |
| `output_path` | `str \| None`, default `None`; export directory. |
| `template` | `str \| None`, default `None`; bundled version key or custom XML path. Exporter falls back to `PLEXOS10.0`. |
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

## Construction and execution signatures

```python
PLEXOSParser.from_context(ctx: PluginContext[ConfigT]) -> Plugin[ConfigT]
PLEXOSExporter.from_context(ctx: PluginContext[ConfigT]) -> Plugin[ConfigT]
plugin.run(*, ctx: PluginContext[ConfigT] | None = None) -> PluginContext[ConfigT]
```

These parser/exporter classes inherit these methods from the [Core lifecycle](https://github.com/NatLabRockies/r2x-core/blob/main/src/r2x_core/plugin_base.py).
`from_context` binds configuration, system and store; `run` executes the normal
hook order. A parser produces `ctx.system`; an exporter requires an input System
and returns a context after writing files. A hook returning `Err` becomes a
`PluginError`, not an `Err` returned by `run`.

Configuration classes are Pydantic models. Invalid types or a missing required
field raise validation errors during construction, before plugin execution.
Inspect `model_json_schema()` from your installed package when generating a UI or
validating YAML. Package fields listed above do not replace inherited Core options.

## Property selection helpers

The public helpers below are exported by `r2x_plexos`:

```python
get_scenario_priority() -> dict[str, int] | None
set_scenario_priority(priority: dict[str, int] | None) -> None
get_horizon() -> tuple[str, str] | None
set_horizon(horizon: tuple[str, str] | None) -> None
scenario_priority(priority: dict[str, int] | None) -> Iterator[None]
horizon(date_from: str, date_to: str) -> Iterator[None]
scenario_and_horizon(
    priority: dict[str, int] | None, date_from: str, date_to: str
) -> Iterator[None]
```

The final three are context managers: use them with `with`, and the previous
selection is restored on exit. Lower scenario-priority values have higher priority.
Date strings describe the selected range. Selection is module-level state, so do
not assume these APIs create independent concurrent per-system contexts.
[Source and examples](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/models/context.py).

`PLEXOSPropertyValue.from_dict(data)` constructs entry-based values from a property
specification; `PLEXOSPropertyValue(value=...)` is not the current constructor.
The `entries` mapping retains PLEXOS dimensions rather than one flattened number.
[Property source](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/models/property.py).

## Parser and exporter side effects

Parser disk input is resolved from `store["xml_file"]` first, then `fpath`.
For a directory, sorted XML discovery selects the first matching file. The selected
Model must exist. `timeseries_dir` uses Pydantic DirectoryPath validation and must
exist when supplied. Failures to load XML or select the Model become lifecycle
errors.

The exporter writes XML and any exported time-series CSVs. Its filename is built
from model, horizon and optional weather-year metadata. Existing output reuse is
not an idempotence guarantee; use a fresh output directory when comparing runs.
The supported template keys and their XML filenames are declared in
[configuration source](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/plugin_config.py), and templates are supplied by
[plexosdb](https://github.com/NatLabRockies/plexosdb).
