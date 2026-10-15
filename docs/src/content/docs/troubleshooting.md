---
title: Troubleshooting
---

Use the error text or the incorrect result below. The commands use the
[public-fixture tutorial](/r2x-plexos/tutorials/round-trip/) directory; replace only
its paths when diagnosing a study. Preserve original inputs and diagnose a working
copy. `PluginError` adds the lifecycle stage to underlying errors; the stage alone
does not identify their cause.

## Plugin is missing

If the CLI cannot resolve `plexos-parser`, or Python cannot import `r2x_plexos`, first
identify **which interpreter** failed. A project installation and the CLI runtime
are separate. These checks do not install or modify packages:

```bash
r2x python show
```

```bash
r2x list r2x-plexos
```

The list must include `plexos-parser` and `plexos-exporter`. If absent, follow the
[CLI installation path](/r2x-plexos/getting-started/installation/#use-the-cli-plugin)
in that same runtime, then repeat `list`. If Python scripts fail instead, check:

```bash
python -c "import sys; print(sys.executable)"
```

```bash
python -c "from importlib.metadata import version; print(version('r2x-plexos'))"
```

A version printed by one interpreter does not establish availability in another.
Select the environment from [environment setup](/r2x-plexos/getting-started/environment/)
and rerun the exact import that failed.

For `Python ABI ... is incompatible with this r2x binary`, use the runtime required
by that binary; the tested CLI 0.1.15 binary required Python 3.12. Do not change a
project's Python and assume the executable ABI changed. Verify with `r2x python show`
and the [CLI runtime guidance](https://github.com/NatLabRockies/r2x-cli/blob/main/docs/development.md).

For `option '--show-help' requires a value`, use the tested current help command:

```bash
r2x run plexos-parser --help
```

Success prints the plugin's actual configuration flags. Use those flags for the
installed release rather than copying legacy Core CLI options.

## `Could not find PLEXOS XML file` or `Object = ... not found on the database`

These are different failures: the first is input resolution; the second means a
named object, commonly the configured Model, is absent in a database that loaded.
The tutorial reproduced the second with `model_name="NotAModel"`.

Save this as `check_xml.py` and run it with the Python environment containing
`r2x-plexos`:

```python
from pathlib import Path
from plexosdb import PlexosDB

path = Path("inputs/model.xml").resolve()
print("XML path:", path)
print("Exists:", path.is_file())
if path.is_file():
    db = PlexosDB.from_xml(path)
    print("Models:", db.list_models())
```

```bash
python check_xml.py
```

For the pinned fixture, expect `Exists: True` and `Models: ['Base']`.
If existence is false, correct `fpath` or restore the input before changing model
settings. If Models lists other names, use one of those names for `model_name`.
A filename such as `5_bus_system_variables.xml` is not a Model name.

Check the actual pipeline selection:

```bash
r2x run pipeline.yaml round_trip --print
```

An `xml_file` DataStore entry takes precedence over `fpath`; a directory-valued
`fpath` chooses the first sorted XML. Use an explicit file to avoid selecting a
second database. For malformed XML, the failure can be wrapped as
`Failed to initialize database: ...`; the database reader above reproduces that
load independently. Restore a valid native XML from the producer rather than
editing an Infrasys archive to imitate one.

**Verify:** preview the corrected path and Model, rerun parsing, then repeat the
[tutorial component checks](/r2x-plexos/tutorials/round-trip/#5-inspect-the-graph-not-just-the-filenames).
The fixture returns 37 components and five nodes. See [parser resolution](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/parser.py)
and [missing-Model regression test](https://github.com/NatLabRockies/r2x-plexos/blob/main/tests/test_parser.py).

## Profile warnings or missing time series despite a successful parse

Recognizable logs include `Failed to attach ...`, `Failed references (first 5)` and
`Data file not found: ...`. The parser catches some attachment failures and warns;
returning a System does not prove that all requested profiles were attached.

First record the exact component/property and file named in the warning. In the
Python workflow, a read-only filesystem check can reproduce path resolution:

```python
from pathlib import Path

xml = Path(config.fpath)
base = Path(store.folder)
if config.timeseries_dir:
    base = base / config.timeseries_dir
print("XML:", xml.resolve())
print("Profile base:", base.resolve())
```

Append the **actual relative filename from the warning/XML** to that base and
check `Path(...).is_file()`. The parser normalizes Windows backslashes to slashes;
it does not download missing data. For a missing directory, `PLEXOSConfig` can
instead raise a Pydantic `timeseries_dir` validation error before parsing begins.

**Fix:** restore the referenced source CSVs with their original relative layout,
or set `timeseries_dir` to the existing profile directory that really contains
them. Keep a DataStore rooted at the run directory rather than relying on a file
path as the base. Do not substitute zero profiles merely to silence the warning.

**Verify:** rerun with Python logging visible:

```bash
r2x run pipeline.yaml round_trip -v --log-python
```

Repeat [component/time-series inspection](/r2x-plexos/how-to/inspect-system/). Compare
series names, timestamps, resolution and values with the source profile. An empty
`list_time_series(component)` query only rules out matching **SingleTimeSeries**;
other types require their explicit query type. The pinned fixture's first generator
has no matching single series, so zero is not a universal error condition.

Evidence: [path resolution and attachment](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/parser.py),
[invalid-DataFile regression](https://github.com/NatLabRockies/r2x-plexos/blob/main/tests/test_parser.py).
This docs update ran the missing-DataFile test; it did not recreate a production
study's unavailable external profiles.

## `contains no data for the requested year` or `Horizon start ... is before time series start`

These messages concern the selected data interval, not plugin discovery.
The parser uses the resolved Model horizon's start year when available, otherwise
its configured year. Changing `horizon_year` alone may not override an XML Model
horizon. A property can also depend on scenario, timeslice and component name.

In an in-memory Python parse, inspect the selected range and the actual series:

```python
from r2x_plexos import get_horizon
from r2x_plexos.models import PLEXOSGenerator

print("Selected horizon:", get_horizon())
for generator in system.get_components(PLEXOSGenerator):
    for series in system.list_time_series(generator):
        last = series.initial_timestamp + (len(series.data) - 1) * series.resolution
        print(generator.name, series.name)
        print(series.initial_timestamp, last, series.resolution, len(series.data))
```

Run this **before serialization**: horizon selection is module-level context, not
an archive's independent study setting. The error `Horizon end ... is after time
series end` is the corresponding upper-bound failure.

**Fix:** select the intended existing Model/scenario/year and supply profiles that
cover its timestamp grid. If a multi-column CSV reports `Component '...' not found
in parsed file ... Available components ...`, compare the reported component key
with the source column names as well as the year. Correct the source run/export or
its selection, rather than relabelling unrelated data as the requested year.

**Verify:** rerun the same parse and inspect the required series through its final
expected timestamp. The [horizon helper](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/utils_parser.py) and
[boundary tests](https://github.com/NatLabRockies/r2x-plexos/blob/main/tests/test_utils_parser.py) establish these messages;
both before-start and after-end errors were reproduced during this docs update.
A corrected synthetic interval passes. A real study's calendar recovery requires
that study's actual profile files and was not claimed here.

## `New database requires 'horizon_year'` or `Template '...' is neither ...`

The exporter needs a simulation year when creating a new database. The parser's
configuration is not automatically the exporter's configuration. Inspect the
**exporter** section with:

```bash
r2x run pipeline.yaml round_trip --print
```

For the fixture workflow, its required selection is:

```yaml
r2x-plexos.plexos-exporter:
  model_name: RoundTrip
  horizon_year: 2024
  template: PLEXOS10.0
  output_path: outputs
```

The template error means an unrecognized key was treated as a path and no such
file was found. Supported keys are `PLEXOS9.0`, `PLEXOS10.0`, `PLEXOS11.0` and
`PLEXOS12.0`; a custom XML file must exist. Inspect bundled template availability
without calling exporter internals:

```python
from r2x_plexos import PLEXOSConfig

folder = PLEXOSConfig.get_config_path()
print(folder)
print(sorted(path.name for path in folder.glob("*.xml")))
```

**Fix:** provide the intended `horizon_year` on the exporter and use a supported
key or a verified custom master XML. A template version chooses schema, not a
solver installation. Do not change simulation defaults to arbitrary values to
make a solve converge; [closed issue #62](https://github.com/NatLabRockies/r2x-plexos/issues/62) documents why Purchaser
Fixed Load and GAP defaults require model-specific review.

**Verify:** rerun into a fresh output directory, confirm the new XML exists, then
inspect Model/Scenario/Horizon settings in the target PLEXOS workflow. Missing-year
and missing-template failures were reproduced here; supplying year 2024 and a
bundled template produced `Recovered_2024.xml`. That is serialization recovery,
not a verified optimization result. See [exporter source](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/exporter.py).

## Archive loading fails

Use the portable ZIP produced by `--zip -o`, not native PLEXOS XML:

```bash
r2x read outputs/system.zip --zip --exec inspect_system.py
```

The [tutorial supplies this script](/r2x-plexos/tutorials/round-trip/#5-inspect-the-graph-not-just-the-filenames).
A module/class import failure means the reading interpreter needs the same model
package and compatible Core/Infrasys versions; use the environment checks above
and [match the writer's release to source](/r2x-plexos/reference/versions/).

If values or units differ after loading, compare a named component in memory and
in the archive using the [inspection guide](/r2x-plexos/how-to/inspect-system/). The tested
fixture retains Alta's `max_capacity` of 40.0 and 34 memberships. A historical
nested-quantity serialization issue is recorded in [closed issue #53](https://github.com/NatLabRockies/r2x-plexos/issues/53);
its closed status is not evidence that every new schema combination works. Preserve
both artifacts and report a minimal mismatch instead of editing serialized types
or quantity metadata.

If only `sys.info()` fails with `no such table: supplemental_attribute_associations`,
use the scripted component queries without `sys.info()` or `--interactive` and
preserve the failing archive. This documented
[ecosystem summary issue](https://natlabrockies.github.io/R2X/troubleshooting/#summary-fails-with-supplemental_attribute_associations)
does not establish that components are absent; the workaround does not repair
supplemental association metadata.

## Report a reproducible failure

Include these items in a [package issue](https://github.com/NatLabRockies/r2x-plexos/issues):

- Exact command or small public-API script, working directory, and failing lifecycle stage.
- `r2x --version`, `r2x python show`, and package versions from the interpreter that failed.
- Complete error and the log path printed by the CLI; remove confidential data/paths before sharing.
- Minimal native input plus required companion files, or the complete failing archive and its writer's versions.
- Expected versus observed names/counts, timestamp range, resolution and units; target PLEXOS version/template when relevant.
- Which diagnostic above passed or failed, the one change tried, and the verification result.

During this docs update, failures were reproduced on disposable fixture copies and
small synthetic inputs, and 5 focused package regressions passed. Native-file
parse/export and archive inspection were verified. Real external profile/calendar data and a licensed PLEXOS solve were not available for recovery testing.
