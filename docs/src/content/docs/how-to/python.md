---
title: "Parse, inspect, and export with Python"
---

Start from the downloaded inputs in the [round-trip tutorial](/r2x-plexos/tutorials/round-trip/).
Install `r2x-plexos` into your active Python environment with
`python -m pip install r2x-plexos`. The CLI's managed environment is separate;
use its printed interpreter if you want the exact same dependencies.

The example creates a parser context, runs it, inspects its returned System,
and sends that System to a separate exporter context. Export configuration does
not retroactively change parser settings. The source XML must contain the selected Model.
Copy the console input below without `>>>`/`...` prompts into `python_roundtrip.py` in the tutorial workspace:

```python doctest
>>> from pathlib import Path
>>> from r2x_core import DataStore, PluginContext
>>> from r2x_plexos import PLEXOSConfig, PLEXOSExporter, PLEXOSParser
>>> xml_path = Path("inputs/model.xml").resolve()
>>> store = DataStore(path=xml_path.parent)
>>> context = PluginContext(
...     config=PLEXOSConfig(fpath=str(xml_path), model_name="Base", horizon_year=2024),
...     store=store,
... )
>>> context = PLEXOSParser.from_context(context).run()
>>> system = context.system
>>> if system is None:
...     raise RuntimeError("Parser returned no system")
...
>>> print(system.name)
PLEXOS
>>> export_context = PluginContext(
...     config=PLEXOSConfig(
...         model_name="RoundTrip", horizon_year=2024,
...         template="PLEXOS10.0", output_path="output",
...     ),
...     system=system,
... )
>>> _ = PLEXOSExporter.from_context(export_context).run()
>>> assert list(Path("output").rglob("*.xml"))
```

Execute with the activated interpreter:

```bash
python python_roundtrip.py
```

The script prints the system name and asserts that the native output exists.
For component counts and archive inspection, follow the
[inspection guide](/r2x-plexos/how-to/inspect-system/). To distinguish schema validation
from hook failure, read the [lifecycle contract](/r2x-plexos/reference/#construction-and-execution-signatures).

`from_context(ctx)` constructs a plugin bound to that context. `run()` returns the updated `PluginContext`; lifecycle failures raise `r2x_core.PluginError`.
Do not treat its return value as a `Result` or call individual lifecycle hooks to bypass normal ordering.
Confirm that the output exists before using it downstream.

## Select inputs explicitly

`fpath` accepts a file or a directory. Directory discovery selects the first sorted XML file; use a file path when several models are present.
A `DataStore` entry named `xml_file` takes precedence over `fpath`.
`model_name` must name a Model object in that XML. `timeseries_dir`, when supplied, must be an existing directory.

If you already manage input files through a Core store, supply a **list** of DataFile
entries. This is an alternative to `fpath`, using the `xml_path` and `store` above:

```python doctest
>>> from r2x_core import DataFile
>>> _ = store.add_data([DataFile(name="xml_file", fpath=xml_path)])
```

The parser looks up the name `xml_file`; a differently named entry is not the same
selection contract. Passing a single DataFile rather than a list does not match the
locked Core API used in the tested environment.

## Inspect components and relationships

```python doctest
>>> from r2x_plexos.models import PLEXOSGenerator, PLEXOSMembership
>>> generators = sorted(generator.name for generator in system.get_components(PLEXOSGenerator))
>>> assert len(generators) == 9 and "Alta" in generators
>>> memberships = sorted(
...     (membership.parent_object.name, membership.child_object.name, str(membership.collection))
...     for membership in system.get_supplemental_attributes(PLEXOSMembership)
... )
>>> assert len(memberships) == 34
>>> print(memberships[:3])
[('Alta', 'node_a', 'Nodes'), ('Base', 'Base', 'Horizon'), ('Brighton', 'node_e', 'Nodes')]
```

Memberships and collection properties are supplemental attributes rather than ordinary generator/node components.
Consult their [definitions](https://github.com/NatLabRockies/r2x-plexos/tree/main/src/r2x_plexos/models) before editing associations.
Select a bundled template (`PLEXOS9.0`, `PLEXOS10.0`, `PLEXOS11.0`, or `PLEXOS12.0`) or pass a custom XML template path.
Verify generated files with the target PLEXOS version before a production solve.
