---
title: "Parse, inspect, and export with Python"
---

Run this example from a disposable repository checkout after installing `r2x-plexos` into your active Python environment.
The fixture and its companions are included under `tests/data/`.
Use a real Model object name from the XML when replacing the fixture.

```python
from pathlib import Path

from r2x_core import DataStore, PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSExporter, PLEXOSParser

xml_path = Path("tests/data/5_bus_system_variables.xml").resolve()
store = DataStore(path=xml_path.parent)
context = PluginContext(
    config=PLEXOSConfig(fpath=str(xml_path), model_name="Base", horizon_year=2024),
    store=store,
)
context = PLEXOSParser.from_context(context).run()
system = context.system
if system is None:
    raise RuntimeError("Parser returned no system")
print(system.name)

export_context = PluginContext(
    config=PLEXOSConfig(
        model_name="RoundTrip", horizon_year=2024,
        template="PLEXOS10.0", output_path="output",
    ),
    system=system,
)
PLEXOSExporter.from_context(export_context).run()
assert list(Path("output").rglob("*.xml"))
```

`from_context(ctx)` constructs a plugin bound to that context. `run()` returns the updated `PluginContext`; lifecycle failures raise `r2x_core.PluginError`.
Do not treat its return value as a `Result` or call individual lifecycle hooks to bypass normal ordering.
Confirm that the output exists before using it downstream.

## Select inputs explicitly

`fpath` accepts a file or a directory. Directory discovery selects the first sorted XML file; use a file path when several models are present.
A `DataStore` entry named `xml_file` takes precedence over `fpath`.
`model_name` must name a Model object in that XML. `timeseries_dir`, when supplied, must be an existing directory.

## Inspect components and relationships

```python
from r2x_plexos.models import PLEXOSGenerator, PLEXOSMembership

for generator in system.get_components(PLEXOSGenerator):
    print(generator.name)
for membership in system.get_supplemental_attributes(PLEXOSMembership):
    print(membership.parent_object, membership.child_object, membership.collection)
```

Memberships and collection properties are supplemental attributes rather than ordinary generator/node components.
Consult their [definitions](https://github.com/NatLabRockies/r2x-plexos/tree/main/src/r2x_plexos/models) before editing associations.
Select a bundled template (`PLEXOS9.0`, `PLEXOS10.0`, `PLEXOS11.0`, or `PLEXOS12.0`) or pass a custom XML template path.
Verify generated files with the target PLEXOS version before a production solve.
