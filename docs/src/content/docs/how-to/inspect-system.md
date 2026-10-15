---
title: "Inspect an archive before using the export"
---

Start from `outputs/system.zip` in the [round-trip tutorial](/r2x-plexos/tutorials/round-trip/).
This is an **Infrasys archive**, not PLEXOS XML.
Keep `r2x-plexos` installed in the environment that reads it; serialized components
refer to Python model modules and class names.

## Repeat component and relationship checks

Save the following as `inspect_details.py` and run it from the tutorial directory:

```python
from r2x_plexos.models import PLEXOSGenerator, PLEXOSMembership

generators = list(sys.get_components(PLEXOSGenerator))
if generators:
    generator = generators[0]
    print("Name:", generator.name)
    print("Example fields:", list(type(generator).model_fields)[:8])
    print("Capacity:", generator.max_capacity)
    series_list = sys.list_time_series(generator)
    print("Matching single series:", len(series_list))
    for series in series_list:
        print(series.name, series.initial_timestamp, series.resolution)
        print(series.data[:3])

memberships = list(sys.get_supplemental_attributes(PLEXOSMembership))
for membership in memberships[:3]:
    print(membership.parent_object.name, membership.collection, membership.child_object.name)
```

```bash
r2x read outputs/system.zip --zip --exec inspect_details.py
```

On the fixture, the first generator is Alta and its selected `max_capacity` is `40.0`. The default single-series query returns zero for that generator. The membership rows show the actual parent, collection and child names. A supplemental attribute count is separate from the ordinary component count.
`list_time_series` queries single time series by default. An empty result means
no matching series is attached to that component, not that every series in the
system is absent. Other time-series types need their corresponding API argument.
See [Infrasys System source](https://github.com/NatLabRockies/infrasys/blob/main/src/infrasys/system.py)
for supported filters and serialization behavior.

## Load the same archive from Python

Use the Python executable shown by `r2x python show`, or a separate environment
with compatible `r2x-plexos` and Infrasys versions. Save this as `load_archive.py`:

```python
from infrasys import Component, System

system = System.load("outputs/system.zip")
print("Components:", len(list(system.get_components(Component))))
```

Run it with that interpreter. For example, in an activated compatible environment:

```bash
python load_archive.py
```

Expect 37 components for the pinned tutorial fixture.
`System.load` extracts and restores the archive's storage. An Infrasys JSON created
with `system.to_json(...)` instead uses `System.from_json(...)` and needs its
sibling storage directory. A filename extension alone does not identify the schema.

## Decide whether the result is usable

Check names/counts against the input, component fields against schema/unit
expectations, and profiles against intended length and resolution.
Inspect Model/Scenario/Horizon objects and generated DataFile references in the native XML before a PLEXOS solve.
For a translation, compare mappings with the relevant [R2X property reference](https://natlabrockies.github.io/R2X/reference/python-api/).
For import/schema/storage errors, follow [troubleshooting](/r2x-plexos/troubleshooting/).
