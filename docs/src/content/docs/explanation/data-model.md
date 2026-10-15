---
title: Data model
---

PLEXOS object classes model generators, nodes, lines, storage, regions, reserves,
fuels, scenarios, and other XML objects. See the [model exports](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/models/__init__.py)
for the supported names.

## Properties and selection

`PLEXOSPropertyValue` stores entries keyed by scenario, band, timeslice, dates,
and other PLEXOS dimensions. It is not a single numeric `value` constructor.
Use `from_dict` for a small property specification:

```python
from r2x_plexos import PLEXOSPropertyValue

property_value = PLEXOSPropertyValue.from_dict({"value": 500.0, "scenario": "Base"})
```

The [property implementation](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/models/property.py)
defines entry selection and supported arithmetic. Scenario priority and horizon
contexts affect which entries participate in a selected property value.

## Relationships and time series

`PLEXOSMembership` links a parent object, child object, and collection.
`CollectionProperties` requires a membership, collection name, and dictionary
of property names to `PLEXOSPropertyValue` instances. Both are supplemental
attributes, preserving relationships independently of ordinary components.

Time-series metadata links profile data to objects and property names.
The exporter writes profile CSVs and DataFile references alongside XML.
Keep the resulting files together; XML alone may not contain all input data.

Use the [Python workflow](/r2x-plexos/how-to/python/) to inspect a parsed system.
