---
title: "Supplemental Attributes in r2x-plexos"
---

## Overview

Supplemental attributes in [`infrasys`](https://nrel.github.io/infrasys/index.html) are classes that inherit from `SupplementalAttribute` and store information about a system that doesn't fit the traditional component model. Unlike components:

- **Many-to-many relationships**: A supplemental attribute can be associated with multiple components, and a component can have multiple supplemental attributes
- **Not standalone**: They must be attached to at least one component
- **Have UUIDs**: Each supplemental attribute instance has a unique identifier
- **Can have time series**: Like components, supplemental attributes can have time series data attached

## Why r2x-plexos Uses Supplemental Attributes

In PLEXOS, relationships between objects (memberships) are modeled as supplemental attributes because:

1. **Bidirectional associations**: A membership connects two objects (parent and child), and both objects need access to the relationship
2. **Not independent entities**: Memberships only exist in the context of the objects they connect
3. **Excluded from component operations**: Memberships are explicitly filtered out when iterating over components for export

## Supplemental Attributes in r2x-plexos

r2x-plexos defines two supplemental attribute classes:

### 1. PLEXOSMembership

Defines functional and logical relationships between PLEXOS objects.

**Attributes:**
- `parent_object`: The parent component in the relationship
- `child_object`: The child component in the relationship
- `collection`: The PLEXOS collection name defining the relationship type
- `membership_id`: Unique identifier for the membership

**Example relationships:**
- Generators belonging to regions
- Buses belonging to nodes
- Fuels used by generators

**How it's stored:** both endpoints share one membership. This complete example
creates two ordinary components, attaches the membership, and checks both directions:

```python doctest
>>> from r2x_core import System
>>> from r2x_plexos import PLEXOSPropertyValue
>>> from r2x_plexos.models import PLEXOSGenerator, PLEXOSNode, PLEXOSMembership
>>> from r2x_plexos.models.collection_property import CollectionProperties
>>> system = System(name="Membership example")
>>> parent = PLEXOSGenerator(name="Generator")
>>> child = PLEXOSNode(name="Node")
>>> _ = system.add_components(parent, child)
>>> membership = PLEXOSMembership(parent_object=parent, child_object=child)
>>> _ = system.add_supplemental_attribute(parent, membership)
>>> _ = system.add_supplemental_attribute(child, membership)
>>> assert len(list(system.get_supplemental_attributes(PLEXOSMembership))) == 1
>>> assert len(system.get_components_with_supplemental_attribute(membership)) == 2
```

### 2. CollectionProperties

A collection-property attribute retains a membership, collection name, and a
mapping of property names to entry-based values. Continue in the same console:

```python doctest
>>> properties = CollectionProperties(
...     membership=membership,
...     collection_name="Nodes",
...     properties={"Load": PLEXOSPropertyValue.from_dict({"value": 10.0})},
... )
>>> _ = system.add_supplemental_attribute(child, properties)
>>> assert system.get_num_supplemental_attributes() == 2
```

## Working with Supplemental Attributes

### Querying Supplemental Attributes

The same System can resolve attached membership attributes without inspecting private managers:

```python doctest
>>> memberships = list(system.get_supplemental_attributes_with_component(
...     child, supplemental_attribute_type=PLEXOSMembership,
... ))
>>> assert memberships == [membership]
>>> assert system.has_supplemental_attribute(child, supplemental_attribute_type=PLEXOSMembership)
```

### Retrieving by UUID

Use the UUID of the attribute itself, rather than an endpoint component UUID:

```python doctest
>>> assert system.get_supplemental_attribute_by_uuid(membership.uuid) is membership
```

## Key Differences from Components

| Aspect | Component | Supplemental Attribute |
|--------|-----------|----------------------|
| **Storage** | Component manager | Supplemental attribute manager |
| **Relationships** | One-to-many (composed in other components) | Many-to-many (associated with multiple components) |
| **Iteration** | `system.get_components()` | `system.get_supplemental_attributes()` |
| **Export behavior** | Included in component exports | Excluded from component iteration (see `exporter.py`) |
| **Attachment** | Exists independently in system | Must be attached to at least one component |

## Implementation Notes

- Supplemental attributes are stored separately from components in the system
- When exporting, `PLEXOSMembership` is explicitly excluded from component type iteration
- Both `PLEXOSMembership` and `CollectionProperties` can be queried through the standard `infrasys` supplemental attribute API
- The system tracks bidirectional associations between components and supplemental attributes
