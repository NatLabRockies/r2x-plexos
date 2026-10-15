---
title: "Data flow and compatibility"
---

The parser opens XML through `plexosdb`, selects a Model and its scenarios/horizon, builds typed PLEXOS objects, and restores time-series references.
Memberships preserve parent/child collections; collection properties remain supplemental attributes attached to components.
The exporter starts from a bundled or custom master XML, constructs simulation configuration, writes objects and relationships, and emits XML plus referenced CSVs.

Template version selects a database schema, not a documentation release or a solver installation.
A valid export still needs inspection in the target PLEXOS workflow: a serialization round trip does not establish that two solves produce identical results.

## Package boundaries

`r2x-plexos` owns format-specific models, parser, and exporter.
[R2X Core](https://github.com/NatLabRockies/r2x-core) owns contexts, plugin lifecycle, and shared system behavior.
[r2x-cli](https://github.com/NatLabRockies/r2x-cli) owns managed Python environments, plugin discovery, and pipeline execution.
[R2X translators](https://natlabrockies.github.io/R2X/) map between formats.

See [parser source](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/parser.py), [exporter source](https://github.com/NatLabRockies/r2x-plexos/blob/main/src/r2x_plexos/exporter.py), and [configuration](/r2x-plexos/reference/) when evaluating behavior for a study.
