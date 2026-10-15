---
title: "Parse and export a PLEXOS model"
---

Complete a two-step round trip using the repository's public test fixture.
This checks the parser/exporter integration; it does not run an optimization or prove numerical equivalence.

## Prepare a working copy

Install [r2x-cli](/r2x-plexos/how-to/install/) and Git. Clone a disposable copy of this repository:

```bash
git clone https://github.com/NatLabRockies/r2x-plexos.git
cd r2x-plexos
r2x python install
r2x install r2x-plexos
r2x list r2x-plexos
```

Keep the fixture's companion files together. The XML contains model selection, memberships, and references used during parsing.

## Create the pipeline

Save this as `round-trip.yaml` in the repository root:

```yaml
variables: {}
pipelines:
  round_trip:
    - r2x-plexos.plexos-parser
    - r2x-plexos.plexos-exporter
config:
  r2x-plexos.plexos-parser:
    fpath: tests/data/5_bus_system_variables.xml
    model_name: Base
    horizon_year: 2024
  r2x-plexos.plexos-exporter:
    model_name: RoundTrip
    horizon_year: 2024
    template: PLEXOS10.0
    output_path: output
output_folder: output
```

Preview the resolved configuration and execution order:

```bash
r2x run round-trip.yaml round_trip --print
r2x run round-trip.yaml round_trip --dry-run
```

Check the input and output paths in the preview. A dry run displays the plan; it does not validate all model data.

## Execute and verify

```bash
r2x run round-trip.yaml round_trip
```

Check that the output folder contains XML and any generated time-series CSVs. Inspect the exported XML model and horizon before opening it in PLEXOS.

```bash
python -c "from pathlib import Path; files = list(Path('output').rglob('*.xml')); assert files; print(*files, sep='\n')"
```

For data inspection before export, use the [Python workflow](/r2x-plexos/how-to/python/).
If a step fails, follow [troubleshooting](/r2x-plexos/troubleshooting/) before retrying with a different input.

## Verification environment

This pipeline was executed with r2x-cli 0.1.15 and Python 3.12, using r2x-plexos 0.3.3 with its locked r2x-core 0.8.0 dependencies.
Both parser and exporter completed. Use the CLI binary’s supported Python runtime when repeating the example.
