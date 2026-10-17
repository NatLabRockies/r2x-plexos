---
title: "Verify installation and first use"
---

Check the environment you [installed into](/r2x-plexos/getting-started/installation/)
before opening a model. These checks need no input dataset and do not run a solve.

## Verify the Python library

Use the activated interpreter to print the installed distribution version:

```bash
python -c "from importlib.metadata import version; print(version('r2x-plexos'))"
```

Expect a version string. Copy the console input below without prompts into
`verify_installation.py`:

```python doctest
>>> from r2x_plexos import PLEXOSConfig, PLEXOSParser, PLEXOSExporter
>>> config = PLEXOSConfig(model_name="Base", horizon_year=2024)
>>> assert (config.model_name, config.horizon_year) == ("Base", 2024)
>>> assert PLEXOSParser.__name__ == "PLEXOSParser"
>>> assert PLEXOSExporter.__name__ == "PLEXOSExporter"
```

Run it in the same environment:

```bash
python verify_installation.py
```

Successful execution produces no output: all assertions pass.

This confirms that the public classes import and that basic configuration
validation works. It does not validate an external XML/JSON model or any profile data.
If an import fails, compare `python -c "import sys; print(sys.executable)"` with the
interpreter used for installation. See [environment setup](/r2x-plexos/getting-started/environment/).

## Verify CLI discovery

```bash
r2x list r2x-plexos
```

Confirm that `plexos-parser` and `plexos-exporter` are listed. Inspect the installed
parser's generated options:

```bash
r2x run plexos-parser --help
```

You should see `--fpath`, `--model-name` and `--horizon-year`
configuration options. Missing entry points usually indicate a different runtime
environment; follow [plugin discovery troubleshooting](/r2x-plexos/troubleshooting/#plugin-is-missing).

## Run the first model

Continue with [read, inspect, and export a model](/r2x-plexos/tutorials/round-trip/).
That tutorial provides pinned data, a two-step pipeline, expected component counts,
and a saved archive you can inspect. For application code, use
[Python contexts](/r2x-plexos/how-to/python/); for exact fields and lifecycle returns,
use the [API reference](/r2x-plexos/reference/).
