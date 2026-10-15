---
title: "Installation"
---

Install the published `r2x-plexos` package for your workflow. It supports
Python 3.11, 3.12, and 3.13 according to [package metadata](https://github.com/NatLabRockies/r2x-plexos/blob/main/pyproject.toml).
Use [environment setup](/r2x-plexos/getting-started/environment/) first if you need a
Python environment or the `r2x` executable.

## Use the Python library

With your supported Python environment activated:

```bash
python -m pip install r2x-plexos
```

This installs the format package and its dependencies, including
[R2X Core](https://github.com/NatLabRockies/r2x-core) and
[Infrasys](https://github.com/NatLabRockies/infrasys).
The PLEXOS XML database interface is supplied by [plexosdb](https://github.com/NatLabRockies/plexosdb).
Published distributions and their Python requirements are on
[PyPI](https://pypi.org/project/r2x-plexos/).

## Use the CLI plugin

After installing [r2x-cli](https://github.com/NatLabRockies/r2x-cli#quick-start)
and preparing its managed runtime:

```bash
r2x install r2x-plexos
```

The CLI installs into its own managed environment. A package installed with pip
in another environment is not automatically available to the CLI. You only need
to follow both installation routes when using both environments.

Continue with [verify installation and first use](/r2x-plexos/getting-started/verify/).
For an existing artifact, compare its writer's package versions with
[release source](/r2x-plexos/reference/versions/) before changing dependencies.

## Develop the package

Source checkouts, `uv sync`, tests and documentation authoring belong to the
separate [contributor guide](/r2x-plexos/contributors/). A source checkout is not
required for normal package installation or the downloaded-fixture tutorial.
