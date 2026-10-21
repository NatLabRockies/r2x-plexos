---
title: "Environment and prerequisites"
---

Choose the environment that will execute your workflow. The Python library and
the CLI share model APIs, but their installed packages need not be the same.

| Workflow | Environment to prepare | Next step |
| --- | --- | --- |
| Python scripts or notebooks | An isolated Python 3.11–3.13 environment. | [Install the library](/r2x-plexos/getting-started/installation/#use-the-python-library). |
| `r2x` pipelines | The runtime managed by [r2x-cli](https://github.com/NatLabRockies/r2x-cli). | [Install the CLI plugin](/r2x-plexos/getting-started/installation/#use-the-cli-plugin). |

## Python scripts and notebooks

Confirm that the interpreter you intend to use is supported:

```bash
python --version
```

If this reports an unsupported version, select an installed Python 3.11, 3.12,
or 3.13 interpreter before creating the environment. Create a virtual environment
in your project directory:

```bash
python -m venv .venv
```

Activate it on macOS/Linux:

```bash
source .venv/bin/activate
```

Or activate it in Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Confirm where Python is running before installing packages:

```bash
python -c "import sys; print(sys.executable)"
```

The path should point inside this `.venv`. For notebooks, select the corresponding
kernel so imports and package installation use the same interpreter.

## CLI pipelines

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and the
[r2x-cli binary](https://github.com/NatLabRockies/r2x-cli#quick-start).
Confirm the executable is available:

```bash
r2x --version
```

Prepare the Python runtime required by that CLI binary:

```bash
r2x python install
```

Inspect its actual version and environment path:

```bash
r2x python show
```

The CLI binary's Python ABI must match its managed runtime. The tutorials were
executed with CLI 0.1.15 and its Python 3.12 runtime. Changing a project virtual
environment does not change the binary's ABI; see
[CLI runtime guidance](https://github.com/NatLabRockies/r2x-cli/blob/main/docs/development.md)
if startup reports an ABI mismatch.

## Data prerequisites

You do not need study data to [verify installation](/r2x-plexos/getting-started/verify/).
The [tutorial](/r2x-plexos/tutorials/round-trip/) downloads pinned public fixture data
using `curl` and explains its companion files. It needs network access and a new
working directory. XML parsing does not need a licensed PLEXOS solver; running an optimization is a separate workflow.
