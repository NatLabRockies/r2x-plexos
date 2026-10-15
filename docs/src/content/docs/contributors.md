---
title: "Contributors"
---

## Prepare the environment

From a source checkout, install the development environment:

```bash
uv sync --all-groups
```

The repository's [hook configuration](https://github.com/NatLabRockies/r2x-plexos/blob/main/.pre-commit-config.yaml) defines format, lint, type, and test checks.
Run the applicable hooks on your changed files before review:

```bash
uv run prek run --all-files --hook-stage pre-push
```

## Build documentation

Use Node.js 22.12 or newer. From the repository root:

```bash
cd docs
npm ci
npm run build
npm run preview -- --port 4331
```

Open the printed URL including `/r2x-plexos/`. The build writes `docs/_build/` and creates the Pagefind search index.
Check sidebar navigation, search for a package-specific term, and follow any legacy `.html` URL affected by your edit.
Use an available port if another preview is running.

Edit pages in `docs/src/content/docs/`. Keep commands tied to current source and preserve redirects when moving existing URLs.
Add documentation archives only when real historical builds are available.

Report bugs through [issues](https://github.com/NatLabRockies/r2x-plexos/issues); contributions use [pull requests](https://github.com/NatLabRockies/r2x-plexos/pulls).
