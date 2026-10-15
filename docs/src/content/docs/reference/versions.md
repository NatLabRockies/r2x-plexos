---
title: "Browse package versions"
---

This site documents current `main` and does not publish a separate documentation build for every release.

- [Release notes](https://github.com/NatLabRockies/r2x-plexos/releases) describe packaged changes.
- [Git tags](https://github.com/NatLabRockies/r2x-plexos/tags) let you browse the README, docs, and API source at an actual release.
- [PyPI package](https://pypi.org/project/r2x-plexos/) lists published distributions and Python requirements.
- [Canonical changelog](https://github.com/NatLabRockies/r2x-plexos/blob/main/CHANGELOG.md) records release history.

Check the version in the Python environment used by your workflow:

```bash
python -c "from importlib.metadata import version; print(version('r2x-plexos'))"
```

For CLI installations, use `r2x list r2x-plexos` to inspect the managed plugin.
Choose the matching tag on GitHub before comparing configuration or signatures; current documentation can include unreleased changes.
