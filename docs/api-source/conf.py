"""Internal API extraction only. The public site is Astro/Starlight."""

project = "r2x-plexos"
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.linkcode",
    "sphinxcontrib.autodoc_pydantic",
]
source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
myst_all_links_external = True
autosummary_generate = True
highlight_language = "python"
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_typehints_format = "short"
autoclass_content = "both"
exclude_patterns = ["_static"]
root_doc = "index"


# Pin source links to the checkout used for this documentation build.
def linkcode_resolve(domain, info):
    import inspect
    import subprocess
    from importlib import import_module
    from pathlib import Path

    if domain != "py" or not info.get("module"):
        return None
    try:
        obj = import_module(info["module"])
        for part in info["fullname"].split("."):
            obj = getattr(obj, part)
        obj = inspect.unwrap(obj)
        source = Path(inspect.getsourcefile(obj)).resolve()
        root = Path(__file__).resolve().parents[2]
        if obj.__module__.startswith("r2x_core."):
            module_path = obj.__module__.replace(".", "/") + ".py"
            return "https://github.com/NatLabRockies/r2x-core/blob/main/src/" + module_path
        relative = "src/" + source.relative_to(root / "src").as_posix()
        lines, start = inspect.getsourcelines(obj)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    except (AttributeError, ImportError, OSError, TypeError, ValueError, subprocess.CalledProcessError):
        return None
    return f"https://github.com/NatLabRockies/r2x-plexos/blob/{revision}/{relative}#L{start}-L{start + len(lines) - 1}"


def keep_inherited_contract(_app, _what, name, obj, _options, lines):
    """Keep dependency contracts while linking to the package's tested usage example."""
    if not name.endswith((".from_context", ".run")) or not obj.__module__.startswith("r2x_core."):
        return
    if "Examples" in lines:
        del lines[lines.index("Examples") :]
    lines.extend(
        ["", "For a complete tested example, use the `Python workflow </r2x-plexos/how-to/python/>`_."]
    )


def setup(app):
    app.connect("autodoc-process-docstring", keep_inherited_contract)
