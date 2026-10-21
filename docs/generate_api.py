"""Generate source-linked public API contracts for the Starlight site."""

from __future__ import annotations

import importlib
import inspect
import json
import re
import shutil
import subprocess
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[1]


class TocItem(TypedDict):
    """One native Starlight navigation entry sourced from Sphinx's contents tree."""

    text: str
    slug: str
    depth: int
    children: list[TocItem]


class SphinxContents(HTMLParser):
    """Retain Sphinx's section/class/member hierarchy without interpreting signatures."""

    def __init__(self) -> None:
        super().__init__()
        self.items: list[TocItem] = []
        self.stack: list[list[TocItem]] = []
        self.active: TocItem | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "ul":
            children: list[TocItem] = []
            if self.stack and self.stack[-1]:
                self.stack[-1][-1]["children"] = children
            else:
                self.items = children
            self.stack.append(children)
        elif tag == "a" and self.stack:
            href = dict(attrs).get("href") or ""
            self.active = TocItem(
                text="", slug=href.removeprefix("#"), depth=min(len(self.stack), 6), children=[]
            )
            self.stack[-1].append(self.active)

    def handle_data(self, data: str) -> None:
        if self.active is not None:
            self.active["text"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.active is not None:
            self.active["text"] = " ".join(self.active["text"].split())
            self.active = None
        elif tag == "ul" and self.stack:
            self.stack.pop()

    def navigation(self) -> list[TocItem]:
        """Omit the duplicated page-title entry, preserving every anchored descendant."""
        return [child for item in self.items for child in ([item] if item["slug"] else item["children"])]


def main() -> None:
    """Render the installed checkout's supported exports and retain member anchors."""
    package = ROOT.name.replace("-", "_")
    module = importlib.import_module(package)
    source = ROOT / "docs/api-source"
    output = ROOT / "docs/build/api-json"
    source.mkdir(parents=True, exist_ok=True)
    if output.exists():
        shutil.rmtree(output)
    lines = [
        "Python API",
        "==========",
        "",
        "Public exports from ``" + package + "``. Select a class or member in the contents tree.",
        "",
    ]
    for name in module.__all__:
        obj = getattr(module, name)
        if name == "__version__":
            continue
        lines.extend([name, "-" * len(name), ""])
        directive = (
            "autoclass" if inspect.isclass(obj) else "autofunction" if inspect.isfunction(obj) else "autodata"
        )
        lines.extend([f".. {directive}:: {package}.{name}"])
        if inspect.isclass(obj):
            lines.extend(["   :members:", "   :undoc-members:"])
        lines.append("")
        if name.endswith(("Parser", "Exporter")):
            for method in ("from_context", "run"):
                lines.extend([f".. automethod:: {package}.{name}.{method}", ""])
    (source / "index.rst").write_text("\n".join(lines), encoding="utf-8")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-E",
            "-d",
            str(output.parent / "doctrees"),
            "-b",
            "json",
            "-W",
            "-q",
            str(source),
            str(output),
        ],
        cwd=ROOT,
        check=True,
    )
    data = json.loads((output / "index.fjson").read_text(encoding="utf-8"))
    body = re.sub(r"<h1\b[^>]*>.*?</h1>", "", data["body"], count=1, flags=re.S)
    body = body.replace('href="index.html#', 'href="#')
    # Sphinx's automatic class/member IDs also preserve former Sphinx API fragments.
    parser = SphinxContents()
    parser.feed(data["toc"])
    navigation = {"reference/api": parser.navigation()}
    generated = ROOT / "docs/src/generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "api-toc.json").write_text(json.dumps(navigation, indent=2) + "\n", encoding="utf-8")
    page = ROOT / "docs/src/content/docs/reference/api.md"
    legacy_sections = {
        "api-reference": None,
        "parser": "plexosparser",
        "configuration": "plexosconfig",
        "see-also": None,
    }
    for anchor, section in legacy_sections.items():
        if f'id="{anchor}"' in body:
            continue
        marker = f'<span id="{anchor}"></span>\n'
        heading = f'<section id="{section}">'
        body = body.replace(heading, heading + marker, 1) if section and heading in body else marker + body
    page.write_text(
        '---\ntitle: Python API\neditUrl: false\n---\n\n<div class="api-contracts">\n' + body + "\n</div>\n",
        encoding="utf-8",
    )
    print(f"Generated {len(navigation['reference/api'])} public API sections for {ROOT.name}.")


if __name__ == "__main__":
    main()
