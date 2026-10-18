"""Check every maintained legacy fragment mapping against the rendered site."""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent
BASE = "/" + ROOT.parent.name + "/"


class Identifiers(HTMLParser):
    """Collect actual rendered IDs, including generated API members."""

    def __init__(self) -> None:
        super().__init__()
        self.identifiers: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if identifier := attributes.get("id"):
            self.identifiers.add(identifier)


def main() -> None:
    """Fail the docs build if a mapped legacy heading loses its destination."""
    checked = 0
    for redirect in (ROOT / "public").rglob("*.html"):
        match = re.search(r"const fragments=(.*?);const target=", redirect.read_text())
        if not match:
            continue
        for target in json.loads(match.group(1)).values():
            url = urlsplit(target)
            if url.netloc:
                assert url.netloc == "github.com" and "/blob/main/CHANGELOG.md" in url.path
                checked += 1
                continue
            assert url.path.startswith(BASE), target
            page = ROOT / "_build" / url.path.removeprefix(BASE) / "index.html"
            assert page.is_file(), (redirect, target)
            identifiers = Identifiers()
            identifiers.feed(page.read_text())
            assert not url.fragment or unquote(url.fragment) in identifiers.identifiers, (redirect, target)
            checked += 1
    assert checked > 0
    print(f"Verified {checked} legacy fragment destinations.")


if __name__ == "__main__":
    main()
