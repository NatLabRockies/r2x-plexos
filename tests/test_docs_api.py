"""Regression checks for the generated API member tree."""

import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "generate_api", Path(__file__).resolve().parents[1] / "docs/generate_api.py"
)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
SphinxContents = module.SphinxContents


class ApiNavigationTest(unittest.TestCase):
    def test_contents_preserve_section_class_and_member_anchors(self) -> None:
        parser = SphinxContents()
        parser.feed(
            '<ul><li><a href="#">API</a><ul><li><a href="#system">Example</a><ul>'
            '<li><a href="#r2x_plexos.Example"><code>Example</code></a><ul><li>'
            '<a href="#r2x_plexos.Example.to_json">Example.to_json()</a>'
            "</li></ul></li></ul></li></ul></li></ul>"
        )
        section = parser.navigation()[0]
        self.assertEqual(section["slug"], "system")
        component = section["children"][0]
        self.assertEqual(component["slug"], "r2x_plexos.Example")
        self.assertEqual(component["children"][0]["slug"], "r2x_plexos.Example.to_json")
        self.assertEqual(component["children"][0]["text"], "Example.to_json()")


def test_source_links_keep_dependency_ownership() -> None:
    """Installed dependency methods must not become nonexistent local .venv links."""
    config_path = Path(__file__).resolve().parents[1] / "docs/api-source/conf.py"
    config_spec = importlib.util.spec_from_file_location("api_conf", config_path)
    assert config_spec is not None and config_spec.loader is not None
    config = importlib.util.module_from_spec(config_spec)
    config_spec.loader.exec_module(config)
    link = config.linkcode_resolve("py", {"module": "r2x_plexos", "fullname": "PLEXOSParser.from_context"})
    assert link == "https://github.com/NatLabRockies/r2x-core/blob/main/src/r2x_core/plugin_base.py"
    local = config.linkcode_resolve("py", {"module": "r2x_plexos", "fullname": "PLEXOSParser"})
    assert "/src/r2x_plexos/parser.py#L" in local
    assert "/.venv/" not in local
