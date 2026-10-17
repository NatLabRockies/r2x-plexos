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
