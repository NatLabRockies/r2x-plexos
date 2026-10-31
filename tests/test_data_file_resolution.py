"""Test variable resolution with constant values."""

from typing import cast

from r2x_core import DataStore, PluginContext
from r2x_plexos.models import PLEXOSRegion
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.parser import PLEXOSParser
from r2x_plexos.plugin_config import PLEXOSConfig


def test_multi_band_datafile(tmp_path, db_with_multiband_variable, caplog):
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    sys = result.system
    assert sys is not None

    # Variable inspection
    variable_component = sys.get_component(PLEXOSDatafile, "LoadProfiles")
    prop_value = variable_component.get_property_value("filename")

    entry = prop_value.get_entry()
    assert entry is not None
    assert entry.scenario_name == "scenario_2"
    assert entry.text is not None
    assert "Load_" in entry.text
    assert prop_value.has_datafile()
    assert prop_value.has_scenarios()

    regions_to_inspect = ["r1", "r2"]
    for region in regions_to_inspect:
        region_component = sys.get_component(PLEXOSRegion, region)
        assert isinstance(region_component, PLEXOSRegion)
        prop_value = region_component.get_property_value("load")
        assert prop_value.has_variable()

        assert sys.has_time_series(region_component)
        assert len(sys.list_time_series(region_component)) == 3

        band_1_ts = sys.get_time_series(region_component, band="1")
        assert len(band_1_ts.data) == 8784
        assert all(val != 0 for val in band_1_ts.data[:24])


def test_xml_mapping_glob_and_ambiguity(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from r2x_core import DataFile, Err

    xml = tmp_path / "model.xml"
    xml.write_text("<root/>")
    store = DataStore(tmp_path)
    store.add_data([DataFile(name="xml_file", glob="*.xml")])
    parser = cast(
        PLEXOSParser,
        PLEXOSParser.from_context(PluginContext(config=PLEXOSConfig(model_name="Base"), store=store)),
    )
    from_xml = Mock(return_value=object())
    monkeypatch.setattr("r2x_plexos.parser.PlexosDB.from_xml", from_xml)
    monkeypatch.setattr(parser, "validate_inputs", lambda: Err("stop after resolution"))
    assert parser.on_build().is_err()
    from_xml.assert_called_once_with(xml)

    (tmp_path / "second.xml").write_text("<root/>")
    parser.db = None
    result = parser.on_build()
    assert result.is_err()
    assert "Multiple files matched" in result.err()
