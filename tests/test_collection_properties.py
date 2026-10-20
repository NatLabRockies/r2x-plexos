"""Test variable resolution with constant values."""

from pathlib import Path
from typing import cast

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataFile, DataStore, PluginContext
from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.models import CollectionProperties, PLEXOSRegion


@pytest.fixture
def xml_with_variables(tmp_path):
    """Create a test XML with a generator that has max capacity referencing a variable."""
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    datafile_path = tmp_path / "generator_capacity.csv"
    datafile_path.write_text("Name,Value\nTestBattery,100.0\n")
    region_name = "Region"
    region_id = db.add_object(ClassEnum.Region, region_name)
    reserve_name = "RegionSpinning"
    _ = db.add_object(ClassEnum.Reserve, reserve_name)
    db.add_membership(ClassEnum.Reserve, ClassEnum.Region, reserve_name, region_name, CollectionEnum.Regions)

    variable_name = "MinReserveMargin"
    variable_id = db.add_object(ClassEnum.Variable, variable_name)
    variable_prop_id = db.add_property(
        ClassEnum.Variable,
        variable_name,
        "Profile",
        value=3.0,
    )
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id,action_id) VALUES (?,?,?)", (region_id, variable_prop_id, 1)
    )
    db._db.execute(
        "INSERT INTO t_band(band_id,data_id) VALUES (?,?)",
        (
            1,
            variable_prop_id,
        ),
    )

    load_risk = db.add_property(
        ClassEnum.Region,
        region_name,
        "Load Risk",
        value=0.0,  # Placeholder when using datafile+variable
        collection_enum=CollectionEnum.Regions,
        parent_class_enum=ClassEnum.Reserve,
        parent_object_name=reserve_name,
    )
    db._db.execute("INSERT INTO t_band(band_id,data_id) VALUES (?,?)", (1, load_risk))
    db._db.execute(
        "INSERT INTO t_tag(object_id,data_id, action_id) VALUES (?,?,?)",
        (variable_id, load_risk, 1),
    )

    xml_path = tmp_path / "collection_properties.xml"
    db.to_xml(xml_path)

    return xml_path, db


def test_battery_capacity_with_constant_variable(xml_with_variables, tmp_path, caplog):
    """Test generator max_capacity computed as base_value * variable_value."""
    xml_path, db = xml_with_variables

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data([data_file], overwrite=True)

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    parser.db = db

    result = parser.run()
    sys = result.system
    assert sys is not None

    region = sys.get_component(PLEXOSRegion, "Region")

    assert sys.has_supplemental_attribute(region)
    assert len(sys.get_supplemental_attributes_with_component(region, CollectionProperties)) == 1
    sup = sys.get_supplemental_attributes_with_component(region, CollectionProperties)[0]
    assert "load_risk" in sup.properties
    assert sup.properties["load_risk"].get_value() == 0.0


def test_collection_properties_match_membership_per_parent(tmp_path, monkeypatch):
    """Each parent's collection property attaches to its own membership, in linear time."""
    from r2x_plexos.models import PLEXOSMembership

    n_parents = 60
    db: PlexosDB = PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))
    db.add_object(ClassEnum.Region, "Region")
    for i in range(n_parents):
        reserve_name = f"Reserve{i}"
        db.add_object(ClassEnum.Reserve, reserve_name)
        db.add_membership(ClassEnum.Reserve, ClassEnum.Region, reserve_name, "Region", CollectionEnum.Regions)
        db.add_property(
            ClassEnum.Region,
            "Region",
            "Load Risk",
            value=float(i),
            collection_enum=CollectionEnum.Regions,
            parent_class_enum=ClassEnum.Reserve,
            parent_object_name=reserve_name,
        )
    xml_path = tmp_path / "many_parents.xml"
    db.to_xml(xml_path)

    parent_lookups = 0
    original_getattribute = PLEXOSMembership.__getattribute__

    def counting_getattribute(self, name):
        nonlocal parent_lookups
        if name == "parent_object":
            parent_lookups += 1
        return original_getattribute(self, name)

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    store = DataStore(path=tmp_path)
    store.add_data([DataFile(name="xml_file", fpath=xml_path)], overwrite=True)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(PluginContext(config=config, store=store)))
    parser.db = db
    monkeypatch.setattr(PLEXOSMembership, "__getattribute__", counting_getattribute)
    sys = parser.run().system
    monkeypatch.undo()

    region = sys.get_component(PLEXOSRegion, "Region")
    attached = sys.get_supplemental_attributes_with_component(region, CollectionProperties)
    values_by_parent = {
        sup.membership.parent_object.name: sup.properties["load_risk"].get_value() for sup in attached
    }
    assert values_by_parent == {f"Reserve{i}": float(i) for i in range(n_parents)}
    # Scanning every membership per parent costs about n_parents**2 / 2 lookups.
    assert parent_lookups < n_parents**2 / 4, parent_lookups
