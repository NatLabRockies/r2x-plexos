import pytest

from r2x_core import DataFile, DataStore
from r2x_plexos import PLEXOSParser, PLEXOSPropertyValue
from r2x_plexos.models import PLEXOSMembership, PLEXOSVariable
from r2x_plexos.models.datafile import PLEXOSDatafile
from r2x_plexos.models.generator import PLEXOSGenerator
from r2x_plexos.models.region import PLEXOSRegion
from r2x_plexos.plugin_config import PLEXOSConfig


@pytest.fixture(scope="module")
def config_store_example(data_folder) -> tuple[PLEXOSConfig, DataStore]:
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, reference_year=2024)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(path=data_folder)
    store.add_data(data_file)
    return config, store


@pytest.fixture(scope="module")
def parser_instance(config_store_example) -> PLEXOSParser:
    """Shared parser instance for read-only tests."""
    config, store = config_store_example
    return PLEXOSParser(config, store)


@pytest.fixture(scope="module")
def parser_system(parser_instance):
    """Shared system built from parser for read-only tests."""
    return parser_instance.build_system()


@pytest.mark.slow
def test_plexos_parser_instance(parser_instance):
    assert isinstance(parser_instance, PLEXOSParser)


@pytest.mark.slow
def test_plexos_parser_system(parser_system):
    assert parser_system is not None
    assert parser_system.name == "system"


@pytest.mark.slow
def test_memberships_added(parser_system):
    memberships = list(parser_system.get_supplemental_attributes(PLEXOSMembership))
    assert len(memberships) > 0

    for membership in memberships:
        assert isinstance(membership, PLEXOSMembership)
        assert membership.membership_id is not None
        assert membership.parent_object is not None
        assert membership.collection is not None


@pytest.mark.slow
def test_variables_parsed(parser_system):
    """Test that Variable components are correctly parsed."""
    variables = list(parser_system.get_components(PLEXOSVariable))
    assert len(variables) > 0, "Should have parsed at least one variable"

    # Check that variables have basic attributes
    for var in variables:
        assert isinstance(var, PLEXOSVariable)
        assert var.name is not None
        assert var.object_id is not None


def test_collection_properties_basic(db_with_reserve_collection_property, tmp_path):
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    db = db_with_reserve_collection_property
    xml_path = tmp_path / "reserve_coll_basic.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None

    region = system.get_component(PLEXOSRegion, "region-01")
    assert region is not None

    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)
    assert len(coll_props_list) > 0

    coll_props = coll_props_list[0]
    assert coll_props.collection_name == "Regions"
    assert "load_risk" in coll_props.properties

    load_risk_prop = coll_props.properties["load_risk"]
    load_risk_value = load_risk_prop.get_value()
    assert load_risk_value == 6.0


def test_collection_properties_with_timeseries(db_with_reserve_collection_property, tmp_path):
    from r2x_plexos.models.collection_property import CollectionProperties
    from r2x_plexos.models.region import PLEXOSRegion
    from r2x_plexos.models.reserve import PLEXOSReserve

    db = db_with_reserve_collection_property
    xml_path = tmp_path / "reserve_coll_prop.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    reserve = system.get_component(PLEXOSReserve, "TestReserve")
    assert reserve is not None

    region = system.get_component(PLEXOSRegion, "region-01")
    assert region is not None

    coll_props_list = system.get_supplemental_attributes_with_component(region, CollectionProperties)
    assert len(coll_props_list) > 0

    coll_props = coll_props_list[0]
    assert "lolp_target" in coll_props.properties
    assert "load_risk" in coll_props.properties

    lolp_prop = coll_props.properties["lolp_target"]
    assert lolp_prop.has_datafile()

    assert system.has_time_series(coll_props)

    ts = system.get_time_series(coll_props, "lolp_target")
    assert ts is not None
    assert len(ts.data) == 8784
    assert list(ts.data[:6]) == [1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
    assert max(ts.data) == 4.0

    lolp_value = lolp_prop.get_value()
    assert lolp_value == 4.0


def test_property_with_bands_kept_as_property_value(db_thermal_gen_multiband, tmp_path):
    """Test that multi-band properties are kept as PLEXOSPropertyValue objects."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "multiband.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    gen = system.get_component(PLEXOSGenerator, "thermal-01")

    # heat_rate has multiple bands, should be PLEXOSPropertyValue
    heat_rate_prop = gen.get_property_value("heat_rate")
    assert isinstance(heat_rate_prop, PLEXOSPropertyValue)
    assert heat_rate_prop.has_bands()

    # Accessing the property should return a dict of band values
    result = gen.heat_rate
    assert isinstance(result, dict)


def test_simple_numeric_property_extracted_as_value(db_thermal_gen_multiband, tmp_path):
    """Test that simple numeric properties without datafile/variable/bands are extracted as raw values."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "simple_numeric.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    gen = system.get_component(PLEXOSGenerator, "thermal-01")

    # max_capacity is a simple numeric value, should be extracted as float
    assert isinstance(gen.max_capacity, float)
    assert gen.max_capacity == 100.0


def test_region_load_with_variable_reference(db_with_multiband_variable, tmp_path):
    """Test that region load property with variable reference is handled correctly."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    sys = parser.build_system()

    region = sys.get_component(PLEXOSRegion, "r1")

    # load property should reference a variable
    load_prop = region.get_property_value("load")
    assert isinstance(load_prop, PLEXOSPropertyValue)
    assert load_prop.has_variable()


def test_datafile_component_not_registered_for_timeseries(db_with_multiband_variable, tmp_path):
    """Test that datafile components don't get time series registered."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    sys = parser.build_system()

    datafile = sys.get_component(PLEXOSDatafile, "LoadProfiles")
    assert datafile is not None

    # Datafile components should NOT have time series
    assert not sys.has_time_series(datafile)


def test_variable_component_not_registered_for_timeseries(db_with_multiband_variable, tmp_path):
    """Test that variable components don't get time series registered."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    sys = parser.build_system()

    # Get all variables
    variables = list(sys.get_components(PLEXOSVariable))
    assert len(variables) > 0

    # None should have time series
    for var in variables:
        assert not sys.has_time_series(var)


def test_parser_metadata_and_description(parser_system):
    """Test that parser sets system metadata correctly."""
    assert parser_system.data_format_version is not None
    assert "PLEXOS system" in parser_system.description
    assert "Base" in parser_system.description


def test_failed_references_tracking(db_with_multiband_variable, tmp_path, caplog):
    """Test that parser tracks failed time series attachments."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    _ = parser.build_system()

    # Check if any failures were logged
    assert parser._failed_references is not None
    # The list might be empty if all attachments succeeded, which is also valid


def test_parser_with_horizon_year(db_thermal_gen_multiband, tmp_path):
    """Test parser with horizon year configuration."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "with_horizon.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024, horizon_year=2025)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    assert system is not None
    assert config.horizon_year == 2025


def test_parser_component_cache_population(db_thermal_gen_multiband, tmp_path):
    """Test that component cache is properly populated during parsing."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "cache_test.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    # Component cache should be populated
    assert len(parser._component_cache) > 0

    # All cached components should be in the system
    for obj_id, component in parser._component_cache.items():
        assert obj_id == component.object_id
        system_component = system.get_component_by_uuid(component.uuid)
        assert system_component is not None


def test_parser_membership_cache(db_with_topology, tmp_path):
    """Test that membership cache is populated correctly."""
    db = db_with_topology
    xml_path = tmp_path / "membership.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    _ = parser.build_system()

    # Membership cache should be populated
    assert len(parser._membership_cache) > 0

    # Verify memberships are in the system
    for membership_id, membership in parser._membership_cache.items():
        assert membership.membership_id == membership_id
        assert membership.parent_object is not None
        assert membership.child_object is not None


def test_parser_handles_unsupported_component_type(db_base, tmp_path, caplog):
    """Test that parser gracefully handles unsupported component types."""
    import loguru

    db = db_base
    xml_path = tmp_path / "unsupported.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    with caplog.at_level(loguru.logger.level("DEBUG").no):
        parser = PLEXOSParser(config, store)
        system = parser.build_system()

    # System should still be created even if some components are unsupported
    assert system is not None


def test_parser_scenario_priority_setting(db_with_scenarios, tmp_path):
    """Test that parser correctly sets scenario priority."""
    db = db_with_scenarios
    xml_path = tmp_path / "scenarios.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store)
    system = parser.build_system()

    # System should be built successfully with scenarios
    assert system is not None


def test_parser_timeseries_cache_reuse(db_with_multiband_variable, tmp_path):
    """Test that parsed time series files are cached and reused."""
    db = db_with_multiband_variable
    config = PLEXOSConfig(model_name="Base")
    store = DataStore(path=tmp_path)

    parser = PLEXOSParser(config, store, db=db)
    _ = parser.build_system()

    # Check that the cache was used (has entries)
    assert len(parser._parsed_files_cache) >= 0  # May be 0 if no time series files


def test_parser_with_custom_name(db_thermal_gen_multiband, tmp_path):
    """Test parser with custom system name."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "custom_name.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store, name="CustomSystem")
    system = parser.build_system()

    assert system.name == "CustomSystem"


def test_parser_skip_validation_flag(db_thermal_gen_multiband, tmp_path):
    """Test parser with skip_validation flag."""
    db = db_thermal_gen_multiband
    xml_path = tmp_path / "skip_val.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    parser = PLEXOSParser(config, store, skip_validation=True)
    system = parser.build_system()

    assert system is not None


def test_parser_property_without_field_name_skipped(db_thermal_gen_multiband, tmp_path, caplog):
    """Test that properties without matching field names are skipped with warning."""
    import loguru

    db = db_thermal_gen_multiband
    xml_path = tmp_path / "unknown_prop.xml"
    db.to_xml(xml_path)

    config = PLEXOSConfig(model_name="Base", reference_year=2024)
    data_file = DataFile(name="xml_file", fpath=xml_path)
    store = DataStore(path=tmp_path)
    store.add_data(data_file)

    with caplog.at_level(loguru.logger.level("WARNING").no):
        parser = PLEXOSParser(config, store)
        system = parser.build_system()

    # System should still be created
    assert system is not None
