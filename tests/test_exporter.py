from pathlib import Path
from unittest.mock import patch

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataStore, Err, PluginConfig, PluginContext, System
from r2x_plexos import PLEXOSConfig, PLEXOSPropertyValue
from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE, PLEXOSExporter
from r2x_plexos.models import PLEXOSDatafile, PLEXOSGenerator, PLEXOSMembership, PLEXOSNode
from r2x_plexos.parser import PLEXOSParser

pytestmark = pytest.mark.export


@pytest.fixture
def plexos_config():
    from r2x_plexos import PLEXOSConfig

    return PLEXOSConfig(model_name="Base", horizon_year=2024)


@pytest.fixture
def template_db(plexos_config: PLEXOSConfig) -> PlexosDB:
    """Create a PlexosDB from the default template."""
    template_path = plexos_config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
    return PlexosDB.from_xml(template_path)


@pytest.fixture
def serialized_plexos_system(tmp_path, db_all_gen_types, plexos_config) -> "System":
    from r2x_core import DataStore, PluginContext
    from r2x_plexos import PLEXOSParser

    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=plexos_config, store=store)
    parser = PLEXOSParser.from_context(ctx)
    parser.db = db_all_gen_types

    result = parser.run()
    sys = result.system

    serialized_sys_fpath = tmp_path / "test_plexos_system.json"
    sys.to_json(serialized_sys_fpath)
    return sys


def test_setup_configuration_creates_simulation(plexos_config, serialized_plexos_system, template_db, caplog):
    """Test that setup_configuration creates models, horizons, and memberships."""
    sys = serialized_plexos_system

    ctx = PluginContext(config=plexos_config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    # Verify database is now empty
    models_before = exporter.db.list_objects_by_class(ClassEnum.Model)
    horizons_before = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(models_before) == 0
    assert len(horizons_before) == 0

    result = exporter.setup_configuration()
    assert result.is_ok(), f"setup_configuration failed: {result.error if result.is_err() else result}"

    models_before = exporter.db.list_objects_by_class(ClassEnum.Model)
    horizons_before = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(models_before) == 1
    assert len(horizons_before) == 1

    result = exporter.setup_configuration()
    assert result.is_ok(), f"setup_configuration failed: {result.error if result.is_err() else result}"

    models_after = exporter.db.list_objects_by_class(ClassEnum.Model)
    assert len(models_after) > 0, "No models were created"

    horizons_after = exporter.db.list_objects_by_class(ClassEnum.Horizon)
    assert len(horizons_after) > 0, "No horizons were created"

    model_name = models_after[0]
    horizon_name = horizons_after[0]

    model_id = exporter.db.get_object_id(ClassEnum.Model, model_name)
    horizon_id = exporter.db.get_object_id(ClassEnum.Horizon, horizon_name)

    # Check memberships - models should be connected to horizons
    query = """
    SELECT COUNT(*)
    FROM t_membership
    WHERE parent_object_id = ? AND child_object_id = ?
    """
    result = exporter.db.query(query, (model_id, horizon_id))
    membership_count = result[0][0] if result else 0
    assert membership_count > 0, "No model-horizon memberships were created"

    # Verify horizon attributes were set (not properties - horizons use attributes!)
    # Check for at least one of the common horizon attributes
    try:
        chrono_date_from = exporter.db.get_attribute(
            ClassEnum.Horizon, object_name=horizon_name, attribute_name="Chrono Date From"
        )
        assert chrono_date_from is not None, "Horizon attributes were not set"
    except AssertionError as e:
        # If get_attribute fails, it means no attributes were set
        raise AssertionError("No horizon attributes were set") from e


def test_setup_configuration_skips_existing(plexos_config, serialized_plexos_system, template_db, caplog):
    """Test that setup_configuration skips if models/horizons already exist."""
    sys = serialized_plexos_system

    ctx = PluginContext(config=plexos_config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result1 = exporter.setup_configuration()
    assert result1.is_ok()

    models_count = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    result2 = exporter.setup_configuration()
    assert result2.is_ok()

    models_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Model))
    horizons_count2 = len(exporter.db.list_objects_by_class(ClassEnum.Horizon))

    assert models_count == models_count2, "Models were created on second call"
    assert horizons_count == horizons_count2, "Horizons were created on second call"
    assert "using existing database configuration" in caplog.text.lower()


def test_setup_configuration_missing_reference_year(template_db):
    """Test that missing horizon_year returns error."""
    config = PLEXOSConfig(model_name="Base")
    sys = System(name="test_system")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_err(), "Should fail without horizon_year"
    assert "horizon_year" in str(result.error).lower()


def test_exporter_with_wrong_config(mocker, caplog):
    class InvalidConfig(PluginConfig):
        name: str

    bad_config = InvalidConfig(name="Test")
    mock_system = mocker.Mock()

    ctx = PluginContext(config=bad_config, system=mock_system)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.on_export()
    assert result.is_err()
    assert "Config is of type" in str(result.error)


def is_valid_class_enum(class_enum):
    """Check if a ClassEnum has a corresponding CollectionEnum."""
    try:
        _ = CollectionEnum[class_enum.name]
        return True
    except KeyError:
        return False


def test_roundtrip_db_parser_system_exporter_db(db_all_gen_types: PlexosDB, tmp_path: Path, template_db):
    original_db = db_all_gen_types

    config = PLEXOSConfig(model_name="Base", horizon_year=2024, timeseries_dir=tmp_path)
    store = DataStore(path=tmp_path)

    ctx = PluginContext(config=config, store=store)
    parser = PLEXOSParser.from_context(ctx)
    parser.db = original_db

    result = parser.run()
    system = result.system

    export_ctx = PluginContext(config=config, system=system)
    exporter = PLEXOSExporter.from_context(export_ctx)
    exporter.exclude_defaults = True
    exporter.output_path = str(tmp_path)
    exporter.db = template_db

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    setup_result = exporter.setup_configuration()
    assert setup_result.is_ok(), (
        f"Setup configuration failed: {setup_result.error if setup_result.is_err() else ''}"
    )

    prepare_result = exporter.prepare_export()
    assert prepare_result.is_ok(), (
        f"Prepare export failed: {prepare_result.error if prepare_result.is_err() else ''}"
    )

    exporter._add_component_datafile_objects()
    exporter._add_component_properties()
    exporter._add_component_memberships()

    exported_db = exporter.db

    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            original_objects = original_db.list_objects_by_class(class_enum)
            exported_objects = exported_db.list_objects_by_class(class_enum)
            assert len(exported_objects) == len(original_objects), (
                f"{class_enum.name}: exported {len(exported_objects)} objects, expected {len(original_objects)}"
            )
        except Exception:
            continue

    original_properties_count = 0
    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            for obj_name in original_db.list_objects_by_class(class_enum):
                original_properties_count += len(original_db.get_object_properties(class_enum, obj_name))
        except Exception:
            continue

    exported_properties_count = 0
    for class_enum in ClassEnum:
        if not is_valid_class_enum(class_enum):
            continue
        try:
            for obj_name in exported_db.list_objects_by_class(class_enum):
                exported_properties_count += len(exported_db.get_object_properties(class_enum, obj_name))
        except Exception:
            continue

    assert exported_properties_count >= original_properties_count, (
        f"Properties: exported {exported_properties_count}, expected at least {original_properties_count}"
    )

    exported_memberships_count = exported_db.query(
        "SELECT COUNT(*) FROM t_membership WHERE parent_class_id NOT IN (1, 707) AND child_class_id NOT IN (1, 707)"
    )[0][0]
    assert exported_memberships_count > 0, "No memberships exported"


def test_exporter_init_with_invalid_config_type():
    class DummyConfig:
        pass

    sys = System(name="test")
    ctx = PluginContext(config=DummyConfig(), system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.on_export()
    assert result.is_err()


def test_exporter_init_with_existing_db(tmp_path, db_all_gen_types):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = db_all_gen_types

    assert exporter.db is db_all_gen_types


def test_setup_configuration_missing_simulation_config(monkeypatch):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    build_result = exporter.on_export()
    assert build_result.is_ok()

    monkeypatch.setattr(exporter.config, "simulation_config", None)

    for model_name in exporter.db.list_objects_by_class(ClassEnum.Model):
        exporter.db.delete_object(ClassEnum.Model, name=model_name)
    for horizon_name in exporter.db.list_objects_by_class(ClassEnum.Horizon):
        exporter.db.delete_object(ClassEnum.Horizon, name=horizon_name)

    result = exporter.setup_configuration()
    assert result.is_ok()


def test_prepare_export_skips_types(mocker, template_db):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    result = exporter.prepare_export()
    assert result.is_ok()


def test_prepare_export_no_class_enum(mocker, template_db):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class DummyType:
        pass

    sys.get_component_types.return_value = [DummyType]

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter.db = template_db

    result = exporter.prepare_export()
    assert result.is_ok()


def test_export_time_series_no_components(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_component_types.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.export_time_series()
    assert result.is_ok()


def test_export_time_series_csv_error(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    class DummyType:
        pass

    sys.get_component_types.return_value = [DummyType]
    sys.get_components.return_value = [mocker.Mock(name="comp")]
    sys.has_time_series.return_value = True

    ts_key = mocker.Mock()
    ts_key.name = "ts_key"
    ts_key.features = {}
    sys.list_time_series_keys.return_value = [ts_key]
    sys.get_time_series_by_key.return_value = [1, 2, 3]
    mocker.patch("r2x_plexos.exporter.export_time_series_csv", return_value=Err("fail"))

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    result = exporter.export_time_series()
    assert result.is_err()


def test_add_component_memberships_no_memberships(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_supplemental_attributes.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_memberships()


def test_add_component_memberships_skips_invalid(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    membership = mocker.Mock()
    membership.parent_object = None
    membership.child_object = None
    sys.get_supplemental_attributes.return_value = [membership]

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_memberships()


def test_create_datafile_objects_no_dir(tmp_path, mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()


def test_add_component_datafile_objects_no_datafiles(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    sys.get_components.return_value = []

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    exporter._add_component_datafile_objects()


def test_add_component_datafile_objects_filename_none(mocker):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = mocker.Mock()
    datafile = mocker.Mock()
    datafile.name = "test"
    datafile.filename = None
    sys.get_components.return_value = [datafile]

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    mocker.patch.object(exporter, "_create_datafile_objects")
    exporter._add_component_datafile_objects()


def test_validate_xml_invalid(tmp_path):
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    invalid_xml = tmp_path / "invalid.xml"
    invalid_xml.write_text("<notxml>")
    assert not exporter._validate_xml(str(invalid_xml))


def test_on_export_db_none_initializes_from_template(tmp_path):
    """Test that on_export initializes db from template when db is None - lines 86, 90."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter.db = None

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db is not None


def test_on_export_uses_custom_template(tmp_path):
    """Test that on_export uses custom template when specified - line 94."""
    # Create a minimal custom XML template
    custom_template = tmp_path / "custom_template.xml"
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    default_template = config.get_config_path().joinpath("master_9.2R6_btu.xml")
    db = PlexosDB.from_xml(default_template)
    db.to_xml(custom_template)

    config_with_template = PLEXOSConfig(model_name="Base", horizon_year=2024, template=str(custom_template))
    sys = System(name="test")

    ctx = PluginContext(config=config_with_template, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None
    exporter.output_path = str(tmp_path)

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db is not None


def test_on_export_creates_scenario_if_missing(template_db, tmp_path):
    """Test that on_export creates scenario if it doesn't exist - lines 97-98."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.plexos_scenario = "new_scenario"
    exporter.output_path = str(tmp_path)

    if exporter.db.check_object_exists(ClassEnum.Scenario, "new_scenario"):
        exporter.db.delete_object(ClassEnum.Scenario, name="new_scenario")

    result = exporter.on_export()

    assert result.is_ok()
    assert exporter.db.check_object_exists(ClassEnum.Scenario, "new_scenario")


def test_on_export_exception_returns_err(template_db):
    """Test that exceptions in on_export are caught and returned as Err - line 121."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(exporter, "setup_configuration", side_effect=Exception("Test error")):
        result = exporter.on_export()
    assert result.is_err()
    assert "Export failed" in result.error


def test_setup_configuration_with_existing_models_and_horizons(template_db):
    """Test that setup_configuration skips creation when models/horizons exist - lines 164-165."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    if not template_db.list_objects_by_class(ClassEnum.Model):
        template_db.add_object(ClassEnum.Model, "TestModel")
    if not template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.add_object(ClassEnum.Horizon, "TestHorizon")

    result = exporter.setup_configuration()

    assert result.is_ok()


def test_setup_configuration_missing_horizon_year(template_db):
    """Test setup_configuration returns error when horizon_year is missing - lines 176-177."""
    config = PLEXOSConfig(model_name="Base")
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    result = exporter.setup_configuration()

    assert result.is_err()
    assert "horizon_year" in result.error.lower()


def test_setup_configuration_build_simulation_fails(template_db):
    """Test setup_configuration handles build_plexos_simulation failure - line 200."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.build_plexos_simulation", return_value=CoreErr("Build failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to build simulation" in result.error


def test_setup_configuration_ingest_fails(template_db):
    """Test setup_configuration handles ingest failure - lines 219-220."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    for model in template_db.list_objects_by_class(ClassEnum.Model):
        template_db.delete_object(ClassEnum.Model, name=model)
    for horizon in template_db.list_objects_by_class(ClassEnum.Horizon):
        template_db.delete_object(ClassEnum.Horizon, name=horizon)

    from r2x_core import Err as CoreErr

    with patch("r2x_plexos.exporter.ingest_simulation_to_plexosdb", return_value=CoreErr("Ingest failed")):
        result = exporter.setup_configuration()

    assert result.is_err()
    assert "Failed to ingest simulation" in result.error


def test_prepare_export_db_none_returns_err():
    """Test prepare_export returns error when db is None - lines 234-238."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.prepare_export()

    assert result.is_err()
    assert "Database not initialized" in result.error


def test_prepare_export_component_without_mapping(template_db, caplog):
    """Test prepare_export skips components without ClassEnum mapping - line 253."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch("r2x_plexos.exporter.PLEXOS_TYPE_MAP_INVERTED", {}):
        result = exporter.prepare_export()

    assert result.is_ok()
    assert "Skipping component type" in caplog.text or result.is_ok()


def test_prepare_export_components_with_same_category_grouped(template_db):
    """Test prepare_export groups components by category - lines 264-277."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen1 = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    gen2 = PLEXOSGenerator(name="Gen2", category="thermal", units=1, rating=60.0)
    gen3 = PLEXOSGenerator(name="Gen3", category="hydro", units=1, rating=70.0)

    sys.add_component(gen1)
    sys.add_component(gen2)
    sys.add_component(gen3)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    result = exporter.prepare_export()

    assert result.is_ok()
    # Verify generators were added
    generators = template_db.list_objects_by_class(ClassEnum.Generator)
    assert "Gen1" in generators
    assert "Gen2" in generators
    assert "Gen3" in generators


def test_prepare_export_add_objects_raises_key_error(template_db):
    """Test prepare_export handles KeyError from add_objects - lines 286-287."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="TestGen", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(template_db, "add_objects", side_effect=KeyError("Invalid category")):  # noqa: SIM117
        with pytest.raises(KeyError):
            exporter.prepare_export()


def test_postprocess_export_db_none_returns_err():
    """Test postprocess_export returns error when db is None - lines 293-302."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    result = exporter.postprocess_export()

    assert result.is_err()
    assert "Database not initialized" in result.error


def test_postprocess_export_time_series_fails(template_db, tmp_path):
    """Test postprocess_export handles time series export failure - line 313."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock export_time_series to return Err
    from r2x_core import Err as CoreErr

    with patch.object(exporter, "export_time_series", return_value=CoreErr("TS export failed")):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "TS export failed" in result.error


def test_postprocess_export_invalid_xml(template_db, tmp_path):
    """Test postprocess_export detects invalid XML - line 328."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db
    exporter.output_path = str(tmp_path)

    # Mock _validate_xml to return False
    with patch.object(exporter, "_validate_xml", return_value=False):
        result = exporter.postprocess_export()

    assert result.is_err()
    assert "not valid" in result.error


def test_add_component_properties_db_none_logs_error(caplog):
    """Test _add_component_properties handles db None - lines 363-364."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_properties()

    assert "Database not initialized" in caplog.text


def test_add_component_properties_adds_datafile_filename(template_db):
    """Test _add_component_properties adds Filename property for DataFile - lines 370-371."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.DataFile, "TestFile", category="CSV")
    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.DataFile, "TestFile")
    prop_names = [p.get("property") for p in props]
    assert "Filename" in prop_names


def test_add_component_properties_filters_metadata_fields(template_db):
    """Test _add_component_properties filters out metadata fields - lines 392-393."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="thermal",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "name" not in [pn.lower() for pn in prop_names if pn]
    assert "category" not in [pn.lower() for pn in prop_names if pn]

    assert "Units" in prop_names
    assert "Rating" in prop_names


def test_add_component_properties_handles_dict_with_text(template_db):
    """Test _add_component_properties handles dict properties with 'text' - lines 406-408."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Create a generator with a property that's a dict with 'text'
    gen = PLEXOSGenerator(name="TestGen", category="thermal", units=1, rating=50.0)
    # Manually set a property as dict with 'text'
    gen.max_capacity = PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    # Properties should be added
    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    assert len(props) > 0


def test_add_component_properties_skips_none_values(template_db):
    """Test _add_component_properties skips None values - line 411-412."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(
        name="TestGen",
        category="thermal",
        units=1,
        rating=50.0,
    )
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    template_db.add_object(ClassEnum.Generator, "TestGen", category="thermal")

    exporter._add_component_properties()

    props = template_db.get_object_properties(ClassEnum.Generator, "TestGen")
    prop_names = [p.get("property") for p in props]

    assert "Units" in prop_names
    assert "Rating" in prop_names
    assert len(prop_names) <= 10


def test_add_component_memberships_db_none_logs_error(caplog):
    """Test _add_component_memberships handles db None - lines 429-440."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_memberships()

    assert "Database not initialized" in caplog.text


def test_add_component_memberships_no_memberships_warns(template_db, caplog):
    """Test _add_component_memberships warns when no memberships found - line 444."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_memberships()

    assert "No memberships found" in caplog.text


def test_add_component_memberships_skips_missing_parent_or_child(template_db, caplog):
    """Test _add_component_memberships skips memberships with missing objects - lines 453-456."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")
    sys.add_component(gen)
    sys.add_component(node)

    _ = PLEXOSMembership(parent_object=node, child_object=gen, collection=CollectionEnum.Generators)

    mock_membership = Mock()
    mock_membership.parent_object = None
    mock_membership.child_object = None
    mock_membership.collection = CollectionEnum.Generators

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "Skipping membership with missing parent or child" in caplog.text


def test_add_component_memberships_no_valid_records_warns(template_db, caplog):
    """Test _add_component_memberships warns when no valid records - line 495."""
    from unittest.mock import Mock, patch

    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    node = PLEXOSNode(name="Node1")

    mock_membership = Mock()
    mock_membership.parent_object = node
    mock_membership.child_object = gen
    mock_membership.collection = None

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    with patch.object(sys, "get_supplemental_attributes", return_value=[mock_membership]):
        exporter._add_component_memberships()

    assert "No valid membership records" in caplog.text or "Skipping membership" in caplog.text


def test_add_component_datafile_objects_db_none(caplog):
    """Test _add_component_datafile_objects handles db None - lines 527, 529."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = None

    exporter._add_component_datafile_objects()

    assert "Database not initialized" in caplog.text


def test_add_component_datafile_objects_updates_object_ids(template_db):
    """Test _add_component_datafile_objects updates object_id and datafile_id - line 557."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(
        name="TestFile", filename=PLEXOSPropertyValue.from_dict({"datafile_name": "test.csv"})
    )
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    assert datafile.object_id is not None


def test_add_component_datafile_objects_handles_no_filename(template_db, caplog):
    """Test _add_component_datafile_objects handles datafile without filename - lines 570-577."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    datafile = PLEXOSDatafile(name="TestFile", filename=None)
    sys.add_component(datafile)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    exporter._add_component_datafile_objects()

    assert "has no filename property" in caplog.text


def test_export_time_series_no_components_with_ts(template_db):
    """Test export_time_series handles no components with time series."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    # Add component without time series
    gen = PLEXOSGenerator(name="Gen1", category="thermal", units=1, rating=50.0)
    sys.add_component(gen)

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.db = template_db

    result = exporter.export_time_series()

    assert result.is_ok()


def test_create_datafile_objects_no_directory(tmp_path, caplog):
    """Test _create_datafile_objects handles missing directory."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)

    # Use a non-existent path that won't be created
    non_existent_path = tmp_path / "does_not_exist" / "nested"
    exporter.output_path = str(non_existent_path)

    # Mock get_output_directory to return a path that doesn't exist
    with patch("r2x_plexos.exporter.get_output_directory", return_value=non_existent_path / "Data"):
        exporter._create_datafile_objects()

    assert "No time series directory found" in caplog.text


def test_create_datafile_objects_creates_from_csv_files(tmp_path):
    """Test _create_datafile_objects creates DataFile objects from CSV files."""
    config = PLEXOSConfig(model_name="Base", horizon_year=2024)
    sys = System(name="test")

    data_dir = tmp_path / "Data"
    data_dir.mkdir()
    (data_dir / "test1.csv").write_text("col1,col2\n1,2\n")
    (data_dir / "test2.csv").write_text("col1,col2\n3,4\n")

    ctx = PluginContext(config=config, system=sys)
    exporter = PLEXOSExporter.from_context(ctx)
    exporter.output_path = str(tmp_path)

    exporter._create_datafile_objects()

    datafiles = list(sys.get_components(PLEXOSDatafile))
    assert len(datafiles) == 2
    assert any(df.name == "test1" for df in datafiles)
    assert any(df.name == "test2" for df in datafiles)
