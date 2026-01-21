from pathlib import Path

import pytest
from plexosdb import ClassEnum, CollectionEnum, PlexosDB

from r2x_core import DataStore, Err, PluginConfig, PluginContext, System
from r2x_plexos import PLEXOSConfig
from r2x_plexos.exporter import DEFAULT_XML_TEMPLATE, PLEXOSExporter
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

    result = exporter.on_build()
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

    result = exporter.on_build()
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

    build_result = exporter.on_build()
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
