"""Tests for time series parsing functionality."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from plexosdb import ClassEnum, CollectionEnum

from r2x_core import DataFile, DataStore, PluginContext
from r2x_plexos import PLEXOSParser, PLEXOSPropertyValue
from r2x_plexos.models import CollectionProperties, PLEXOSGenerator, PLEXOSRegion, get_scenario_priority
from r2x_plexos.parser import TimeSeriesReference, TimeSeriesSourceType
from r2x_plexos.plugin_config import PLEXOSConfig


@pytest.fixture(scope="module")
def parser_basic(data_folder) -> PLEXOSParser:
    """Create a basic parser instance for testing (read-only tests)."""
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, horizon_year=2023)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(path=data_folder)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    return parser


@pytest.fixture
def parser_basic_mutable(data_folder) -> PLEXOSParser:
    """Create a basic parser instance for tests that modify the parser state."""
    config = PLEXOSConfig(model_name="Base", timeseries_dir=None, horizon_year=2023)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(path=data_folder)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    return parser


@pytest.fixture(scope="module")
def parser_with_timeseries_dir(data_folder) -> PLEXOSParser:
    timeseries_path = data_folder / "timeseries"
    timeseries_path.mkdir(exist_ok=True)

    config = PLEXOSConfig(model_name="Base", timeseries_dir=timeseries_path, horizon_year=2023)
    data_file = DataFile(name="xml_file", glob="*.xml")
    store = DataStore(path=data_folder)
    store.add_data([data_file])

    ctx = PluginContext(config=config, store=store)
    parser = cast(PLEXOSParser, PLEXOSParser.from_context(ctx))
    return parser


@pytest.mark.parametrize("collection", [False, True])
@pytest.mark.parametrize(
    ("entries", "expected"),
    [
        ([("Excluded", "excluded.csv")], None),
        ([(None, "base.csv"), ("Excluded", "excluded.csv")], "base.csv"),
        ([("Low", "low.csv"), ("High", "high.csv"), ("Excluded", "excluded.csv")], "high.csv"),
        ([("High", "high.csv"), ("Low", "low.csv")], "high.csv"),
        ([("Low", "LowFile"), ("High", "HighFile")], "HighFile"),
    ],
)
def test_register_reference_uses_selected_scenario_priority(db_base, tmp_path, collection, entries, expected):
    for name, order, selected in [("Low", 1, True), ("High", 5, True), ("Excluded", 99, False)]:
        db_base.add_object(ClassEnum.Scenario, name)
        db_base.add_attribute(ClassEnum.Scenario, name, attribute_name="Read Order", attribute_value=order)
        if selected:
            db_base.add_membership(
                ClassEnum.Model, ClassEnum.Scenario, "Base", name, CollectionEnum.Scenarios
            )

    parser = cast(
        PLEXOSParser,
        PLEXOSParser.from_context(
            PluginContext(config=PLEXOSConfig(model_name="Base"), store=DataStore(path=tmp_path))
        ),
    )
    parser.db = db_base
    parser.model_name = "Base"
    assert parser.validate_inputs().is_ok()
    prop = PLEXOSPropertyValue()
    for scenario, filename in entries:
        prop.add_entry(value=0.0, scenario=scenario, datafile_name=filename)
    component = PLEXOSGenerator(name="test")
    if collection:
        parser._register_collection_property_time_series_reference(component, "rating", prop, 42)
    else:
        parser._register_time_series_reference(component, "rating", prop)

    if expected is None:
        assert not parser.time_series_references
    else:
        assert len(parser.time_series_references) == 1
        ref = parser.time_series_references[0]
        assert (ref.datafile_path or ref.datafile_component_name) == expected
        assert ref.is_collection_property == collection
        assert ref.membership_id == (42 if collection else None)


def test_scenario_read_order_ignores_other_attributes(db_base, tmp_path):
    db_base.add_object(ClassEnum.Scenario, "Low")
    db_base.add_attribute(ClassEnum.Scenario, "Low", attribute_name="Read Order", attribute_value=0)
    db_base.add_attribute(ClassEnum.Scenario, "Low", attribute_name="Locked", attribute_value=1)
    db_base.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "Low", CollectionEnum.Scenarios)
    db_base.add_object(ClassEnum.Scenario, "Default")
    db_base.add_attribute(ClassEnum.Scenario, "Default", attribute_name="Locked", attribute_value=1)
    db_base.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", "Default", CollectionEnum.Scenarios)
    parser = cast(
        PLEXOSParser,
        PLEXOSParser.from_context(
            PluginContext(config=PLEXOSConfig(model_name="Base"), store=DataStore(path=tmp_path))
        ),
    )
    parser.db = db_base
    parser.model_name = "Base"
    assert parser.validate_inputs().is_ok()
    assert get_scenario_priority() == {"Low": 0, "Default": 0}


@pytest.mark.parametrize("collection", [False, True])
def test_register_active_variable_when_datafile_scenario_is_unselected(collection):
    parser = PLEXOSParser()
    parser._scenario_priority = {"Selected": 2}
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, scenario="Excluded", datafile_name="missing.csv")
    prop.add_entry(value=0.0, scenario="Selected", variable_name="active-variable")
    component = PLEXOSGenerator(name="test")
    if collection:
        parser._register_collection_property_time_series_reference(component, "rating", prop, 42)
    else:
        parser._register_time_series_reference(component, "rating", prop)

    ref = parser.time_series_references[0]
    assert ref.source_type == TimeSeriesSourceType.VARIABLE
    assert ref.variable_name == "active-variable"


@pytest.mark.parametrize("collection", [False, True])
def test_register_without_model_scenarios_only_uses_unscoped_entry(collection):
    parser = PLEXOSParser()
    prop = PLEXOSPropertyValue()
    prop.add_entry(value=0.0, scenario="Excluded", datafile_name="missing.csv")
    prop.add_entry(value=0.0, text="base.CSV")
    component = PLEXOSGenerator(name="test")
    if collection:
        parser._register_collection_property_time_series_reference(component, "rating", prop, 42)
    else:
        parser._register_time_series_reference(component, "rating", prop)

    assert parser.time_series_references[0].datafile_path == "base.CSV"


def test_parser_attaches_only_selected_scenario_profiles(db_with_topology, tmp_path):
    db = db_with_topology
    for name, order, selected in [("Low", 1, True), ("High", 5, True), ("Excluded", 99, False)]:
        db.add_object(ClassEnum.Scenario, name)
        db.add_attribute(ClassEnum.Scenario, name, attribute_name="Read Order", attribute_value=order)
        if selected:
            db.add_membership(ClassEnum.Model, ClassEnum.Scenario, "Base", name, CollectionEnum.Scenarios)

    db.add_object(ClassEnum.Reserve, "Reserve")
    db.add_membership(ClassEnum.Reserve, ClassEnum.Region, "Reserve", "region-01", CollectionEnum.Regions)
    for scenario, offset in [("Low", 10), ("High", 100), ("Excluded", 1000)]:
        path = tmp_path / f"{scenario}.csv"
        path.write_text(
            "Datetime,thermal-01,region-01\n"
            + "".join(
                f"{(datetime(2024, 1, 1) + timedelta(hours=hour)).isoformat()},"
                f"{offset + hour % 24},{offset + hour % 24}\n"
                for hour in range(8784)
            )
        )
        db.add_property(
            ClassEnum.Generator, "thermal-01", "Rating", 0.0, scenario=scenario, datafile_text=str(path)
        )
        db.add_property(
            ClassEnum.Region,
            "region-01",
            "LOLP Target",
            0.0,
            scenario=scenario,
            datafile_text=str(path),
            collection_enum=CollectionEnum.Regions,
            parent_class_enum=ClassEnum.Reserve,
            parent_object_name="Reserve",
        )
    db.add_property(
        ClassEnum.Generator,
        "thermal-01",
        "Commit",
        0.0,
        scenario="Excluded",
        datafile_text=str(tmp_path / "missing.csv"),
    )
    parser = cast(
        PLEXOSParser,
        PLEXOSParser.from_context(
            PluginContext(
                config=PLEXOSConfig(model_name="Base", horizon_year=2024), store=DataStore(tmp_path)
            )
        ),
    )
    parser.db = db

    system = parser.run().system
    assert system is not None
    generator = system.get_component(PLEXOSGenerator, "thermal-01")
    region = system.get_component(PLEXOSRegion, "region-01")
    collection = system.get_supplemental_attributes_with_component(region, CollectionProperties)[0]
    assert not parser._failed_references
    assert {ref.field_name for ref in parser.time_series_references} == {"rating", "lolp_target"}
    assert list(system.get_time_series(generator, "max_active_power").data[:3]) == [100, 101, 102]
    assert list(system.get_time_series(collection, "lolp_target").data[:3]) == [100, 101, 102]
    assert not any(ref.field_name == "commit" for ref in parser.time_series_references)


@pytest.mark.slow
def test_resolve_datafile_path_basic(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test basic datafile path resolution."""
    result = parser_basic._resolve_datafile_path("test_file.csv")
    expected = data_folder / "test_file.csv"
    assert result == expected


@pytest.mark.slow
def test_resolve_datafile_path_with_timeseries_dir(
    parser_with_timeseries_dir: PLEXOSParser, data_folder: Path
) -> None:
    """Test datafile path resolution with timeseries_dir."""
    result = parser_with_timeseries_dir._resolve_datafile_path("test_file.csv")
    expected = data_folder / "timeseries" / "test_file.csv"
    assert result == expected


@pytest.mark.slow
def test_resolve_datafile_path_windows_style(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test Windows-style path normalization."""
    result = parser_basic._resolve_datafile_path("TimeSeries\\Annual\\test_file.csv")
    expected = data_folder / "TimeSeries" / "Annual" / "test_file.csv"
    assert result == expected


@pytest.mark.slow
def test_resolve_datafile_path_subdirectory(parser_basic: PLEXOSParser, data_folder: Path) -> None:
    """Test path resolution with subdirectories."""
    result = parser_basic._resolve_datafile_path("data/annual/test.csv")
    expected = data_folder / "data" / "annual" / "test.csv"
    assert result == expected


@pytest.mark.slow
def test_resolve_datafile_path_none(parser_basic: PLEXOSParser) -> None:
    """Test that None path raises ValueError."""
    with pytest.raises(ValueError, match="No datafile path provided"):
        parser_basic._resolve_datafile_path(None)


@pytest.mark.slow
def test_resolve_datafile_path_empty(parser_basic: PLEXOSParser) -> None:
    """Test that empty path raises ValueError."""
    with pytest.raises(ValueError, match="No datafile path provided"):
        parser_basic._resolve_datafile_path("")


@pytest.mark.slow
def test_get_or_parse_timeseries_value_file(parser_basic: PLEXOSParser, tmp_path: Path) -> None:
    """Test parsing a simple value file."""
    csv_path = tmp_path / "test_value.csv"
    csv_path.write_text("Name,Value\nGenerator1,150.0\nGenerator2,250.0")

    result = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Generator1", reference_year=2023, timeslices=None
    )

    # ValueFile returns float constants, not time series
    assert result is not None
    assert isinstance(result, float)
    assert result == 150.0


@pytest.mark.slow
def test_get_or_parse_timeseries_caching(parser_basic: PLEXOSParser, tmp_path: Path) -> None:
    """Test that parsed files are cached."""
    csv_path = tmp_path / "test_cache.csv"
    csv_path.write_text("Name,Value\nGen1,100.0\nGen2,200.0")

    _ = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Gen1", reference_year=2023
    )
    assert str(csv_path) in parser_basic._parsed_files_cache

    result2 = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="Gen2", reference_year=2023
    )
    # ValueFile returns float constants
    assert result2 is not None
    assert isinstance(result2, float)
    assert result2 == 200.0


@pytest.mark.slow
def test_get_or_parse_timeseries_component_not_found(parser_basic: PLEXOSParser, tmp_path: Path) -> None:
    """Test fallback to single time series when component not found but only one exists."""
    csv_path = tmp_path / "test_missing.csv"
    csv_path.write_text("Name,Value\nGenerator1,150.0")

    result = parser_basic._get_or_parse_timeseries(
        file_path=str(csv_path), component_name="NonExistent", reference_year=2023
    )
    # ValueFile returns float constant
    assert result is not None
    assert isinstance(result, float)
    assert result == 150.0


@pytest.mark.slow
def test_attach_direct_datafile_timeseries_component_not_found_in_system(
    parser_basic: PLEXOSParser, tmp_path: Path
) -> None:
    """Test error when component not in system."""
    from unittest.mock import patch

    from r2x_core import System

    # Initialize system in context
    system = System(name="test_system")
    parser_basic.ctx.system = system

    parser_basic.config.timeseries_dir = tmp_path

    csv_path = tmp_path / "test.csv"
    csv_path.write_text("Name,Value\nGen1,100.0")

    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="MissingGen",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="test.csv",
    )

    with (
        patch.object(parser_basic.system, "get_component_by_uuid", return_value=None),
        pytest.raises(ValueError, match=r"Component MissingGen.*"),
    ):
        parser_basic._attach_direct_datafile_timeseries(
            ref=ref, reference_year=2023, timeslices=None, horizon=None
        )


@pytest.mark.slow
def test_attach_direct_datafile_timeseries_already_attached(
    parser_basic_mutable: PLEXOSParser, data_folder: Path
) -> None:
    """Test that already attached time series are skipped."""
    from unittest.mock import patch

    from r2x_core import System

    # Initialize system in context
    system = System(name="test_system")
    parser_basic_mutable.ctx.system = system

    test_uuid = UUID("12345678-1234-5678-1234-567812345678")

    parser_basic_mutable._attached_timeseries[(test_uuid, "capacity")] = True

    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="Gen1",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="test.csv",
    )

    with patch.object(parser_basic_mutable.system, "get_component_by_uuid") as mock_get:
        parser_basic_mutable._attach_direct_datafile_timeseries(
            ref=ref, reference_year=2023, timeslices=None, horizon=None
        )

        mock_get.assert_not_called()


@pytest.mark.slow
def test_attach_direct_datafile_timeseries_file_not_found(
    parser_basic_mutable: PLEXOSParser, data_folder: Path
) -> None:
    """Test handling of missing datafile."""
    from unittest.mock import patch

    from r2x_core import System

    # Initialize system in context
    system = System(name="test_system")
    parser_basic_mutable.ctx.system = system

    mock_component = MagicMock()
    test_uuid = UUID("12345678-1234-5678-1234-567812345678")
    mock_component.uuid = test_uuid

    ref = TimeSeriesReference(
        component_uuid=test_uuid,
        component_name="Gen1",
        field_name="capacity",
        source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
        datafile_path="nonexistent.csv",
    )

    with (
        patch.object(parser_basic_mutable.system, "get_component_by_uuid", return_value=mock_component),
        pytest.raises(FileNotFoundError, match="not found"),
    ):
        parser_basic_mutable._attach_direct_datafile_timeseries(
            ref=ref, reference_year=2023, timeslices=None, horizon=None
        )


@pytest.mark.slow
def test_build_time_series_integration(parser_basic_mutable: PLEXOSParser, tmp_path: Path) -> None:
    """Test the full build_time_series workflow."""
    from unittest.mock import patch

    from r2x_core import System

    # Initialize system in context
    system = System(name="test_system")
    parser_basic_mutable.ctx.system = system

    parser_basic_mutable.config.timeseries_dir = tmp_path

    gen_csv = tmp_path / "generators.csv"
    gen_csv.write_text("Name,Value\nGen1,500.0\nGen2,750.0\nGen3,1000.0")

    load_csv = tmp_path / "loads.csv"
    load_csv.write_text("Name,Value\nLoad1,200.0\nLoad2,300.0")

    gen1 = MagicMock()
    gen1_uuid = UUID("11111111-1111-1111-1111-111111111111")
    gen1.uuid = gen1_uuid
    gen1.name = "Gen1"

    gen2 = MagicMock()
    gen2_uuid = UUID("22222222-2222-2222-2222-222222222222")
    gen2.uuid = gen2_uuid
    gen2.name = "Gen2"

    load1 = MagicMock()
    load1_uuid = UUID("33333333-3333-3333-3333-333333333333")
    load1.uuid = load1_uuid
    load1.name = "Load1"

    def get_component_by_uuid_side_effect(uuid):
        if uuid == gen1_uuid:
            return gen1
        elif uuid == gen2_uuid:
            return gen2
        elif uuid == load1_uuid:
            return load1
        return None

    parser_basic_mutable.time_series_references = [
        TimeSeriesReference(
            component_uuid=gen1_uuid,
            component_name="Gen1",
            field_name="max_capacity",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=gen2_uuid,
            component_name="Gen2",
            field_name="max_capacity",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=load1_uuid,
            component_name="Load1",
            field_name="load",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="loads.csv",
            units="MW",
        ),
        TimeSeriesReference(
            component_uuid=UUID("44444444-4444-4444-4444-444444444444"),
            component_name="MissingComponent",
            field_name="value",
            source_type=TimeSeriesSourceType.DIRECT_DATAFILE,
            datafile_path="generators.csv",
        ),
    ]

    with (
        patch.object(
            parser_basic_mutable.system,
            "get_component_by_uuid",
            side_effect=get_component_by_uuid_side_effect,
        ),
        patch.object(parser_basic_mutable.system, "get_components", return_value=[]),
    ):
        parser_basic_mutable.build_time_series()

        assert len(parser_basic_mutable._attached_timeseries) == 3  # 3 successful attachments
        assert (gen1_uuid, "max_capacity") in parser_basic_mutable._attached_timeseries
        assert (gen2_uuid, "max_capacity") in parser_basic_mutable._attached_timeseries
        assert (load1_uuid, "load") in parser_basic_mutable._attached_timeseries

        assert len(parser_basic_mutable._failed_references) == 1
        failed_ref, error_msg = parser_basic_mutable._failed_references[0]
        assert failed_ref.component_name == "MissingComponent"
        assert "not found" in error_msg

        assert str(gen_csv) in parser_basic_mutable._parsed_files_cache
