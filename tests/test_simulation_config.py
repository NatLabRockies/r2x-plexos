"""Tests for simulation configuration functionality."""

from pathlib import Path

import plexosdb
import pytest
from plexosdb import ClassEnum, PlexosDB

from r2x_plexos.models.model import PLEXOSHorizon, PLEXOSModel
from r2x_plexos.models.simulation_config import (
    PLEXOSPASA,
    PLEXOSDiagnostic,
    PLEXOSLTPlan,
    PLEXOSMTSchedule,
    PLEXOSPerformance,
    PLEXOSProduction,
    PLEXOSReport,
    PLEXOSSTSchedule,
    PLEXOSTransmission,
)
from r2x_plexos.utils_plexosdb import validate_simulation_attribute
from r2x_plexos.utils_simulation import (
    SimulationConfig,
    build_plexos_simulation,
    convert_simulation_config_to_attributes,
    get_default_simulation_config,
    ingest_simulation_config_to_plexosdb,
    ingest_simulation_to_plexosdb,
    validate_simulation_config,
)

SCHEMA_PATH = Path(plexosdb.__file__).parent / "config" / "master_12.0R3_btu.xml"


@pytest.fixture
def plexos_db():
    """Create a PlexosDB instance with populated schema from test XML."""
    return PlexosDB.from_xml(Path("tests/data/5_bus_system_variables.xml"))


def test_validate_simulation_attribute_valid(plexos_db):
    """Test validation with valid attribute."""
    result = validate_simulation_attribute(plexos_db, ClassEnum.Performance, "SOLVER")
    assert result.is_ok()

    result = validate_simulation_attribute(plexos_db, ClassEnum.STSchedule, "Transmission Detail")
    assert result.is_ok()


def test_validate_simulation_attribute_invalid(plexos_db):
    """Test validation with invalid attribute."""
    result = validate_simulation_attribute(plexos_db, ClassEnum.Performance, "InvalidAttribute")
    assert result.is_err()
    assert result.error is not None
    assert "Invalid attribute" in result.error

    result = validate_simulation_attribute(plexos_db, ClassEnum.Production, "NotARealAttribute")
    assert result.is_err()


def test_get_default_simulation_config():
    """Test default template generation."""
    defaults = get_default_simulation_config()

    assert "mt_schedule" in defaults
    assert "st_schedule" in defaults
    assert "production" in defaults
    assert "pasa" in defaults
    assert "performance" in defaults
    assert "report" in defaults
    assert "transmission" in defaults
    assert "diagnostic" in defaults

    # Check that all defaults are proper instances
    assert isinstance(defaults["mt_schedule"], PLEXOSMTSchedule)
    assert isinstance(defaults["st_schedule"], PLEXOSSTSchedule)
    assert isinstance(defaults["production"], PLEXOSProduction)
    assert isinstance(defaults["pasa"], PLEXOSPASA)
    assert isinstance(defaults["performance"], PLEXOSPerformance)
    assert isinstance(defaults["report"], PLEXOSReport)
    assert isinstance(defaults["transmission"], PLEXOSTransmission)
    assert isinstance(defaults["diagnostic"], PLEXOSDiagnostic)

    # Check some default values
    assert defaults["performance"].solver == 4  # Gurobi
    assert defaults["st_schedule"].stochastic_method == 1
    assert defaults["st_schedule"].transmission_detail == 1


def test_plexos_lt_plan_v12_attributes():
    """Test LT Plan attributes and constraints from the v12 reference."""
    result = convert_simulation_config_to_attributes(PLEXOSLTPlan(name="LTPlan"))

    assert result.is_ok()
    attributes = result.unwrap()
    assert len(attributes) == 52
    assert attributes["Allow Capacity Sharing"] == 0
    assert attributes["Block Count"] == 12
    assert attributes["Chronology"] == 2
    assert attributes["Optimize Expansion"] == -1
    assert attributes["Write Expansion Plan Text Files"] == 0

    with pytest.raises(ValueError):
        PLEXOSLTPlan(name="InvalidChronology", chronology=1)
    with pytest.raises(ValueError):
        PLEXOSLTPlan(name="InvalidSolutionQuality", solution_quality=100.1)


def test_ingest_lt_plan_config_and_model_membership():
    """Test LT Plan config routing and its Model membership."""
    db = PlexosDB.from_xml(SCHEMA_PATH)
    simulation = SimulationConfig(
        models=[PLEXOSModel(name="LTPlanModel")],
        horizons=[PLEXOSHorizon(name="LTPlanHorizon")],
        memberships=[
            ("LTPlanModel", "LTPlanHorizon", "Horizon"),
            ("LTPlanModel", "LTPlan", "LT Plan"),
        ],
        simulation_configs={"lt_plan": PLEXOSLTPlan(name="LTPlan")},
    )

    result = ingest_simulation_to_plexosdb(db, simulation)

    assert result.is_ok()
    assert db.check_object_exists("LT Plan", "LTPlan")
    assert db.get_membership_id("LTPlanModel", "LTPlan", "LT Plan")
    assert db.get_attribute("LT Plan", object_name="LTPlan", attribute_name="Chronology")[0] == 2

    info = result.unwrap()["simulation_objects"]
    assert len(info) == 1
    assert info[0]["class"] == "LT Plan"


def test_convert_simulation_config_to_attributes():
    """Test Pydantic to dict conversion."""
    perf = PLEXOSPerformance(
        name="MyPerf",
        solver=4,
        mip_relative_gap=0.01,
        mip_maximum_threads=20,
    )

    result = convert_simulation_config_to_attributes(perf)
    assert result.is_ok()

    attrs = result.unwrap()
    assert "SOLVER" in attrs
    assert attrs["SOLVER"] == 4
    assert "MIP Relative Gap" in attrs
    assert attrs["MIP Relative Gap"] == 0.01
    assert "MIP Maximum Threads" in attrs
    assert attrs["MIP Maximum Threads"] == 20


def test_plexos_model_v12_defaults():
    """Test Model attributes and defaults from the v12 reference."""
    result = convert_simulation_config_to_attributes(PLEXOSModel(name="TestModel"))

    assert result.is_ok()
    assert result.unwrap() == {
        "Enabled": 0,
        "Execution Order": 0,
        "Load Custom Assemblies": -1,
        "Make Unique Name": 0,
        "Objective Absolute Tolerance": 0.0,
        "Objective Priority": 1,
        "Objective Relative Tolerance": 0.0,
        "Objective Weight": 1.0,
        "Output to Folder": -1,
        "Random Number Seed": 0,
        "Run Mode": 0,
        "Write Input": 0,
    }


def test_plexos_horizon_v12_enum_constraints():
    """Test Horizon enum constraints from the v12 reference."""
    horizon = PLEXOSHorizon(
        name="TestHorizon",
        look_ahead_indicator=-1,
        look_ahead_type=6,
        step_type=4,
    )
    assert horizon.look_ahead_indicator == -1
    assert horizon.look_ahead_type == 6
    assert horizon.step_type == 4

    with pytest.raises(ValueError):
        PLEXOSHorizon(name="InvalidLookAhead", look_ahead_indicator=1)
    with pytest.raises(ValueError):
        PLEXOSHorizon(name="InvalidStepType", step_type=0)
    with pytest.raises(ValueError):
        PLEXOSHorizon(name="InvalidLookAheadType", look_ahead_type=3)


def test_validate_simulation_config_success():
    """Test full config validation against the v12 schema."""
    db = PlexosDB.from_xml(SCHEMA_PATH)
    perf = PLEXOSPerformance(
        name="ValidPerf",
        solver=4,
        mip_relative_gap=0.01,
    )

    result = validate_simulation_config(db, ClassEnum.Performance, perf)
    assert result.is_ok()


def test_validate_simulation_config_failure():
    """Test full config validation (invalid)."""
    # Create a custom config with an invalid field
    # Note: This test is tricky because Pydantic validates field names at creation time
    # So we test the validation logic by using a valid object first
    perf = PLEXOSPerformance(name="TestPerf", solver=4)

    # Create an in-memory DB (won't have proper schema)
    db = PlexosDB()

    # This should fail because the in-memory DB doesn't have attribute schema
    result = validate_simulation_config(db, ClassEnum.Performance, perf)
    assert result.is_err()


def test_plexos_performance_mip_start_solution_modes():
    """Test MIP Start Solution accepts only the documented integer modes."""
    assert PLEXOSPerformance(name="DefaultPerf").mip_start_solution == 1
    for mode in (0, 1, 2):
        assert PLEXOSPerformance(name=f"Mode{mode}", mip_start_solution=mode).mip_start_solution == mode

    with pytest.raises(ValueError):
        PLEXOSPerformance(name="FractionalMode", mip_start_solution=1.5)


def test_ingest_simulation_config_to_plexosdb():
    """Test single simulation config ingestion."""
    db = PlexosDB.from_xml(SCHEMA_PATH)
    perf = PLEXOSPerformance(
        name="Test_Performance",
        solver=4,
        mip_relative_gap=0.01,
        mip_maximum_threads=20,
    )

    result = ingest_simulation_config_to_plexosdb(
        db,
        ClassEnum.Performance,
        perf,
        validate=True,
    )

    assert result.is_ok()
    info = result.unwrap()

    assert info["object_name"] == "Test_Performance"
    assert info["class"] == "Performance"
    assert info["attribute_count"] == 43
    assert "SOLVER" in info["attributes_added"]
    assert "MIP Relative Gap" in info["attributes_added"]
    assert "MIP Maximum Threads" in info["attributes_added"]


def test_build_with_simulation_config():
    """Test build_plexos_simulation with sim config."""
    # Create simulation configs
    sim_configs = {
        "performance": PLEXOSPerformance(name="MyPerf", solver=4),
        "report": PLEXOSReport(
            name="MyReport",
            output_results_by_day=-1,
            output_results_by_month=-1,
        ),
    }

    result = build_plexos_simulation(
        {"horizon_year": 2012, "resolution": "1D"},
        simulation_config=sim_configs,
    )

    assert result.is_ok()
    build_result = result.unwrap()

    assert build_result.simulation_configs is not None
    assert "performance" in build_result.simulation_configs
    assert "report" in build_result.simulation_configs


def test_ingest_full_simulation_with_config():
    """Test simulation config objects are included in build result."""
    # Create simulation configs
    sim_configs = {
        "performance": PLEXOSPerformance(
            name="Test_Perf",
            solver=4,
            mip_relative_gap=0.02,
        ),
        "st_schedule": PLEXOSSTSchedule(
            name="Test_ST",
            stochastic_method=1,
            step_link_mode=0,
        ),
        "diagnostic": None,  # Test that None values are skipped
    }

    # Build simulation
    build_result_res = build_plexos_simulation(
        {"horizon_year": 2012, "resolution": "1D"},
        simulation_config=sim_configs,
    )
    assert build_result_res.is_ok()
    build_result = build_result_res.unwrap()

    # Verify simulation configs are in the result
    assert build_result.simulation_configs is not None
    assert "performance" in build_result.simulation_configs
    assert "st_schedule" in build_result.simulation_configs
    assert "diagnostic" in build_result.simulation_configs

    # Verify the config objects
    perf_config = build_result.simulation_configs["performance"]
    st_config = build_result.simulation_configs["st_schedule"]
    assert perf_config is not None
    assert st_config is not None
    assert perf_config.name == "Test_Perf"
    assert st_config.name == "Test_ST"
    assert build_result.simulation_configs["diagnostic"] is None

    # Check for data type consistency
    assert isinstance(build_result.models, list)
    assert isinstance(build_result.horizons, list)


def test_simulation_config_examples():
    """Test all .example() methods work."""
    mt_schedule = PLEXOSMTSchedule.example()
    assert mt_schedule.name == "MT_Schedule_Example"
    assert mt_schedule.step_type == 4

    st_schedule = PLEXOSSTSchedule.example()
    assert st_schedule.name == "ST_Schedule_Example"
    assert st_schedule.stochastic_method == 1
    assert st_schedule.transmission_detail == 1

    production = PLEXOSProduction.example()
    assert production.name == "Production_Example"
    assert production.unit_commitment_optimality == 2

    pasa = PLEXOSPASA.example()
    assert pasa.name == "PASA_Example"
    assert pasa.step_type == 1

    performance = PLEXOSPerformance.example()
    assert performance.name == "Performance_Example"
    assert performance.solver == 4

    report = PLEXOSReport.example()
    assert report.name == "Report_Example"
    assert report.output_results_by_day == -1

    transmission = PLEXOSTransmission.example()
    assert transmission.name == "Transmission_Example"
    assert transmission.opf_method == 1

    diagnostic = PLEXOSDiagnostic.example()
    assert diagnostic.name == "Diagnostic_Example"
