"""The following file contains Pydantic models for a PLEXOS Horizon class."""

from typing import Annotated, Literal

from pydantic import Field

from .component import PLEXOSConfiguration


class PLEXOSModel(PLEXOSConfiguration):
    """PLEXOS Model class."""

    enabled: Annotated[
        Literal[0, -1],
        Field(
            alias="Enabled",
            description="If the model is ready to be executed",
        ),
    ] = 0
    execution_order: Annotated[
        int,
        Field(
            alias="Execution Order",
            description="Order in which to execute the Model when running in a batch.",
            ge=0,
        ),
    ] = 0
    load_custom_assemblies: Annotated[
        Literal[0, -1],
        Field(
            alias="Load Custom Assemblies",
            description="If the Model should run custom OpenPLEXOS assemblies.",
        ),
    ] = -1
    make_unique_name: Annotated[
        Literal[-1, 0, 1],
        Field(
            alias="Make Unique Name",
            description="If model output should be written to a unique filename for each execution",
        ),
    ] = 0
    objective_absolute_tolerance: Annotated[
        float,
        Field(
            alias="Objective Absolute Tolerance",
            description=(
                "Allowable degradation for the objective when doing hierarchical multi-objective "
                "optimization for MIP models"
            ),
            ge=0,
        ),
    ] = 0.0
    objective_priority: Annotated[
        int,
        Field(
            alias="Objective Priority",
            description="Priority of the objective when doing hierarchical multi-objective optimization",
            ge=0,
        ),
    ] = 1
    objective_relative_tolerance: Annotated[
        float,
        Field(
            alias="Objective Relative Tolerance",
            description=(
                "Allowable relative degradation for the objective when doing hierarchical multi-objective "
                "optimization for MIP models"
            ),
            ge=0,
            le=1,
        ),
    ] = 0.0
    objective_weight: Annotated[
        float,
        Field(
            alias="Objective Weight",
            description="Weight of the objective when doing blended multi-objective optimization",
        ),
    ] = 1.0
    output_to_folder: Annotated[
        Literal[0, -1],
        Field(
            alias="Output to Folder",
            description=(
                "If model output should be written to a folder under the source directory, "
                "or into the source folder itself"
            ),
        ),
    ] = -1
    random_number_seed: Annotated[
        float | int,
        Field(
            alias="Random Number Seed",
            description="Random number seed for this model",
            ge=0,
            le=2147483647,
        ),
    ] = 0
    run_mode: Annotated[
        Literal[0, 1],
        Field(
            alias="Run Mode",
            description="Switches between Normal and Dry run modes",
        ),
    ] = 0
    write_input: Annotated[
        Literal[0, -1],
        Field(
            alias="Write Input",
            description="If Model input should be written to the output location along with the solution.",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSModel":
        """Create an example PlexosModel."""
        return PLEXOSModel(
            name="PLEXOSModel",
        )


class PLEXOSHorizon(PLEXOSConfiguration):
    """PLEXOS Horizon class."""

    chrono_at_a_time: Annotated[
        float | int,
        Field(
            alias="Chrono At a Time",
            description="Number of steps in the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_date_from: Annotated[
        float | int,
        Field(
            alias="Chrono Date From",
            description="Start date for the chronological model",
            ge=0,
        ),
    ] = 43831
    chrono_period_from: Annotated[
        float | int,
        Field(
            alias="Chrono Period From",
            description="Start interval for the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_period_to: Annotated[
        float | int,
        Field(
            alias="Chrono Period To",
            description="End interval for the chronological model",
            ge=1,
        ),
    ] = 24
    chrono_step_count: Annotated[
        float | int,
        Field(
            alias="Chrono Step Count",
            description="Number of step types in each step of the chronological model",
            ge=1,
        ),
    ] = 1
    chrono_step_type: Annotated[
        int,
        Field(
            alias="Chrono Step Type",
            description="Chronological model step type",
            json_schema_extra={"enum": [-1, 0, 1, 2, 3]},
        ),
    ] = 2
    chronology: Annotated[
        int,
        Field(
            alias="Chronology",
            description="Type of chronology used",
            json_schema_extra={"enum": [0, 1]},
        ),
    ] = 0
    compression_factor: Annotated[
        float | int,
        Field(
            alias="Compression Factor",
            description="Number of intervals to output per interval simulated",
            ge=1,
        ),
    ] = 1
    date_from: Annotated[
        float | int,
        Field(
            alias="Date From",
            description="Start date of the planning horizon",
            ge=0,
        ),
    ] = 43831
    day_beginning: Annotated[
        float | int,
        Field(
            alias="Day Beginning",
            description="Start hour of the trading day",
            ge=0,
            le=23,
        ),
    ] = 0
    look_ahead_at_a_time: Annotated[
        float | int,
        Field(
            alias="Look-ahead At a Time",
            description="Number of step types in each step of the chronological model look-ahead",
            ge=1,
        ),
    ] = 1
    look_ahead_count: Annotated[
        float | int,
        Field(
            alias="Look-ahead Count",
            description="Number of additional look-ahead steps in the planning horizon",
            ge=0,
        ),
    ] = 0
    look_ahead_indicator: Annotated[
        Literal[0, -1],
        Field(
            alias="Look-ahead Indicator",
            description="Flag if chronological model used a look-ahead",
        ),
    ] = 0
    look_ahead_periods_per_day: Annotated[
        float | int,
        Field(
            alias="Look-ahead Periods per Day",
            description="Number of intervals in each trading day of the look-ahead",
            ge=1,
            le=86400,
        ),
    ] = 24
    look_ahead_type: Annotated[
        Literal[0, 1, 2, 6],
        Field(
            alias="Look-ahead Type",
            description="Step type for look-ahead in chronological model",
        ),
    ] = 1
    periods_per_day: Annotated[
        float | int,
        Field(
            alias="Periods per Day",
            description="Number of intervals in each trading day",
            ge=1,
            le=86400,
        ),
    ] = 24
    step_count: Annotated[
        float | int,
        Field(
            alias="Step Count",
            description="Number of steps in the planning horizon",
            ge=1,
        ),
    ] = 1
    step_type: Annotated[
        Literal[1, 2, 3, 4],
        Field(
            alias="Step Type",
            description="Planning horizon step type",
        ),
    ] = 1
    week_beginning: Annotated[
        float | int,
        Field(
            alias="Week Beginning",
            description="Start day for weekly constraints",
            ge=-1,
            le=7,
        ),
    ] = 0
    year_ending: Annotated[
        float | int,
        Field(
            alias="Year Ending",
            description="Last month of the fiscal year",
            ge=0,
            le=12,
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSHorizon":
        """Create an example PlexosHorizon."""
        return PLEXOSHorizon(
            name="ExampleHorizon",
            step_type=1,
            periods_per_day=24,
            step_count=1,
        )
