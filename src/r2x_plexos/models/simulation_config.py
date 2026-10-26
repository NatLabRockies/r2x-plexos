"""Pydantic models for PLEXOS simulation configuration objects."""

from typing import Annotated, Literal

from pydantic import Field

from .component import PLEXOSConfiguration

# PLEXOS Yes/No attributes: Yes (-1), No (0)
ALLOWED_SET = Literal[0, -1]


class PLEXOSLTPlan(PLEXOSConfiguration):
    """PLEXOS LT Plan (Long-Term Plan) configuration."""

    allow_capacity_sharing: Annotated[
        ALLOWED_SET,
        Field(
            alias="Allow Capacity Sharing",
            description="If regions/zones can share capacity reserves to meet requirements.",
        ),
    ] = 0
    always_annualize_build_cost: Annotated[
        ALLOWED_SET,
        Field(
            alias="Always Annualize Build Cost",
            description="If Build Cost is always annualized even when an option retires inside the horizon.",
        ),
    ] = 0
    at_a_time: Annotated[
        int,
        Field(
            alias="At a Time",
            description="Number of years solved in each step of LT Plan.",
            ge=0,
        ),
    ] = 0
    block_count: Annotated[
        int,
        Field(
            alias="Block Count",
            description="Number of load duration curve blocks in each day/week/month.",
            ge=1,
        ),
    ] = 12
    bridge_constraints: Annotated[
        ALLOWED_SET,
        Field(
            alias="Bridge Constraints",
            description="If LT Plan should bridge (decompose) constraints for subsequent simulation phases.",
        ),
    ] = -1
    bridge_storage: Annotated[
        ALLOWED_SET,
        Field(
            alias="Bridge Storage",
            description="If LT Plan should bridge (decompose) storage for subsequent simulation phases.",
        ),
    ] = -1
    capacity_payments_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Capacity Payments Enabled",
            description="If payments are made for generation capacity.",
        ),
    ] = 0
    chronology: Annotated[
        Literal[2, 3, 4],
        Field(alias="Chronology", description="Type of chronology used."),
    ] = 2
    co_optimize_unit_commitment: Annotated[
        ALLOWED_SET,
        Field(
            alias="Co-optimize Unit Commitment",
            description="If unit commitment is co-optimized rather than sequentially optimized with expansion.",
        ),
    ] = 0
    compute_multi_area_reliability_indices: Annotated[
        ALLOWED_SET,
        Field(
            alias="Compute Multi-area Reliability Indices",
            description="Master switch for computation of reliability indices LOLP, LOLE, etc across multiple areas.",
        ),
    ] = 0
    compute_reliability_indices: Annotated[
        ALLOWED_SET,
        Field(
            alias="Compute Reliability Indices",
            description="Master switch for computation of reliability indices LOLP, LOLE, etc.",
        ),
    ] = 0
    decomposition: Annotated[
        int,
        Field(
            alias="Decomposition",
            description="Reduce the number of simulated periods by this factor in the first pass of a two-pass decomposition.",
            ge=1,
        ),
    ] = 1
    depreciation_method: Annotated[
        Literal[0, 1, 2],
        Field(alias="Depreciation Method", description="Depreciation method for computing annuities."),
    ] = 0
    discount_period_type: Annotated[
        Literal[1, 2, 3, 4, 6, 7],
        Field(
            alias="Discount Period Type",
            description="A unique discount factor and expansion decisions will be computed for each of these periods.",
        ),
    ] = 4
    discount_rate: Annotated[
        float,
        Field(alias="Discount Rate", description="Discount rate.", ge=0),
    ] = 0.0
    end_effects_method: Annotated[
        Literal[0, 1],
        Field(alias="End Effects Method", description="Method used to account for end of horizon effects."),
    ] = 1
    inflation_rate: Annotated[
        float,
        Field(
            alias="Inflation Rate",
            description="Inflation rate used in depreciation calculations.",
        ),
    ] = 0.0
    integerization_horizon: Annotated[
        int,
        Field(
            alias="Integerization Horizon",
            description="Number of years over which the expansion decisions are integerized.",
            ge=-1,
        ),
    ] = -1
    last_block_count: Annotated[
        int,
        Field(
            alias="Last Block Count",
            description=(
                "Number of load duration curve blocks in the last load duration curve of the horizon, "
                "or zero if same as Block Count."
            ),
            ge=0,
        ),
    ] = 0
    ldc_pin_bottom: Annotated[
        int,
        Field(
            alias="LDC Pin Bottom",
            description="Number of points at the bottom of the LDC that are pinned and cannot be aggregated into blocks.",
            ge=-1,
        ),
    ] = -1
    ldc_pin_top: Annotated[
        int,
        Field(
            alias="LDC Pin Top",
            description="Number of points at the top of the LDC that are pinned and cannot be aggregated into blocks.",
            ge=-1,
        ),
    ] = -1
    ldc_slicing_method: Annotated[
        Literal[0, 1],
        Field(alias="LDC Slicing Method", description="Method used to slice the LDC into blocks."),
    ] = 0
    ldc_type: Annotated[
        Literal[1, 2, 3, 4, 7],
        Field(alias="LDC Type", description="One LDC is created for each period of this type in horizon."),
    ] = 3
    ldc_weight_a: Annotated[
        float,
        Field(alias="LDC Weight a", description="LDC weighting polynomial function 'a' parameter."),
    ] = 0.0
    ldc_weight_b: Annotated[
        float,
        Field(alias="LDC Weight b", description="LDC weighting polynomial function 'b' parameter."),
    ] = 1.0
    ldc_weight_c: Annotated[
        float,
        Field(alias="LDC Weight c", description="LDC weighting polynomial function 'c' parameter."),
    ] = 0.0
    ldc_weight_d: Annotated[
        float,
        Field(alias="LDC Weight d", description="LDC weighting polynomial function 'd' parameter."),
    ] = 0.0
    limit_capital_cost_perpetuity_by_economic_life: Annotated[
        ALLOWED_SET,
        Field(
            alias="Limit Capital Cost Perpetuity by Economic Life",
            description=(
                "With perpetuity end effect, if annuity calculation should extend beyond the end of the "
                "horizon up to the Economic Life."
            ),
        ),
    ] = -1
    maintenance_rate_output_basis: Annotated[
        Literal[0, 1],
        Field(
            alias="Maintenance Rate Output Basis",
            description="Basis for Maintenance Rate (%) Output due to Maintenance Sculpting.",
        ),
    ] = 0
    maintenance_sculpting: Annotated[
        ALLOWED_SET,
        Field(
            alias="Maintenance Sculpting",
            description="If LT Plan should sculpt maintenance derating according to the inverse of load.",
        ),
    ] = 0
    optimality: Annotated[
        Literal[0, 1, 2],
        Field(alias="Optimality", description="LT Plan integerization scheme."),
    ] = 2
    optimize_expansion: Annotated[
        ALLOWED_SET,
        Field(
            alias="Optimize Expansion",
            description="If expansion is optimized by this simulation phase.",
        ),
    ] = -1
    outage_increment: Annotated[
        float,
        Field(alias="Outage Increment", description="Generator outage bin size in convolution.", ge=1),
    ] = 10.0
    overlap: Annotated[
        int,
        Field(
            alias="Overlap",
            description="Number of years overlap between steps where -1 invokes Rolling Horizon.",
            ge=-1,
        ),
    ] = 0
    pricing_method: Annotated[
        Literal[0, 1],
        Field(alias="Pricing Method", description="Type of generator pricing used in LT Plan."),
    ] = 0
    reduced_sample_count: Annotated[
        int,
        Field(
            alias="Reduced Sample Count",
            description="Statistically reduce the Sample Type periods to at most this number of samples each year.",
            ge=1,
        ),
    ] = 4
    reduction_relative_accuracy: Annotated[
        float,
        Field(
            alias="Reduction Relative Accuracy",
            description="Stop sample reduction when the accuracy of the reduced sample set reaches this level.",
            ge=0,
            le=1,
        ),
    ] = 1.0
    sample_type: Annotated[
        Literal[1, 2, 3, 4],
        Field(alias="Sample Type", description="For Chronology = Sampled, take this type of sample."),
    ] = 2
    sample_year_count: Annotated[
        int,
        Field(
            alias="Sample Year Count",
            description="For Chronology = Sampled, first select this many years then sample within only those years.",
            ge=0,
        ),
    ] = 0
    sampled_block_count: Annotated[
        int,
        Field(
            alias="Sampled Block Count",
            description=(
                "Number of blocks modeled in each sample period (day/week) where zero means the original "
                "number of intervals."
            ),
            ge=0,
        ),
    ] = 0
    sampled_shallow_storage_cutoff: Annotated[
        float,
        Field(
            alias="Sampled Shallow Storage Cutoff",
            description=(
                "For Chronology = Sampled and Sampled Shallow Storage Treatment = Custom, maximum storage "
                "duration in hours for shallow storage formulation."
            ),
            ge=0,
        ),
    ] = 0.0
    sampled_shallow_storage_treatment: Annotated[
        Literal[1, 2, 3],
        Field(
            alias="Sampled Shallow Storage Treatment",
            description="For Chronology = Sampled, method used for formulating low duration storages and batteries.",
        ),
    ] = 1
    sampling_interval: Annotated[
        Literal[-1, 2, 3, 4, 7],
        Field(
            alias="Sampling Interval",
            description="For Chronology = Sampled, take Sample Type samples in each of these intervals of time.",
        ),
    ] = 4
    solution_count: Annotated[
        int,
        Field(
            alias="Solution Count",
            description="Maximum number of solutions produced in the solution hierarchy.",
            ge=1,
        ),
    ] = 1
    solution_quality: Annotated[
        float,
        Field(
            alias="Solution Quality",
            description="Continue producing solutions up to Solution Count or when Solution Quality falls to this level.",
            ge=0,
            le=100,
        ),
    ] = 0.0
    start_cost_amortization_period: Annotated[
        int,
        Field(
            alias="Start Cost Amortization Period",
            description="Number of hours over which generator start costs are amortized.",
            ge=0,
        ),
    ] = 0
    stochastic_method: Annotated[
        Literal[0, 1, 2, 3],
        Field(alias="Stochastic Method", description="Stochastic optimization method for LT Plan."),
    ] = 0
    storage_formulate_head_effects: Annotated[
        ALLOWED_SET,
        Field(
            alias="Storage Formulate Head Effects",
            description="If storage head effects should be formulated in LT Plan.",
        ),
    ] = -1
    storage_restart: Annotated[
        ALLOWED_SET,
        Field(
            alias="Storage Restart",
            description="If recycled storage should restart from their initial volume at the start of each capacity optimization period.",
        ),
    ] = 0
    tax_rate: Annotated[
        float,
        Field(alias="Tax Rate", description="Tax rate used to compute tax credits."),
    ] = 0.0
    use_effective_load_approach: Annotated[
        ALLOWED_SET,
        Field(
            alias="Use Effective Load Approach",
            description="If LT Plan uses the effective load approach.",
        ),
    ] = 0
    write_expansion_plan_text_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Write Expansion Plan Text Files",
            description="If expansion plans should be written to text files.",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSLTPlan":
        """Create an example LT Plan configuration."""
        return PLEXOSLTPlan(
            name="LT_Plan_Example",
            block_count=12,
            chronology=2,
            discount_period_type=4,
            optimality=2,
            optimize_expansion=-1,
            sample_type=2,
        )


class PLEXOSMTSchedule(PLEXOSConfiguration):
    """PLEXOS MT Schedule (Medium-Term Schedule) configuration."""

    at_a_time: Annotated[
        int,
        Field(
            alias="At a Time",
            description="Number of day/week/months/years in each MT Schedule simulation step.",
            ge=0,
        ),
    ] = 0
    block_count: Annotated[
        int,
        Field(
            alias="Block Count",
            description="Number of load duration curve blocks in each day/week/month.",
            ge=1,
        ),
    ] = 3
    chronology: Annotated[
        Literal[2, 3, 4, 5],
        Field(
            alias="Chronology",
            description="Type of chronology used.",
        ),
    ] = 2
    discount_period_type: Annotated[
        Literal[1, 2, 3, 4, 6, 7],
        Field(
            alias="Discount Period Type",
            description="A unique discount factor will be computed for each of these periods.",
        ),
    ] = 2
    discount_rate: Annotated[
        float,
        Field(alias="Discount Rate", description="Discount rate.", ge=0),
    ] = 0.0
    end_effects_method: Annotated[
        Literal[0, 1],
        Field(
            alias="End Effects Method",
            description="Method used to account for end of horizon discounting.",
        ),
    ] = 1
    heat_rate_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Heat Rate Detail",
            description="Level of detail for Generator heat rate modelling.",
        ),
    ] = 1
    last_block_count: Annotated[
        int,
        Field(
            alias="Last Block Count",
            description=(
                "Number of load duration curve blocks in the last load duration curve of the horizon, "
                "or zero if same as Block Count."
            ),
            ge=0,
        ),
    ] = 0
    ldc_pin_bottom: Annotated[
        int,
        Field(
            alias="LDC Pin Bottom",
            description="Number of points at the bottom of the LDC that are pinned and cannot be aggregated into blocks.",
            ge=-1,
        ),
    ] = -1
    ldc_pin_top: Annotated[
        int,
        Field(
            alias="LDC Pin Top",
            description="Number of points at the top of the LDC that are pinned and cannot be aggregated into blocks.",
            ge=-1,
        ),
    ] = -1
    ldc_slicing_method: Annotated[
        Literal[0, 1],
        Field(
            alias="LDC Slicing Method",
            description="Method used to slice the LDC into blocks.",
        ),
    ] = 0
    ldc_type: Annotated[
        Literal[1, 2, 3, 4, 7],
        Field(
            alias="LDC Type",
            description="Create one LDC for each period of this type in the horizon.",
        ),
    ] = 1
    ldc_weight_a: Annotated[
        float,
        Field(
            alias="LDC Weight a",
            description="LDC weighting polynomial function 'a' parameter.",
        ),
    ] = 0.0
    ldc_weight_b: Annotated[
        float,
        Field(
            alias="LDC Weight b",
            description="LDC weighting polynomial function 'b' parameter.",
        ),
    ] = 1.0
    ldc_weight_c: Annotated[
        float,
        Field(
            alias="LDC Weight c",
            description="LDC weighting polynomial function 'c' parameter.",
        ),
    ] = 0.0
    ldc_weight_d: Annotated[
        float,
        Field(
            alias="LDC Weight d",
            description="LDC weighting polynomial function 'd' parameter.",
        ),
    ] = 0.0
    new_entry_capacity_mechanism: Annotated[
        Literal[0, 1, 2],
        Field(alias="New Entry Capacity Mechanism", description="Capacity payment mechanism."),
    ] = 0
    new_entry_driver: Annotated[
        Literal[0, 1, 2, 3],
        Field(alias="New Entry Driver", description="New entry driver."),
    ] = 0
    new_entry_time_lag: Annotated[
        int,
        Field(alias="New Entry Time Lag", description="Lag time for entrepreneurial entry.", ge=0),
    ] = 12
    outage_increment: Annotated[
        float,
        Field(alias="Outage Increment", description="Generator outage bin size in convolution.", ge=1),
    ] = 10.0
    pricing_method: Annotated[
        Literal[0, 1],
        Field(alias="Pricing Method", description="Type of generator pricing used in MT Schedule."),
    ] = 0
    reduced_sample_count: Annotated[
        int,
        Field(
            alias="Reduced Sample Count",
            description="Statistically reduce the Sample Type periods to at most this number of samples each year.",
            ge=1,
        ),
    ] = 4
    reduction_relative_accuracy: Annotated[
        float,
        Field(
            alias="Reduction Relative Accuracy",
            description="Stop sample reduction when the accuracy of the reduced sample set reaches this level.",
            ge=0,
            le=1,
        ),
    ] = 1.0
    reliability_min_contiguous_block: Annotated[
        int,
        Field(
            alias="Reliability Min Contiguous Block",
            description=(
                "For reliability based sampled chronology ensure sampled periods are contained in contiguous "
                "blocks of at least this many hours."
            ),
            ge=0,
        ),
    ] = 0
    sample_type: Annotated[
        Literal[1, 2, 3, 4],
        Field(
            alias="Sample Type",
            description="For Chronology = Sampled, take this type of sample.",
        ),
    ] = 2
    sampled_block_count: Annotated[
        int,
        Field(
            alias="Sampled Block Count",
            description=(
                "Number of blocks modeled in each sample period (day/week) where zero means the original "
                "number of intervals."
            ),
            ge=0,
        ),
    ] = 0
    sampled_shallow_storage_cutoff: Annotated[
        float,
        Field(
            alias="Sampled Shallow Storage Cutoff",
            description=(
                "For Chronology = Sampled and Sampled Shallow Storage Treatment = Custom, maximum storage "
                "duration in hours for shallow storage formulation."
            ),
            ge=0,
        ),
    ] = 0.0
    sampled_shallow_storage_treatment: Annotated[
        Literal[1, 2, 3],
        Field(alias="Sampled Shallow Storage Treatment", description="Sampled shallow storage treatment."),
    ] = 1
    sampling_interval: Annotated[
        Literal[-1, 2, 3, 4, 7],
        Field(
            alias="Sampling Interval",
            description="For Chronology = Sampled, take Sample Type samples in each of these intervals of time.",
        ),
    ] = 4
    start_cost_amortization_period: Annotated[
        int,
        Field(
            alias="Start Cost Amortization Period",
            description="Number of hours over which generator start costs are amortized.",
            ge=0,
        ),
    ] = 0
    step_link_mode: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Step Link Mode",
            description="Controls how the solutions of each step are linked together.",
        ),
    ] = 0
    step_type: Annotated[
        Literal[1, 2, 3, 4, 7],
        Field(
            alias="Step Type",
            description="Each simulation step will span steps of this type.",
        ),
    ] = 4
    stochastic_algorithm: Annotated[
        Literal[0, 1],
        Field(
            alias="Stochastic Algorithm",
            description="Algorithm invoked by the Stochastic Method when a scenario tree is present.",
        ),
    ] = 0
    stochastic_method: Annotated[
        Literal[0, 1, 2, 3],
        Field(
            alias="Stochastic Method",
            description="Stochastic optimization method for MT Schedule.",
        ),
    ] = 0
    storage_formulate_head_effects: Annotated[
        ALLOWED_SET,
        Field(
            alias="Storage Formulate Head Effects",
            description="If storage head effects should be formulated in MT Schedule.",
        ),
    ] = -1
    transmission_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Transmission Detail",
            description="Level of detail used in transmission modelling in MT Schedule.",
        ),
    ] = 0
    use_effective_load_approach: Annotated[
        ALLOWED_SET,
        Field(
            alias="Use Effective Load Approach",
            description="If MT Schedule uses the effective load approach.",
        ),
    ] = 0
    write_bridge_text_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Write Bridge Text Files",
            description=(
                "If bridge information such as constraint decomposition and storage targets should be written "
                "to text files."
            ),
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSMTSchedule":
        """Create an example MT Schedule configuration."""
        return PLEXOSMTSchedule(
            name="MT_Schedule_Example",
            at_a_time=1,
            block_count=6,
            chronology=2,
            discount_rate=0.05,
            ldc_type=1,
            new_entry_time_lag=12,
            outage_increment=10,
            reduced_sample_count=4,
            sample_type=2,
            step_type=4,
            stochastic_method=0,
        )


class PLEXOSSTSchedule(PLEXOSConfiguration):
    """PLEXOS ST Schedule (Short-Term Schedule) configuration."""

    discount_period_type: Annotated[
        Literal[1, 2, 3, 4, 6, 7],
        Field(
            alias="Discount Period Type",
            description="A unique discount factor will be computed for each of these periods.",
        ),
    ] = 2
    discount_rate: Annotated[
        float,
        Field(alias="Discount Rate", description="Discount rate.", ge=0),
    ] = 0.0
    discount_rate_reset: Annotated[
        ALLOWED_SET,
        Field(
            alias="Discount Rate Reset",
            description="Flag if discount factor resets at the beginning of each ST Schedule step.",
        ),
    ] = 0
    end_effects_method: Annotated[
        Literal[0, 1],
        Field(
            alias="End Effects Method",
            description="Method used to account for end of horizon discounting.",
        ),
    ] = 1
    heat_rate_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Heat Rate Detail",
            description="Level of detail for Generator heat rate modelling.",
        ),
    ] = 0
    sequential_steps: Annotated[
        int,
        Field(
            alias="Sequential Steps",
            description="Number of steps run in each sequential block when running in parallel Step Link Mode.",
            ge=1,
        ),
    ] = 1
    step_link_mode: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Step Link Mode",
            description="Controls how the solutions of each step are linked together.",
        ),
    ] = 0
    step_relink_count: Annotated[
        int,
        Field(
            alias="Step Relink Count",
            description=(
                "Number of steps that require relinking of initial conditions in parallel Step Link Mode, "
                "where 1 means no relinking."
            ),
            ge=1,
        ),
    ] = 1
    stochastic_method: Annotated[
        Literal[0, 1, 2, 3],
        Field(
            alias="Stochastic Method",
            description="Stochastic optimization method for ST Schedule.",
        ),
    ] = 1
    storage_formulate_head_effects: Annotated[
        ALLOWED_SET,
        Field(
            alias="Storage Formulate Head Effects",
            description="If storage head effects should be formulated in ST Schedule.",
        ),
    ] = -1
    transmission_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Transmission Detail",
            description="Transmission modeling detail: Regional (0), Nodal (1), Zonal (2)",
        ),
    ] = 1

    @classmethod
    def example(cls) -> "PLEXOSSTSchedule":
        """Create an example ST Schedule configuration."""
        return PLEXOSSTSchedule(
            name="ST_Schedule_Example",
        )


class PLEXOSProduction(PLEXOSConfiguration):
    """PLEXOS Production simulation configuration."""

    battery_station_aggregation_mode: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Battery Station Aggregation Mode",
            description="Automatically create Battery Station objects by aggregating Batteries",
        ),
    ] = 0
    capacity_factor_constraint_basis: Annotated[
        Literal[0, 1],
        Field(
            alias="Capacity Factor Constraint Basis",
            description="Basis for capacity factor constraints.",
        ),
    ] = 0
    cooling_states_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Cooling States Enabled", description="If generator unit cooling states are enabled"),
    ] = -1
    dispatch_by_battery_station: Annotated[
        ALLOWED_SET,
        Field(
            alias="Dispatch by Battery Station",
            description="If batteries should be aggregated through manual or automatic battery stations for dispatch.",
        ),
    ] = -1
    dispatch_by_power_station: Annotated[
        ALLOWED_SET,
        Field(
            alias="Dispatch by Power Station",
            description="If generators in power stations should be treated in aggregate for dispatch.",
        ),
    ] = -1
    dp_capacity_factor_error_threshold: Annotated[
        float,
        Field(
            alias="DP Capacity Factor Error Threshold",
            description=(
                "Error in DP capacity factor compared to MT Schedule capacity factor below which "
                "the DP solution is accepted"
            ),
            ge=0,
        ),
    ] = 20.0
    dp_capacity_factor_threshold: Annotated[
        float,
        Field(
            alias="DP Capacity Factor Threshold",
            description="Minimum capacity factor for generators to be dispatched by the DP",
            ge=0,
        ),
    ] = 20.0
    forced_outage_relaxes_min_down_time: Annotated[
        ALLOWED_SET,
        Field(
            alias="Forced Outage Relaxes Min Down Time",
            description="If forced outages are allowed to relax minimum down-time constraints.",
        ),
    ] = 0
    formulate_ramp_upfront: Annotated[
        ALLOWED_SET,
        Field(
            alias="Formulate Ramp Upfront",
            description="If ramp constraints should all be formulated upfront rather than checked iteratively.",
        ),
    ] = -1
    formulate_upfront: Annotated[
        ALLOWED_SET,
        Field(
            alias="Formulate Upfront",
            description="If production constraints should all be formulated upfront rather than checked iteratively.",
        ),
    ] = -1
    fuel_use_function_precision: Annotated[
        float,
        Field(
            alias="Fuel Use Function Precision",
            description="Precision for calculation of linear approximation to fuel function.",
            ge=0,
        ),
    ] = 0.0
    gas_demand_resolution: Annotated[
        Literal[0, 1, 2],
        Field(alias="Gas Demand Resolution", description="Resolution of input gas demands"),
    ] = 0
    heat_rate_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Heat Rate Detail",
            description="Level of detail for Generator heat rate modelling",
        ),
    ] = 0
    heat_rate_error_method: Annotated[
        Literal[0, 1, 2, 3, 4],
        Field(
            alias="Heat Rate Error Method",
            description="Method for handling non-convex heat rate functions.",
        ),
    ] = 2
    increment_and_decrement: Annotated[
        ALLOWED_SET,
        Field(
            alias="Increment and Decrement",
            description="If simultaneous closure of increment and decrement bids/offers around a base is allowed",
        ),
    ] = -1
    integers_in_look_ahead: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Integers in Look-ahead",
            description="Controls when the look-ahead contains integers for unit commitment and other decisions.",
        ),
    ] = 1
    max_heat_rate_tranches: Annotated[
        int,
        Field(
            alias="Max Heat Rate Tranches",
            description="Maximum number of tranches in the fuel function piecewise linear approximation",
            ge=1,
            le=100,
        ),
    ] = 10
    min_heat_rate_tranche_size: Annotated[
        float,
        Field(
            alias="Min Heat Rate Tranche Size",
            description="Minimum tranche size in fuel function piecewise linear approximation",
            ge=0,
        ),
    ] = 0.0
    power_station_aggregation_mode: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Power Station Aggregation Mode",
            description="Automatically create Power Station objects by aggregating Generators",
        ),
    ] = 0
    pump_and_generate: Annotated[
        ALLOWED_SET,
        Field(
            alias="Pump and Generate",
            description=(
                "If pumped storage/battery is allowed to pump/charge and generate/discharge simultaneously"
            ),
        ),
    ] = -1
    ramping_constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Ramping Constraints Enabled",
            description="If facility ramping constraints are modeled",
        ),
    ] = -1
    rounded_relaxation_commitment_model: Annotated[
        Literal[0, 1],
        Field(
            alias="Rounded Relaxation Commitment Model",
            description="Determines if the unit commitment decisions are made centrally or by self-commitment",
        ),
    ] = 0
    rounded_relaxation_end_threshold: Annotated[
        float,
        Field(
            alias="Rounded Relaxation End Threshold",
            description="End value for [Rounding Up Threshold] when Rounded Relaxation is running self-tune.",
            le=1,
        ),
    ] = 0.75
    rounded_relaxation_start_threshold: Annotated[
        float,
        Field(
            alias="Rounded Relaxation Start Threshold",
            description="Start value for [Rounding Up Threshold] when Rounded Relaxation is running self-tune.",
            ge=0,
        ),
    ] = 0.25
    rounded_relaxation_threshold_increment: Annotated[
        float,
        Field(
            alias="Rounded Relaxation Threshold Increment",
            description="Increment value for [Rounding Up Threshold] when Rounded Relaxation is running self-tune.",
            ge=0.01,
        ),
    ] = 0.05
    rounded_relaxation_tuning: Annotated[
        ALLOWED_SET,
        Field(
            alias="Rounded Relaxation Tuning",
            description="If the Rounded Relaxation method should self-tune the [Rounding Up Threshold].",
        ),
    ] = 0
    rounding_up_threshold: Annotated[
        float,
        Field(
            alias="Rounding Up Threshold",
            description="Threshold at which non-integers are rounded up.",
            ge=0,
            le=1,
        ),
    ] = 0.5
    run_up_and_down_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Run Up and Down Enabled", description="If generator run up and run down are modeled"),
    ] = -1
    start_and_stop_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Start and Stop Enabled", description="If facility start up and shut down are modeled"),
    ] = -1
    start_cost_method: Annotated[
        Literal[0, 1],
        Field(
            alias="Start Cost Method",
            description=(
                "Method for handling start costs in the mathematical formulation "
                "(integrate into the formulation, or calculate ex-post)"
            ),
        ),
    ] = 0
    transitions_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Transitions Enabled", description="If generator transitions are modeled"),
    ] = -1
    unit_commitment_heat_rate_detail: Annotated[
        ALLOWED_SET,
        Field(
            alias="Unit Commitment Heat Rate Detail",
            description=(
                "If modeling fully detailed heat rates for unit commitment. Otherwise perform a two-pass UC/ED."
            ),
        ),
    ] = -1
    unit_commitment_optimality: Annotated[
        Literal[0, 1, 2, 3],
        Field(alias="Unit Commitment Optimality", description="Unit commitment integerization scheme."),
    ] = 0
    warm_up_process_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Warm Up Process Enabled",
            description="If modeling of Facility Warm Up Process is enabled",
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSProduction":
        """Create an example Production configuration."""
        return PLEXOSProduction(
            name="Production_Example",
            unit_commitment_optimality=2,
            rounding_up_threshold=0.26,
            max_heat_rate_tranches=1,
        )


class PLEXOSPASA(PLEXOSConfiguration):
    """PLEXOS PASA (Projected Assessment of System Adequacy) configuration."""

    compute_multi_area_reliability_indices: Annotated[
        ALLOWED_SET,
        Field(
            alias="Compute Multi-area Reliability Indices",
            description="Master switch for computation of reliability indices LOLP, LOLE, etc across multiple areas.",
        ),
    ] = 0
    compute_reliability_indices: Annotated[
        ALLOWED_SET,
        Field(
            alias="Compute Reliability Indices",
            description="Master switch for computation of reliability indices LOLP, LOLE, etc.",
        ),
    ] = 0
    constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Constraints Enabled",
            description="If transmission line constraints are enabled in PASA.",
        ),
    ] = -1
    energy_objective: Annotated[
        float,
        Field(
            alias="Energy Objective",
            description="Relative contribution of energy margins to PASA objective function",
            ge=0,
            le=100,
        ),
    ] = 0.0
    include_contract_generation: Annotated[
        ALLOWED_SET,
        Field(
            alias="Include Contract Generation",
            description="Include physical contract generation capacity in PASA",
        ),
    ] = 0
    include_contract_load: Annotated[
        ALLOWED_SET,
        Field(
            alias="Include Contract Load",
            description="Include physical contract load capacity in PASA",
        ),
    ] = 0
    include_demand_bids: Annotated[
        ALLOWED_SET,
        Field(
            alias="Include Demand Bids",
            description="Include demand bids are load in PASA",
        ),
    ] = 0
    include_dsp: Annotated[
        ALLOWED_SET,
        Field(alias="Include DSP", description="Include demand-side participation in PASA"),
    ] = -1
    include_market_purchases: Annotated[
        ALLOWED_SET,
        Field(
            alias="Include Market Purchases",
            description="Include market purchases as generation capacity in PASA",
        ),
    ] = 0
    interface_constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Interface Constraints Enabled",
            description="If transmission interface constraints are enabled in PASA.",
        ),
    ] = -1
    maintenance_sculpting: Annotated[
        float,
        Field(
            alias="Maintenance Sculpting",
            description=(
                "Level of sculpting of maintenance outages: Higher sculpting means outages are more "
                "concentrated in high reserve periods"
            ),
            ge=0,
            le=100,
        ),
    ] = 0.0
    reliability_criterion: Annotated[
        Literal[0, 1],
        Field(
            alias="Reliability Criterion",
            description="Criterion used to select periods for Monte Carlo reliability analysis",
        ),
    ] = 0
    reliability_lolp_tolerance: Annotated[
        float,
        Field(
            alias="Reliability LOLP Tolerance",
            description=(
                "For reliability-based sampled chronology using LOLP criterion select intervals "
                "with LOLP at or above this level"
            ),
            ge=0,
        ),
    ] = 0.01
    reliability_max_samples: Annotated[
        float,
        Field(
            alias="Reliability Max Samples",
            description=(
                "For reliability-based sampled chronology select no more than this many hours per year"
            ),
            ge=1,
        ),
    ] = 1e30
    step_type: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Step Type",
            description="PASA step type: One period is modelled for each period of this type in horizon.",
        ),
    ] = 1
    stochastic_method: Annotated[
        Literal[0, 1, 2, 3],
        Field(
            alias="Stochastic Method",
            description="Stochastic optimization method for PASA.",
        ),
    ] = 0
    transmission_detail: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Transmission Detail",
            description="Definition of maintenance area for PASA.",
        ),
    ] = 0
    write_outage_text_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Write Outage Text Files",
            description="If outage patterns should be written to text files",
        ),
    ] = 0
    write_reliability_text_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Write Reliability Text Files",
            description=(
                "If the periods selected for Monte Carlo reliability analysis should be written to text files"
            ),
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSPASA":
        """Create an example PASA configuration."""
        return PLEXOSPASA(
            name="PASA_Example",
            step_type=1,
            write_outage_text_files=-1,
        )


class PLEXOSPerformance(PLEXOSConfiguration):
    """PLEXOS Performance configuration for solver settings."""

    barrier_convergence_tolerance: Annotated[
        float,
        Field(
            alias="Barrier Convergence Tolerance",
            description="Termination criteria for the Barrier algorithm",
            ge=0,
            le=1,
        ),
    ] = 1e-08
    carry_over_mip_time: Annotated[
        ALLOWED_SET,
        Field(
            alias="Carry over MIP Time",
            description=(
                "If unused MIP Max Time is carried over to the next MIP solve allowing more time on harder problems"
            ),
        ),
    ] = 0
    cold_start_optimizer_1: Annotated[
        Literal[0, 1, 4, 5, 6, 9, 10],
        Field(
            alias="Cold Start Optimizer 1",
            description="First priority optimizer used to solve problems from a cold-start.",
        ),
    ] = 0
    cold_start_optimizer_2: Annotated[
        Literal[0, 1, 4, 5, 6],
        Field(
            alias="Cold Start Optimizer 2",
            description="Second priority optimizer used to solve problems from a cold-start.",
        ),
    ] = 0
    cold_start_optimizer_3: Annotated[
        Literal[0, 1, 4, 5, 6],
        Field(
            alias="Cold Start Optimizer 3",
            description="Third priority optimizer used to solve problems from a cold-start.",
        ),
    ] = 0
    concurrent_mode: Annotated[
        Literal[0, 1],
        Field(alias="Concurrent Mode", description="Mode for the concurrent optimizer"),
    ] = 0
    crossover: Annotated[
        ALLOWED_SET,
        Field(
            alias="Crossover",
            description=(
                "If crossover is used to transform the interior solution produced by barrier into a basic solution"
            ),
        ),
    ] = -1
    feasibility_repair_failure: Annotated[
        Literal[0, 1],
        Field(
            alias="Feasibility Repair Failure",
            description="Action to take if an infeasible problem cannot be repaired.",
        ),
    ] = 0
    feasibility_tolerance: Annotated[
        float,
        Field(
            alias="Feasibility Tolerance",
            description="Allowable violation of variable bounds (0 = solver default value)",
            ge=0,
        ),
    ] = 0.0
    hint_mode: Annotated[
        Literal[0, 1, 2],
        Field(alias="Hint Mode", description="Controls how variable hints are used by the solver"),
    ] = 0
    hot_start_optimizer_1: Annotated[
        Literal[0, 1, 4, 5, 6, 9, 10],
        Field(
            alias="Hot Start Optimizer 1",
            description="First priority optimizer used to solve problems from a hot-start.",
        ),
    ] = 0
    hot_start_optimizer_2: Annotated[
        Literal[0, 1, 4, 5, 6],
        Field(
            alias="Hot Start Optimizer 2",
            description="Second priority optimizer used to solve problems from a hot-start.",
        ),
    ] = 0
    hot_start_optimizer_3: Annotated[
        Literal[0, 1, 4, 5, 6],
        Field(
            alias="Hot Start Optimizer 3",
            description="Third priority optimizer used to solve problems from a hot-start.",
        ),
    ] = 0
    maximum_monitored_mip_iterations: Annotated[
        int,
        Field(
            alias="Maximum Monitored MIP Iterations",
            description="Maximum number of monitored row/column iterations with MIP enabled",
        ),
    ] = -1
    maximum_parallel_tasks: Annotated[
        int,
        Field(
            alias="Maximum Parallel Tasks",
            description=(
                "Maximum number of parallel optimizations run concurrently, where -1 means one task per CPU."
            ),
            ge=-1,
        ),
    ] = -1
    maximum_threads: Annotated[
        int,
        Field(
            alias="Maximum Threads",
            description=(
                "Maximum number of threads used simultaneously by all simplex and interior point optimizers, "
                "where -1 means all available threads."
            ),
            ge=-1,
        ),
    ] = -1
    mip_absolute_gap: Annotated[
        float,
        Field(
            alias="MIP Absolute Gap",
            description=(
                "Absolute tolerance on the gap between the best integer objective and the objective "
                "of the best node remaining"
            ),
            ge=0,
        ),
    ] = 0.0
    mip_focus: Annotated[
        Literal[0, 1, 2, 3],
        Field(alias="MIP Focus", description="High-level solution strategy for the MIP solver"),
    ] = 0
    mip_hard_stop: Annotated[
        int,
        Field(
            alias="MIP Hard Stop",
            description=(
                "Hard limit on the mixed integer programming time when using MIP Max Relative Gap "
                "and MIP Max Time is a soft limit"
            ),
            ge=-1,
        ),
    ] = -1
    mip_improve_start_gap: Annotated[
        float,
        Field(
            alias="MIP Improve Start Gap",
            description="Switch strategy to improving the best integer solution when this gap is reached.",
            ge=0,
        ),
    ] = 0.0
    mip_interrupt: Annotated[
        ALLOWED_SET,
        Field(
            alias="MIP Interrupt",
            description="If the MIP search can be interrupted by pressing the CTRL-P key combination",
        ),
    ] = 0
    mip_max_absolute_gap: Annotated[
        float,
        Field(
            alias="MIP Max Absolute Gap",
            description=(
                "When set to a value greater than zero, the MIP Max Time is treated as a soft constraint "
                "with optimization continuing until the MIP Max Absolute Gap is reached."
            ),
            ge=0,
        ),
    ] = 0.0
    mip_max_relative_gap: Annotated[
        float,
        Field(
            alias="MIP Max Relative Gap",
            description=(
                "When set to a value greater than zero, the MIP Max Time is treated as a soft constraint "
                "with optimization continuing until the MIP Max Relative Gap is reached."
            ),
            ge=0,
        ),
    ] = 0.0
    mip_max_relaxation_repair_time: Annotated[
        int,
        Field(
            alias="MIP Max Relaxation Repair Time",
            description="Maximum time allowed in repairing an infeasible mixed integer programming problem",
            ge=-1,
        ),
    ] = -1
    mip_max_time: Annotated[
        int,
        Field(
            alias="MIP Max Time",
            description="Maximum time in mixed integer programming algorithm",
            ge=-1,
        ),
    ] = -1
    mip_max_time_with_carry_over: Annotated[
        int,
        Field(
            alias="MIP Max Time with Carry over",
            description=(
                "Maximum time in mixed integer programming algorithm including any unused time carried over"
            ),
            ge=-1,
        ),
    ] = -1
    mip_maximum_threads: Annotated[
        int,
        Field(
            alias="MIP Maximum Threads",
            description=(
                "Maximum number of threads used by the mixed integer optimizer, where -1 means all available threads."
            ),
            ge=-1,
        ),
    ] = -1
    mip_node_optimizer: Annotated[
        Literal[0, 1, 4, 5, 6, 9, 10],
        Field(
            alias="MIP Node Optimizer",
            description="Optimizer used at the nodes of the branch and bound algorithm.",
        ),
    ] = 0
    mip_relative_gap: Annotated[
        float,
        Field(
            alias="MIP Relative Gap",
            description=(
                "Declare the integer solution optimal when this gap is reached between the current integer "
                "solution and best-bound linear relaxation (this is not a measure of optimality)."
            ),
            ge=0,
        ),
    ] = 0.0001
    mip_root_optimizer: Annotated[
        Literal[0, 1, 4, 5, 6, 9, 10],
        Field(
            alias="MIP Root Optimizer",
            description="Optimizer used to solve the linear relaxation of a mixed-integer program.",
        ),
    ] = 0
    mip_start_solution: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="MIP Start Solution",
            description="MIP solver will be warm started with previous solutions according to this setting.",
        ),
    ] = 1
    monitoring_maximum_threads: Annotated[
        int,
        Field(
            alias="Monitoring Maximum Threads",
            description=(
                "Maximum number of threads for monitored constraint iterations, where -1 means all available threads."
            ),
            ge=-1,
        ),
    ] = -1
    monitoring_periodic_clearing: Annotated[
        int,
        Field(
            alias="Monitoring Periodic Clearing",
            description=(
                "Clear monitored constraints and variables from the formulation periodically (number of steps)"
            ),
            ge=0,
        ),
    ] = 0
    objective_scalar: Annotated[
        float,
        Field(
            alias="Objective Scalar",
            description="Scale the objective function internally by this factor",
            gt=0,
        ),
    ] = 1.0
    objective_tolerance: Annotated[
        float,
        Field(
            alias="Objective Tolerance",
            description="Smallest allowable objective function coefficient",
            ge=0,
        ),
    ] = 0.0
    optimality_tolerance: Annotated[
        float,
        Field(
            alias="Optimality Tolerance",
            description="Optimality tolerance (0 = solver default value)",
            ge=0,
        ),
    ] = 0.0
    presolve: Annotated[
        ALLOWED_SET,
        Field(alias="Presolve", description="Toggles on/off solver presolve"),
    ] = -1
    random_number_seed: Annotated[
        int,
        Field(
            alias="Random Number Seed",
            description="Random number seed for the solver",
            ge=0,
            le=2000000000,
        ),
    ] = 0
    scaling: Annotated[
        ALLOWED_SET,
        Field(alias="Scaling", description="Toggles on/off solver problem scaling"),
    ] = -1
    small_lp_nonzero_count: Annotated[
        int,
        Field(
            alias="Small LP Nonzero Count",
            description="Maximum number of non-zeros in a 'small' linear programming problem.",
            gt=0,
        ),
    ] = 250000
    small_lp_optimizer: Annotated[
        int,
        Field(
            alias="Small LP Optimizer",
            description="Optimizer used to solve 'small' liner programming problems.",
            ge=0,
            le=9,
        ),
    ] = 0
    solver: Annotated[
        Literal[0, 1, 2, 3, 4, 6, 7, 8],
        Field(alias="SOLVER", description="SOLVER engine used by PLEXOS."),
    ] = 4
    sparsify: Annotated[
        Literal[-1, 0, 1, 2],
        Field(alias="Sparsify", description="Strategy for matrix sparsification during presolve"),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSPerformance":
        """Create an example Performance configuration."""
        return PLEXOSPerformance(
            name="Performance_Example",
            solver=4,
            mip_relative_gap=0.01,
            mip_maximum_threads=20,
            mip_max_time=6500,
        )


class PLEXOSReport(PLEXOSConfiguration):
    """PLEXOS Report configuration for output settings."""

    output_results_by_day: Annotated[
        int,
        Field(
            alias="Output Results by Day",
            description="Output results aggregated by day: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1
    output_results_by_month: Annotated[
        int,
        Field(
            alias="Output Results by Month",
            description="Output results aggregated by month: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1
    output_results_by_fiscal_year: Annotated[
        int,
        Field(
            alias="Output Results by Fiscal Year",
            description="Output results aggregated by fiscal year: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1

    @classmethod
    def example(cls) -> "PLEXOSReport":
        """Create an example Report configuration."""
        return PLEXOSReport(
            name="Report_Example",
            output_results_by_day=-1,
            output_results_by_month=-1,
            output_results_by_fiscal_year=-1,
        )


class PLEXOSTransmission(PLEXOSConfiguration):
    """PLEXOS Transmission configuration for network modeling."""

    allow_dump_energy: Annotated[
        ALLOWED_SET,
        Field(alias="Allow Dump Energy", description="Model Dump Energy in OPF"),
    ] = -1
    allow_unserved_energy: Annotated[
        ALLOWED_SET,
        Field(alias="Allow Unserved Energy", description="Model Unserved Energy in OPF"),
    ] = -1
    bound_node_phase_angles: Annotated[
        ALLOWED_SET,
        Field(
            alias="Bound Node Phase Angles",
            description="If the Node [Phase Angle] values are subject to bounds in absolute value.",
        ),
    ] = 0
    cache_transmission_matrices: Annotated[
        ALLOWED_SET,
        Field(
            alias="Cache Transmission Matrices",
            description="Cache the transmission matrices to disk for future use.",
        ),
    ] = 0
    calculate_node_lpf_from_load: Annotated[
        Literal[0, 1],
        Field(
            alias="Calculate Node LPF from Load",
            description="Automatically calculate Node LPF from input Load property",
        ),
    ] = 0
    commitment_ptdf_threshold: Annotated[
        float,
        Field(
            alias="Commitment PTDF Threshold",
            description="Minimum absolute value of PTDF coefficient in commitment solves.",
            ge=0,
        ),
    ] = 0.0
    constraint_limit_penalty: Annotated[
        float,
        Field(
            alias="Constraint Limit Penalty",
            description="Penalty for exceeding line and transformer limits.",
        ),
    ] = -1.0
    constraint_voltage_threshold: Annotated[
        float,
        Field(
            alias="Constraint Voltage Threshold",
            description="Voltage level at which thermal limits are modelled.",
            ge=0,
        ),
    ] = 0.0
    constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Constraints Enabled",
            description="If transmission line constraints are enabled.",
        ),
    ] = -1
    contingency_limit_penalty: Annotated[
        float,
        Field(
            alias="Contingency Limit Penalty",
            description="Penalty for exceeding contingency flow limits.",
        ),
    ] = -1.0
    contingency_monitoring_threshold: Annotated[
        float,
        Field(
            alias="Contingency Monitoring Threshold",
            description="Monitor only this percentage of most affected Line/Transformer/Interface flows.",
            ge=0,
            le=100,
        ),
    ] = 100.0
    convergence_report_level: Annotated[
        Literal[0, 1, 2],
        Field(
            alias="Convergence Report Level",
            description="Level of screen reporting during transmission convergence iterations "
            "(none, normal, verbose)",
        ),
    ] = 1
    detail: Annotated[
        Literal[0, 1, 2],
        Field(alias="Detail", description="Level of detail used in transmission modeling"),
    ] = 1
    detect_non_physical_losses: Annotated[
        ALLOWED_SET,
        Field(
            alias="Detect Non-physical Losses",
            description="If non-physical losses should be detected (in the piecewise linear loss model) "
            "and removed via mixed-integer programming",
        ),
    ] = 0
    energy_balance_violation_variables: Annotated[
        ALLOWED_SET,
        Field(
            alias="Energy Balance Violation Variables",
            description="Include violation variables on the transmission system energy balance equations",
        ),
    ] = 0
    enforce_limits_on_lines_in_interfaces: Annotated[
        ALLOWED_SET,
        Field(
            alias="Enforce Limits On Lines In Interfaces",
            description="If lines and transformers in interfaces should have their limits enforced "
            "regardless of voltage",
        ),
    ] = 0
    enforce_n_1_contingencies: Annotated[
        ALLOWED_SET,
        Field(
            alias="Enforce N-1 Contingencies",
            description="If N-1 contingencies should be automatically enforced.",
        ),
    ] = 0
    filter_limits: Annotated[
        ALLOWED_SET,
        Field(
            alias="Filter Limits",
            description="If transmission limits are filtered using extreme flows from MT Schedule",
        ),
    ] = 0
    flow_ptdf_threshold: Annotated[
        float,
        Field(
            alias="Flow PTDF Threshold",
            description="Minimum absolute value of PTDF as coefficient in transmission flow constraints.",
            ge=0,
        ),
    ] = 0.01
    formulate_upfront: Annotated[
        ALLOWED_SET,
        Field(
            alias="Formulate Upfront",
            description="If transmission constraints should all be formulated upfront rather than "
            "checked iteratively.",
        ),
    ] = 0
    interface_constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="Interface Constraints Enabled",
            description="If interface constraints are enabled.",
        ),
    ] = -1
    interface_limit_penalty: Annotated[
        float,
        Field(
            alias="Interface Limit Penalty",
            description="Penalty for exceeding interface flow limits.",
        ),
    ] = -1.0
    internal_voll: Annotated[
        float,
        Field(
            alias="Internal VoLL",
            description="Value of Lost Load used internally",
            ge=0,
            le=1e9,
        ),
    ] = 100000.0
    interruption_sharing: Annotated[
        ALLOWED_SET,
        Field(
            alias="Interruption Sharing",
            description="If region interruption sharing is activated.",
        ),
    ] = 0
    kron_reduced_model_input: Annotated[
        Literal[0, 1],
        Field(
            alias="Kron-reduced Model Input",
            description="Input properties and files written out for Kron-reduced model.",
        ),
    ] = 0
    limit_bootstrap_initial_threshold: Annotated[
        float,
        Field(
            alias="Limit Bootstrap Initial Threshold",
            description="Initial value of [Limit Threshold] during transmission constraint bootstrap phase.",
            ge=0,
        ),
    ] = 2.0
    limit_bootstrap_threshold_decrement: Annotated[
        float,
        Field(
            alias="Limit Bootstrap Threshold Decrement",
            description="[Limit Threshold] decrement used for each iteration of transmission bootstrap phase.",
            ge=0,
            le=1,
        ),
    ] = 0.25
    limit_threshold: Annotated[
        float,
        Field(
            alias="Limit Threshold",
            description="Line, Transformer, or Interface flow loading threshold above which limits are added.",
            ge=0,
            le=1,
        ),
    ] = 0.95
    lodf_threshold: Annotated[
        float,
        Field(
            alias="LODF Threshold",
            description="Minimum absolute value of LODF to be used in contingency flow constraints.",
            ge=0,
        ),
    ] = 0.03
    loss_method: Annotated[
        Literal[0, 1, 3, 4, 5],
        Field(
            alias="Loss Method",
            description="Formulation method used to model transmission losses.",
        ),
    ] = 0
    loss_tolerance: Annotated[
        float,
        Field(
            alias="Loss Tolerance",
            description="Relative gap between real and modelled losses.",
            ge=0,
        ),
    ] = 1e-05
    loss_voltage_threshold: Annotated[
        float,
        Field(
            alias="Loss Voltage Threshold",
            description="Voltage level at which losses are modelled.",
            ge=0,
        ),
    ] = 0.0
    losses_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Losses Enabled", description="If transmission losses are enabled."),
    ] = 0
    max_absolute_phase_angle: Annotated[
        float,
        Field(
            alias="Max Absolute Phase Angle",
            description="Maximum absolute value allowed for any Node [Phase Angle].",
            ge=0,
            le=6.28318530717958,
        ),
    ] = 6.28
    max_embedded_loss_iterations: Annotated[
        int,
        Field(
            alias="Max Embedded Loss Iterations",
            description="Maximum number of iterations for removing embedded losses from load.",
            ge=0,
        ),
    ] = 2
    max_limit_iterations: Annotated[
        int,
        Field(
            alias="Max Limit Iterations",
            description="Maximum number of iterations during any one call to enforce transmission limits.",
            ge=0,
        ),
    ] = 1000
    max_loss_absolute_error: Annotated[
        float,
        Field(
            alias="Max Loss Absolute Error",
            description="Maximum allowed absolute error in the piecewise linear loss function.",
            ge=0,
        ),
    ] = 0.0
    max_loss_iterations: Annotated[
        int,
        Field(
            alias="Max Loss Iterations",
            description="Maximum number of iterations for converging the losses.",
            ge=1,
        ),
    ] = 50
    max_loss_relative_error: Annotated[
        float,
        Field(
            alias="Max Loss Relative Error",
            description="Maximum allowed relative error in the piecewise linear loss function.",
            ge=0,
        ),
    ] = 0.0
    max_loss_tranches: Annotated[
        int,
        Field(
            alias="Max Loss Tranches",
            description="Maximum number of tranches in piecewise linear line loss function.",
            ge=1,
        ),
    ] = 100
    mva_base: Annotated[
        float,
        Field(alias="MVA Base", description="MVA base for AC line parameters.", ge=0),
    ] = 100.0
    n_1_contingency_voltage_threshold: Annotated[
        float,
        Field(
            alias="N-1 Contingency Voltage Threshold",
            description="Voltage level at which N-1 contingencies are automatically enforced.",
            ge=0,
        ),
    ] = 0.0
    of_method: Annotated[
        Literal[0, 1],
        Field(alias="OF Method", description="Method for solving optimal power flow."),
    ] = 0
    ptdf_method: Annotated[
        Literal[0, 1, 2, 3, 4],
        Field(alias="PTDF Method", description="Method for computation of shift factors"),
    ] = 0
    reactance_cut_off: Annotated[
        float,
        Field(
            alias="Reactance Cut-off",
            description="Smallest allowable reactance when performing OPF.",
        ),
    ] = 0.0
    rental_method: Annotated[
        Literal[0, 1],
        Field(alias="Rental Method", description="Method used to calculate transmission rentals"),
    ] = 0
    report_all_interregional_flows: Annotated[
        ALLOWED_SET,
        Field(
            alias="Report All Interregional Flows",
            description="If line flows between regions should be reported regardless of kV.",
        ),
    ] = -1
    report_all_interzonal_flows: Annotated[
        ALLOWED_SET,
        Field(
            alias="Report All Interzonal Flows",
            description="If line flows between zones should be reported regardless of kV.",
        ),
    ] = -1
    report_enabled: Annotated[
        ALLOWED_SET,
        Field(alias="Report Enabled", description="If transmission reporting is enabled."),
    ] = -1
    report_injection_and_load_nodes: Annotated[
        ALLOWED_SET,
        Field(
            alias="Report Injection and Load Nodes",
            description="If all injection and load buses (nodes) are reported on (regardless of voltage)",
        ),
    ] = 0
    report_lines_in_interfaces: Annotated[
        ALLOWED_SET,
        Field(
            alias="Report Lines In Interfaces",
            description="If all flows on lines selected interfaces are reported",
        ),
    ] = 0
    report_voltage_threshold: Annotated[
        float,
        Field(
            alias="Report Voltage Threshold",
            description="Voltage level at which transmission reporting begins.",
            ge=0,
        ),
    ] = 0.0
    scuc_constraint_voltage_threshold: Annotated[
        float,
        Field(
            alias="SCUC Constraint Voltage Threshold",
            description="Voltage level at which SCUC line thermal limits are modelled.",
            ge=0,
        ),
    ] = 0.0
    scuc_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="SCUC Enabled",
            description="If security constrained unit commitment (SCUC) is enabled",
        ),
    ] = 0
    scuc_interface_constraints_enabled: Annotated[
        ALLOWED_SET,
        Field(
            alias="SCUC Interface Constraints Enabled",
            description="If enabled interface constraints are monitored in SCUC.",
        ),
    ] = 0
    use_threshold: Annotated[
        float,
        Field(
            alias="USE Threshold",
            description="Formulates the [Unserved Energy] variables on the top x% nodes (highest load).",
            ge=0,
            le=100,
        ),
    ] = 100.0
    wheeling_ptdf_threshold: Annotated[
        float,
        Field(
            alias="Wheeling PTDF Threshold",
            description="Minimum absolute value of PTDF as coefficient in wheeling definitions.",
            ge=0,
        ),
    ] = 0.05
    write_kron_reduced_model: Annotated[
        ALLOWED_SET,
        Field(
            alias="Write Kron-reduced Model",
            description="If write Kron-reduced model is enabled.",
        ),
    ] = 0
    zero_limit_treatment: Annotated[
        Literal[0, 1],
        Field(
            alias="Zero Limit Treatment",
            description="Treatment of AC elements with zero limits",
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSTransmission":
        """Create an example Transmission configuration."""
        return PLEXOSTransmission(
            name="Transmission_Example",
            of_method=1,
            constraint_voltage_threshold=0.0,
            ptdf_method=1,
            bound_node_phase_angles=0,
        )


class PLEXOSDiagnostic(PLEXOSConfiguration):
    """PLEXOS Diagnostic configuration for debugging and analysis."""

    algebraic: Annotated[
        ALLOWED_SET,
        Field(alias="Algebraic", description="Write LP files in algebraic format"),
    ] = 0
    annuities: Annotated[
        ALLOWED_SET,
        Field(alias="Annuities", description="Write diagnostics for annuity calculations"),
    ] = 0
    atc_reports: Annotated[
        ALLOWED_SET,
        Field(
            alias="ATC Reports",
            description="Write ATC min, ATC max, and most limiting elements for ATC to diagnostic files.",
        ),
    ] = 0
    bertrand_pricing: Annotated[
        ALLOWED_SET,
        Field(alias="Bertrand Pricing", description="Write diagnostics for the Bertrand pricing algorithm"),
    ] = 0
    bid_cost_mark_up: Annotated[
        ALLOWED_SET,
        Field(alias="Bid-Cost Mark-up", description="Write diagnostics for RSI bid cost markup calculations"),
    ] = 0
    binary_files: Annotated[
        ALLOWED_SET,
        Field(alias="Binary Files", description="Write math programs to disk in binary format"),
    ] = 0
    binding_contingencies: Annotated[
        ALLOWED_SET,
        Field(
            alias="Binding Contingencies", description="Write diagnostics for binding Contingency constraints"
        ),
    ] = 0
    clear_existing: Annotated[
        ALLOWED_SET,
        Field(
            alias="Clear Existing",
            description="Clear (delete) existing diagnostic files from the solution folder.",
        ),
    ] = -1
    computer_information: Annotated[
        ALLOWED_SET,
        Field(alias="Computer Information", description="Output computer information."),
    ] = -1
    congestion_charges: Annotated[
        ALLOWED_SET,
        Field(
            alias="Congestion Charges",
            description="Write diagnostics for Node.[Congestion Charge] calculations",
        ),
    ] = 0
    constraint_decomposition: Annotated[
        ALLOWED_SET,
        Field(
            alias="Constraint Decomposition",
            description="Write diagnostics for constraint decomposition in MT Schedule",
        ),
    ] = 0
    constraint_rollover: Annotated[
        ALLOWED_SET,
        Field(
            alias="Constraint Rollover",
            description="Write diagnostics for constraint RHS carry-over in ST Schedule",
        ),
    ] = 0
    data_file_read: Annotated[
        ALLOWED_SET,
        Field(alias="Data File Read", description="Output data file information to log file."),
    ] = 0
    database_load: Annotated[
        ALLOWED_SET,
        Field(alias="Database Load", description="Output the database loading process to log file."),
    ] = 0
    decimal_places_lp: Annotated[
        int,
        Field(
            alias="Decimal Places LP", description="Generic writer decimal places for LP file writing", ge=0
        ),
    ] = 6
    decimal_places_sol: Annotated[
        int,
        Field(
            alias="Decimal Places SOL", description="Generic writer decimal places for SOL file writing", ge=0
        ),
    ] = 6
    embedded_losses: Annotated[
        ALLOWED_SET,
        Field(alias="Embedded Losses", description="Write diagnostics for embedded loss iterations"),
    ] = 0
    feasibility_repair_weight: Annotated[
        ALLOWED_SET,
        Field(
            alias="Feasibility Repair Weight",
            description="Write the weights used in feasibility repair for each variable/constraint class",
        ),
    ] = 0
    future_cost_function: Annotated[
        ALLOWED_SET,
        Field(alias="Future Cost Function", description="Write diagnostics for the future cost function."),
    ] = 0
    generic_names: Annotated[
        ALLOWED_SET,
        Field(alias="Generic Names", description="Use generic names in math program files"),
    ] = 0
    heat_rate: Annotated[
        ALLOWED_SET,
        Field(
            alias="Heat Rate",
            description="Write diagnostics for Generator Heat Rate curve fitting and non-convex corrections.",
        ),
    ] = 0
    historical_sampling: Annotated[
        ALLOWED_SET,
        Field(alias="Historical Sampling", description="Write the historical samples."),
    ] = 0
    iis: Annotated[
        ALLOWED_SET,
        Field(
            alias="IIS",
            description=(
                "Compute the irreducibly inconsistent set (IIS) for each infeasibility and write to disk in text format"
            ),
        ),
    ] = 0
    infeasibility_lp_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Infeasibility LP Files",
            description="Write infeasible and repaired math programs to disk in text format",
        ),
    ] = 0
    infeasibility_mps_files: Annotated[
        ALLOWED_SET,
        Field(
            alias="Infeasibility MPS Files",
            description="Write infeasible and repaired math programs to disk in MPS format",
        ),
    ] = 0
    interleaved: Annotated[
        ALLOWED_SET,
        Field(alias="Interleaved", description="Write diagnostics for Model Interleaved run mode"),
    ] = 0
    interruption_sharing: Annotated[
        ALLOWED_SET,
        Field(alias="Interruption Sharing", description="Write diagnostics for interruption sharing"),
    ] = 0
    levelized_company_costs: Annotated[
        ALLOWED_SET,
        Field(
            alias="Levelized Company Costs",
            description="Write diagnostics for LT Plan Company Levelized Costs",
        ),
    ] = 0
    licensing: Annotated[
        ALLOWED_SET,
        Field(alias="Licensing", description="Writes details of licenses checked out"),
    ] = 0
    lp_files: Annotated[
        ALLOWED_SET,
        Field(alias="LP Files", description="Write math programs to disk in LP text format"),
    ] = 0
    lp_progress: Annotated[
        ALLOWED_SET,
        Field(alias="LP Progress", description="Show progress messages from the LP/QP solver"),
    ] = 0
    marginal_expansion_increment: Annotated[
        float,
        Field(
            alias="Marginal Expansion Increment",
            description="Increment used for load in the region marginal expansion unit diagnostic.",
        ),
    ] = 1000.0
    marginal_expansion_unit: Annotated[
        ALLOWED_SET,
        Field(
            alias="Marginal Expansion Unit",
            description="Run algorithm to calculate the marginal generating unit for expansion (LT Plan).",
        ),
    ] = 0
    marginal_loss_charges: Annotated[
        ALLOWED_SET,
        Field(
            alias="Marginal Loss Charges",
            description="Write diagnostics for Node.[Marginal Loss Charge] calculations",
        ),
    ] = 0
    marginal_unit: Annotated[
        ALLOWED_SET,
        Field(
            alias="Marginal Unit",
            description="Execute region marginal unit diagnostic (this is an active diagnostic)",
        ),
    ] = 0
    marginal_unit_increment: Annotated[
        float,
        Field(
            alias="Marginal Unit Increment",
            description="Increment used for load in the marginal unit diagnostic.",
        ),
    ] = -1.0
    marginal_unit_transmission_detail: Annotated[
        Literal[0, 1],
        Field(
            alias="Marginal Unit Transmission Detail",
            description="Transmission area for marginal unit diagnostic",
        ),
    ] = 0
    max_infeasibility_log_lines: Annotated[
        int,
        Field(
            alias="Max Infeasibility Log Lines",
            description="Maximum number of infeasibility diagnostic lines written to the screen and log file.",
            ge=-1,
        ),
    ] = -1
    mip_progress: Annotated[
        ALLOWED_SET,
        Field(alias="MIP Progress", description="Show progress messages from the MIP solver"),
    ] = 0
    monitored_iterations: Annotated[
        ALLOWED_SET,
        Field(
            alias="Monitored Iterations",
            description="Print messages for every iteration of row/column monitoring",
        ),
    ] = 0
    monitored_iterations_summary: Annotated[
        ALLOWED_SET,
        Field(
            alias="Monitored Iterations Summary",
            description="Print a summery message after iterations of row/column monitoring",
        ),
    ] = 0
    mps_files: Annotated[
        ALLOWED_SET,
        Field(alias="MPS Files", description="Write math programs to disk in MPS text format"),
    ] = 0
    network_traversal: Annotated[
        ALLOWED_SET,
        Field(
            alias="Network Traversal",
            description="Invokes network reduction algorithm which can improve performance on large-scale networks.",
        ),
    ] = 0
    new_entry: Annotated[
        ALLOWED_SET,
        Field(alias="New Entry", description="Write diagnostics for MT Schedule new entry calculations"),
    ] = 0
    npv: Annotated[
        ALLOWED_SET,
        Field(alias="NPV", description="Write diagnostics for LT Plan NPV of optimal plan"),
    ] = 0
    objective_function: Annotated[
        ALLOWED_SET,
        Field(
            alias="Objective Function",
            description="Write diagnostics for non-zero terms in the objective function.",
        ),
    ] = 0
    outages: Annotated[
        ALLOWED_SET,
        Field(alias="Outages", description="Write diagnostics for generator and line outages"),
    ] = 0
    performance_summary: Annotated[
        ALLOWED_SET,
        Field(
            alias="Performance Summary",
            description="Print performance summary at the completion of each simulation phase.",
        ),
    ] = -1
    random_number_seed: Annotated[
        ALLOWED_SET,
        Field(
            alias="Random Number Seed",
            description="Write out the Random Number Seed used for each Generator and Line",
        ),
    ] = 0
    region_supply: Annotated[
        ALLOWED_SET,
        Field(
            alias="Region Supply",
            description="Execute region supply diagnostic (this is an active diagnostic)",
        ),
    ] = 0
    revenue_recovery: Annotated[
        ALLOWED_SET,
        Field(alias="Revenue Recovery", description="Write diagnostics for the LRMC recovery algorithm"),
    ] = 0
    sample_from: Annotated[
        int,
        Field(
            alias="Sample From",
            description="Limit diagnostic file writing to sample numbers starting at this number.",
            ge=1,
        ),
    ] = 1
    sample_reduction: Annotated[
        ALLOWED_SET,
        Field(
            alias="Sample Reduction",
            description="Write diagnostics for reduced sample periods and periods mapping by sampled chronology.",
        ),
    ] = 0
    sample_summary: Annotated[
        ALLOWED_SET,
        Field(alias="Sample Summary", description="Print summary for each sample of a multi-sample run."),
    ] = 0
    sample_to: Annotated[
        int,
        Field(
            alias="Sample To",
            description="Limit diagnostic file writing to sample numbers ending at this number (-1 means infinity).",
            ge=-1,
        ),
    ] = -1
    sample_weights: Annotated[
        ALLOWED_SET,
        Field(alias="Sample Weights", description="Write diagnostics for the sample weights"),
    ] = 0
    scenario_tree: Annotated[
        ALLOWED_SET,
        Field(alias="Scenario Tree", description="Write diagnostics for the scenario tree"),
    ] = 0
    sddp_convergence: Annotated[
        ALLOWED_SET,
        Field(alias="SDDP Convergence", description="Write SDDP convergence diagnostic file"),
    ] = 0
    shift_factors: Annotated[
        ALLOWED_SET,
        Field(
            alias="Shift Factors", description="Write the computed shift factors (PTDF) to a diagnostic file"
        ),
    ] = 0
    skip_zero_values: Annotated[
        ALLOWED_SET,
        Field(
            alias="Skip Zero Values",
            description="Generic writer will print only non-zero valued primal and dual variables in SOL files",
        ),
    ] = 0
    solution_files: Annotated[
        ALLOWED_SET,
        Field(alias="Solution Files", description="Write math program solutions to disk in text format"),
    ] = 0
    solution_status: Annotated[
        ALLOWED_SET,
        Field(
            alias="Solution Status",
            description="Status of the solution to each mathematical programming problem.",
        ),
    ] = 0
    solver_summary: Annotated[
        ALLOWED_SET,
        Field(
            alias="Solver Summary",
            description="Print a summary of the solution status, objective function value, etc for each mathematical program solved.",
        ),
    ] = 0
    sort_row_column_names: Annotated[
        ALLOWED_SET,
        Field(alias="Sort Row Column Names", description="Generic writer will sort row and columns names"),
    ] = 0
    standardize_names: Annotated[
        ALLOWED_SET,
        Field(
            alias="Standardize Names", description="Generic writer will standardize names in LP and SOL files"
        ),
    ] = 0
    step_from: Annotated[
        int,
        Field(
            alias="Step From",
            description="Limit diagnostic file writing to step numbers starting at this number.",
            ge=1,
        ),
    ] = 1
    step_summary: Annotated[
        ALLOWED_SET,
        Field(alias="Step Summary", description="Print summary for each step of a multi-step run."),
    ] = 0
    step_to: Annotated[
        int,
        Field(
            alias="Step To",
            description="Limit diagnostic file writing to step numbers ending at this number (-1 means infinity).",
            ge=-1,
        ),
    ] = -1
    storage_decomposition: Annotated[
        ALLOWED_SET,
        Field(alias="Storage Decomposition", description="Write diagnostics for storage decomposition"),
    ] = 0
    strip_model_name: Annotated[
        ALLOWED_SET,
        Field(
            alias="Strip Model Name",
            description="Generic writer will strip out the Model name from LP and SOL files",
        ),
    ] = 0
    summary_exact_conditioning: Annotated[
        ALLOWED_SET,
        Field(
            alias="Summary Exact Conditioning",
            description="Report exact basis condition number (if available) in the solver summary.",
        ),
    ] = 0
    task_components: Annotated[
        ALLOWED_SET,
        Field(alias="Task Components", description="Print a summary of the formulation elements by class."),
    ] = 0
    task_size: Annotated[
        ALLOWED_SET,
        Field(alias="Task Size", description="Print the size of the optimization task."),
    ] = 0
    times: Annotated[
        ALLOWED_SET,
        Field(alias="Times", description="Print the time taken for each activity."),
    ] = -1
    transmission_losses: Annotated[
        ALLOWED_SET,
        Field(
            alias="Transmission Losses",
            description="Write diagnostics for convergence of the quadratic loss method",
        ),
    ] = 0
    transmission_topology: Annotated[
        ALLOWED_SET,
        Field(
            alias="Transmission Topology",
            description="Write a summary of the transmission topology to the log",
        ),
    ] = 0
    uniform_pricing: Annotated[
        ALLOWED_SET,
        Field(alias="Uniform Pricing", description="Write diagnostics for uplift and Uniform Pricing"),
    ] = 0
    unit_commitment: Annotated[
        ALLOWED_SET,
        Field(
            alias="Unit Commitment",
            description="Write diagnostics for the Rounded Relaxation unit commitment algorithm",
        ),
    ] = 0
    unserved_energy: Annotated[
        ALLOWED_SET,
        Field(alias="Unserved Energy", description="Write diagnostics each time USE occurs at a node"),
    ] = 0
    use_generic_writer: Annotated[
        ALLOWED_SET,
        Field(alias="Use Generic Writer", description="Use generic writer for LP and solution files"),
    ] = 0
    zero_tolerance_lp: Annotated[
        float,
        Field(
            alias="Zero Tolerance LP",
            description="Generic writer zero value tolerance for LP file writing",
            ge=0,
        ),
    ] = 0.0
    zero_tolerance_sol: Annotated[
        float,
        Field(
            alias="Zero Tolerance SOL",
            description="Generic writer zero value tolerance for SOL file writing",
            ge=0,
        ),
    ] = 0.0

    @classmethod
    def example(cls) -> "PLEXOSDiagnostic":
        """Create an example Diagnostic configuration."""
        return PLEXOSDiagnostic(name="Diagnostic_Example")
