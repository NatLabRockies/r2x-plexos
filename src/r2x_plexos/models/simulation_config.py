"""Pydantic models for PLEXOS simulation configuration objects."""

from typing import Annotated, Literal

from pydantic import Field

from .component import PLEXOSConfiguration

# PLEXOS Yes/No attributes: Yes (-1), No (0)
ALLOWED_SET = Literal[0, -1]


class PLEXOSMTSchedule(PLEXOSConfiguration):
    """PLEXOS MT Schedule (Medium-Term Schedule) configuration."""

    step_type: Annotated[
        int,
        Field(
            alias="Step Type",
            description="Step type: Day (1), Week (2), Month (3), Year (4)",
            json_schema_extra={"enum": [1, 2, 3, 4]},
        ),
    ] = 4
    at_a_time: Annotated[
        int,
        Field(
            alias="At a Time",
            description="Number of steps to process at a time",
            ge=1,
        ),
    ] = 1
    ldc_type: Annotated[
        int,
        Field(
            alias="LDC Type",
            description="Load duration curve type",
            ge=0,
        ),
    ] = 3
    block_count: Annotated[
        int,
        Field(
            alias="Block Count",
            description="Number of blocks for load duration curve",
            ge=1,
        ),
    ] = 6
    ldc_slicing_method: Annotated[
        int,
        Field(
            alias="LDC Slicing Method",
            description="Method for slicing load duration curve",
            ge=0,
        ),
    ] = 0
    heat_rate_detail: Annotated[
        int,
        Field(
            alias="Heat Rate Detail",
            description="Level of heat rate detail (2 = simplest)",
            ge=0,
        ),
    ] = 2
    transmission_detail: Annotated[
        int,
        Field(
            alias="Transmission Detail",
            description="Transmission modeling detail: Regional (0), Zonal (1), Nodal (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 0

    @classmethod
    def example(cls) -> "PLEXOSMTSchedule":
        """Create an example MT Schedule configuration."""
        return PLEXOSMTSchedule(
            name="MT_Schedule_Example",
            step_type=4,
            at_a_time=1,
            ldc_type=3,
            block_count=6,
            ldc_slicing_method=0,
            heat_rate_detail=2,
            transmission_detail=0,
        )


class PLEXOSSTSchedule(PLEXOSConfiguration):
    """PLEXOS ST Schedule (Short-Term Schedule) configuration."""

    transmission_detail: Annotated[
        int,
        Field(
            alias="Transmission Detail",
            description="Transmission modeling detail: Regional (0), Zonal (1), Nodal (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 1
    stochastic_method: Annotated[
        int,
        Field(
            alias="Stochastic Method",
            description="Stochastic optimization method",
            ge=0,
        ),
    ] = 0
    heat_rate_detail: Annotated[
        int,
        Field(
            alias="Heat Rate Detail",
            description="Level of heat rate detail",
            ge=0,
        ),
    ] = 2

    @classmethod
    def example(cls) -> "PLEXOSSTSchedule":
        """Create an example ST Schedule configuration."""
        return PLEXOSSTSchedule(
            name="ST_Schedule_Example",
            transmission_detail=1,
            stochastic_method=0,
            heat_rate_detail=2,
        )


class PLEXOSProduction(PLEXOSConfiguration):
    """PLEXOS Production simulation configuration."""

    unit_commitment_optimality: Annotated[
        int,
        Field(
            alias="Unit Commitment Optimality",
            description="Unit commitment method: LP (0), Rounded Relaxation (1), MIP (2)",
            json_schema_extra={"enum": [0, 1, 2]},
        ),
    ] = 2
    rounding_up_threshold: Annotated[
        float,
        Field(
            alias="Rounding Up Threshold",
            description="Threshold for rounding up unit commitment decisions",
            ge=0.0,
            le=1.0,
        ),
    ] = 0.26
    max_heat_rate_tranches: Annotated[
        int,
        Field(
            alias="Max Heat Rate Tranches",
            description="Maximum number of heat rate tranches",
            ge=1,
        ),
    ] = 1

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

    step_type: Annotated[
        int,
        Field(
            alias="Step Type",
            description="Step type: Day (1), Week (2), Month (3), Year (4)",
            json_schema_extra={"enum": [1, 2, 3, 4]},
        ),
    ] = 1
    write_outage_text_files: Annotated[
        int,
        Field(
            alias="Write Outage Text Files",
            description="Write outage text files: Yes (-1), No (0)",
            json_schema_extra={"enum": [-1, 0]},
        ),
    ] = -1

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

    solver: Annotated[
        int,
        Field(
            alias="SOLVER",
            description="Solver selection: CPLEX (1), Xpress (2), CBC (3), Gurobi (4), MOSEK (5), HiGHS (6)",
            json_schema_extra={"enum": [1, 2, 3, 4, 5, 6]},
        ),
    ] = 4
    mip_relative_gap: Annotated[
        float,
        Field(
            alias="MIP Relative Gap",
            description="MIP solver relative optimality gap tolerance",
            ge=0.0,
        ),
    ] = 0.01
    mip_maximum_threads: Annotated[
        int,
        Field(
            alias="MIP Maximum Threads",
            description="Maximum number of threads for MIP solver",
            ge=1,
        ),
    ] = 20
    mip_max_time: Annotated[
        int,
        Field(
            alias="MIP Max Time",
            description="Maximum time in seconds for MIP solver",
            ge=0,
        ),
    ] = 6500

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
    opf_method: Annotated[
        Literal[0, 1],
        Field(alias="OPF Method", description="Method for solving optimal power flow."),
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
            opf_method=1,
            constraint_voltage_threshold=0.0,
            ptdf_method=1,
            bound_node_phase_angles=0,
        )


class PLEXOSDiagnostic(PLEXOSConfiguration):
    """PLEXOS Diagnostic configuration for debugging and analysis.

    This class serves as a container for diagnostic settings.
    Specific attributes can be added as needed.
    """

    @classmethod
    def example(cls) -> "PLEXOSDiagnostic":
        """Create an example Diagnostic configuration."""
        return PLEXOSDiagnostic(name="Diagnostic_Example")
