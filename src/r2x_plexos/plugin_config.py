"""PLEXOS configuration class."""

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

from pydantic import DirectoryPath, Field, FilePath

from r2x_core.plugin_config import PluginConfig
from r2x_plexos.utils_simulation import SimulationConfig


class PLEXOSConfig(PluginConfig):
    """Configuration for PLEXOS model parser.

    This configuration class defines all parameters needed to parse
    PLEXOS model data, including model identification, time series handling,
    and simulation settings. Model-specific defaults and constants should be
    loaded using the `load_defaults()` class method and used in parser logic.

    Parameters
    ----------
    model_name : str
        Name of the PLEXOS model
    timeseries_dir : DirectoryPath, optional
        Optional subdirectory containing time series files. If passed it must exist.
    horizon_year : int, optional
        Horizon year for the model simulation
    template : FilePath, optional
        File path to the XML to use as template. If passed it must exist.
    simulation_config : SimulationConfig, optional
        Simulation configuration parameters

    Examples
    --------
    Basic configuration with model name:

    >>> config = PLEXOSConfig(
    ...     model_name="MyPLEXOSModel",
    ...     horizon_year=2030,
    ... )

    Full configuration with time series and simulation:

    >>> config = PLEXOSConfig(
    ...     model_name="MyPLEXOSModel",
    ...     timeseries_dir=Path("./timeseries"),
    ...     horizon_year=2030,
    ...     template=Path("./template.xml"),
    ...     simulation_config=SimulationConfig(...),
    ... )

    See Also
    --------
    r2x_core.plugin_config.PluginConfig : Base configuration class
    r2x_plexos.utils_simulation.SimulationConfig : Simulation configuration class
    load_defaults : Class method to load default constants from JSON
    """

    model_name: Annotated[str, Field(description="Name of the PLEXOS model.")]
    timeseries_dir: Annotated[
        DirectoryPath | None,
        Field(
            description="Optional subdirectory containing time series files. If passed it must exist.",
            default=None,
        ),
    ]
    horizon_year: Annotated[int | None, Field(description="Horizon year", default=None)]
    template: Annotated[
        FilePath | None, Field(description="File to the XML to use as template. If passed it must exist.")
    ] = None
    simulation_config: Annotated[SimulationConfig | None, Field(description="Simulation configuration")] = (
        None
    )

    @classmethod
    def get_config_path(cls) -> Path:
        """Return the plugin's configuration directory path."""
        resolve_method: Callable[[Any], Path] | None = getattr(cls, "_resolve_config_path", None)
        if resolve_method:
            return resolve_method(None)
        return cls._package_config_path()
