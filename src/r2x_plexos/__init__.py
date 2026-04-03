"""R2X PLEXOS Plugin.

A plugin for parsing PLEXOS model data into the R2X framework using infrasys components.
"""

from importlib.metadata import version

from loguru import logger

from .exporter import PLEXOSExporter
from .models import (
    PLEXOSObject,
    PLEXOSProperty,
    PLEXOSPropertyValue,
    PLEXOSRow,
    get_horizon,
    get_scenario_priority,
    horizon,
    scenario_and_horizon,
    scenario_priority,
    set_horizon,
    set_scenario_priority,
)
from .parser import PLEXOSParser
from .plugin_config import PLEXOSConfig

__version__ = version("r2x_plexos")


# Disable default loguru handler for library usage
# Applications using this library should configure their own handlers
logger.disable("r2x_plexos")


__all__ = [
    "PLEXOSConfig",
    "PLEXOSExporter",
    "PLEXOSObject",
    "PLEXOSParser",
    "PLEXOSProperty",
    "PLEXOSPropertyValue",
    "PLEXOSRow",
    "__version__",
    "get_horizon",
    "get_scenario_priority",
    "horizon",
    "scenario_and_horizon",
    "scenario_priority",
    "set_horizon",
    "set_scenario_priority",
]
