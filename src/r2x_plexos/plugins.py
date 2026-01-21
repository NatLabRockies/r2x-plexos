"""Plugin exports for r2x-plexos package.

This module provides direct class exports for the r2x-plexos plugin system.
The new r2x-core 0.2.x pattern uses direct class exports instead of PluginManifest.
"""

from r2x_plexos import PLEXOSConfig, PLEXOSParser
from r2x_plexos.exporter import PLEXOSExporter

# Main plugins - direct class references for entry points
parser = PLEXOSParser
exporter = PLEXOSExporter
config = PLEXOSConfig

__all__ = [
    # Classes
    "PLEXOSConfig",
    "PLEXOSExporter",
    "PLEXOSParser",
    # Plugin references
    "config",
    "exporter",
    "parser",
]
