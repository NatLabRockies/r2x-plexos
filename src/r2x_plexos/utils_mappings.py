"""MAPPING FOR CLASS ENUM."""

from plexosdb import ClassEnum, CollectionEnum

from .models import (
    PLEXOSBattery,
    PLEXOSDatafile,
    PLEXOSGenerator,
    PLEXOSHorizon,
    PLEXOSInterface,
    PLEXOSLine,
    PLEXOSModel,
    PLEXOSNode,
    PLEXOSObject,
    PLEXOSRegion,
    PLEXOSReserve,
    PLEXOSScenario,
    PLEXOSStorage,
    PLEXOSTimeslice,
    PLEXOSTransformer,
    PLEXOSVariable,
    PLEXOSZone,
)

PLEXOS_TYPE_MAP: dict[ClassEnum, type[PLEXOSObject]] = {
    ClassEnum.Generator: PLEXOSGenerator,
    ClassEnum.Node: PLEXOSNode,
    ClassEnum.Storage: PLEXOSStorage,
    ClassEnum.Line: PLEXOSLine,
    ClassEnum.DataFile: PLEXOSDatafile,
    ClassEnum.Variable: PLEXOSVariable,
    ClassEnum.Scenario: PLEXOSScenario,
    ClassEnum.Battery: PLEXOSBattery,
    ClassEnum.Reserve: PLEXOSReserve,
    ClassEnum.Region: PLEXOSRegion,
    ClassEnum.Zone: PLEXOSZone,
    ClassEnum.Interface: PLEXOSInterface,
    ClassEnum.Timeslice: PLEXOSTimeslice,
    ClassEnum.Transformer: PLEXOSTransformer,
    ClassEnum.Model: PLEXOSModel,
    ClassEnum.Horizon: PLEXOSHorizon,
}
PLEXOS_TYPE_MAP_INVERTED = dict(zip(PLEXOS_TYPE_MAP.values(), PLEXOS_TYPE_MAP.keys(), strict=False))

MEMBERSHIP_TYPE_MAP = {
    "Horizon": (ClassEnum.Horizon, CollectionEnum.Horizon),
    "Diagnostic": (ClassEnum.Diagnostic, CollectionEnum.Diagnostic),
    "MT Schedule": (ClassEnum.MTSchedule, CollectionEnum.MTSchedule),
    "ST Schedule": (ClassEnum.STSchedule, CollectionEnum.STSchedule),
    "Production": (ClassEnum.Production, CollectionEnum.Production),
    "PASA": (ClassEnum.PASA, CollectionEnum.PASA),
    "Performance": (ClassEnum.Performance, CollectionEnum.Performance),
    "Report": (ClassEnum.Report, CollectionEnum.Report),
    "Transmission": (ClassEnum.Transmission, CollectionEnum.Transmission),
}

CONFIG_CLASS_MAP = {
    "mt_schedule": ClassEnum.MTSchedule,
    "st_schedule": ClassEnum.STSchedule,
    "production": ClassEnum.Production,
    "pasa": ClassEnum.PASA,
    "performance": ClassEnum.Performance,
    "report": ClassEnum.Report,
    "transmission": ClassEnum.Transmission,
    "diagnostic": ClassEnum.Diagnostic,
}

GENERATOR_TS_PROPERTY_MAP: dict[str, str] = {
    "max_active_power": "Rating",
    "load": "Load",
    "fixed_load": "Rating",
    "hydro_budget": "Max Energy Day"
}

GENERATOR_TO_STORAGE_TS_PROPERTY_MAP: dict[str, str] = {
    "natural_inflow": "Natural Inflow",
    "inflow": "Natural Inflow",
}

FIXED_TS_PROP: dict[type, str] = {
    PLEXOSReserve: "Min Provision",
    PLEXOSRegion: "Load",
    PLEXOSStorage: "Natural Inflow",
}
