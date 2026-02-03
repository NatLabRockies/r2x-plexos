"""Export PLEXOS system to XML."""

import os
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import get_default_collection

from r2x_core import Err, Ok, Plugin, Result

from .models import (
    PLEXOSBattery,
    PLEXOSDatafile,
    PLEXOSGenerator,
    PLEXOSHorizon,
    PLEXOSInterface,
    PLEXOSLine,
    PLEXOSMembership,
    PLEXOSModel,
    PLEXOSNode,
    PLEXOSObject,
    PLEXOSRegion,
    PLEXOSReserve,
    PLEXOSStorage,
)
from .models.property import PLEXOSPropertyValue
from .plugin_config import PLEXOSConfig
from .utils_exporter import (
    export_time_series_csv,
    generate_csv_filename,
    get_output_directory,
)
from .utils_mappings import PLEXOS_TYPE_MAP_INVERTED
from .utils_simulation import (
    build_plexos_simulation,
    get_default_simulation_config,
    ingest_simulation_to_plexosdb,
)

NESTED_ATTRIBUTES = {"ext", "bus", "services"}
DEFAULT_XML_TEMPLATE = "master_9.2R6_btu.xml"

REQUIRED_PROPERTIES = {
    PLEXOSGenerator: {
        "units",
        "forced_outage_rate",
        "min_stable_level",
        "maintenance_rate",
        "mean_time_to_repair",
    },
    PLEXOSStorage: {"units", "initial_volume", "max_capacity"},
    PLEXOSRegion: {"units"},
    PLEXOSBattery: {
        "units",
        "initial_soc",
        "min_soc",
        "max_soc",
        "charge_efficiency",
        "discharge_efficiency",
    },
    PLEXOSLine: {"units"},
    PLEXOSNode: {"units"},
    PLEXOSInterface: {"units"},
}


class PLEXOSExporter(Plugin[PLEXOSConfig]):
    """PLEXOS XML exporter."""

    def __init__(self) -> None:
        """Initialize the exporter with minimal state.

        Notes
        -----
        Actual initialization happens after context is set via from_context().
        """
        super().__init__()

        self.should_export_time_series: bool = True
        self.exclude_defaults: bool = True
        self.output_path: str | None = None
        self.solve_year: int | None = None
        self.weather_year: int | None = None
        self.plexos_scenario: str = "default"
        self.db: PlexosDB | None = None

    def on_export(self) -> Result[None, str]:
        """Initialize the exporter after context is set.

        Returns
        -------
        Result[None, str]
            Ok(None) on success, Err with error message on failure
        """
        try:
            logger.debug("Starting {} using configuration {}", type(self).__name__, self.config)

            if not isinstance(self.config, PLEXOSConfig):
                msg = (
                    f"Config is of type {type(self.config)}. "
                    f"It should be type of `{type(PLEXOSConfig).__name__}`."
                )
                return Err(msg)

            if self.plexos_scenario == "default":
                self.plexos_scenario = self.config.model_name

            if self.db is None:
                xml_fname = self.config.template
                if not xml_fname:
                    xml_fname = self.config.get_config_path().joinpath(DEFAULT_XML_TEMPLATE)
                    logger.debug("Using default XML template")

                self.db = PlexosDB.from_xml(xml_path=xml_fname)

            if not self.db.check_object_exists(ClassEnum.Scenario, self.plexos_scenario):
                self.db.add_scenario(self.plexos_scenario)

            setup_result = self.setup_configuration()
            if setup_result.is_err():
                return setup_result

            prepare_result = self.prepare_export()
            if prepare_result.is_err():
                return prepare_result

            postprocess_result = self.postprocess_export()
            if postprocess_result.is_err():
                return postprocess_result

            return Ok(None)
        except Exception as e:
            return Err(f"Export failed: {e}")

    def setup_configuration(self) -> Result[None, str]:
        """Set up simulation configuration (models, horizons, and simulation configs).

        This method supports two workflows:

        1. **Existing Database Workflow**: If the database already contains models and horizons
           (e.g., loaded from an existing XML template), the simulation configuration is skipped.
           This allows users to work with pre-configured databases without modification.

        2. **New Database Workflow**: If the database is new (no models or horizons exist),
           this method creates the complete simulation structure from user configuration:
           - Models and horizons based on horizon_year and resolution
           - Model-horizon memberships
           - Simulation configuration objects (Performance, Production, etc.)

        Returns
        -------
        Result[None, str]
            Ok(None) if successful, Err with error message if failed
        """
        if self.db is None:
            return Err("Database not initialized")

        logger.info("Setting up simulation configuration")

        existing_models = self.db.list_objects_by_class(ClassEnum.Model)
        existing_horizons = self.db.list_objects_by_class(ClassEnum.Horizon)

        if existing_models and existing_horizons:
            logger.info(
                f"Using existing database configuration: "
                f"{len(existing_models)} model(s), {len(existing_horizons)} horizon(s)"
            )
            return Ok(None)

        logger.info("New database detected - creating simulation configuration from user input")
        simulation_config_dict = getattr(self.config, "simulation_config", None)
        if simulation_config_dict is None:
            logger.debug("Using default simulation configuration")
            simulation_config_dict = get_default_simulation_config()

        horizon_year = getattr(self.config, "horizon_year", None) or getattr(
            self.config, "reference_year", None
        )
        if horizon_year is None:
            return Err(
                "New database requires 'horizon_year' (or 'reference_year') in config "
                "to create simulation configuration"
            )

        sim_config = {
            "horizon_year": horizon_year,
            "resolution": getattr(self.config, "resolution", "1D"),
        }

        logger.info(f"Building simulation for year {horizon_year}")

        simulation_result = build_plexos_simulation(
            config=sim_config,
            defaults=None,
            simulation_config=simulation_config_dict,
        )

        if simulation_result.is_err():
            assert isinstance(simulation_result, Err)
            return Err(f"Failed to build simulation: {simulation_result.error}")

        build_result = simulation_result.unwrap()
        logger.info(
            f"Built simulation: {len(build_result.models)} model(s), "
            f"{len(build_result.horizons)} horizon(s), "
            f"{len(build_result.memberships)} membership(s)"
        )

        ingest_result = ingest_simulation_to_plexosdb(self.db, build_result, validate=False)
        if ingest_result.is_err():
            assert isinstance(ingest_result, Err)
            return Err(f"Failed to ingest simulation: {ingest_result.error}")

        ingest_info = ingest_result.unwrap()
        sim_config_count = len(ingest_info.get("simulation_objects", []))
        logger.info(
            f"Successfully created simulation configuration: "
            f"{len(ingest_info['models'])} model(s), "
            f"{len(ingest_info['horizons'])} horizon(s), "
            f"{sim_config_count} simulation config object(s)"
        )

        return Ok(None)

    def prepare_export(self) -> Result[None, str]:
        """Add component objects to the database.

        This method bulk inserts component objects (generators, nodes, etc.) into the database.
        It skips simulation configuration objects (Model, Horizon) as those are handled in setup_configuration().
        It does NOT add properties or memberships - those are added in postprocess_export().
        """
        from itertools import groupby

        if self.db is None:
            return Err("Database not initialized")

        logger.info("Adding components to database")

        # Skip these types - they're either config objects or don't get added as objects
        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                logger.debug(f"Skipping component type: {component_type.__name__}")
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                logger.warning("No ClassEnum mapping for {}, skipping.", type(component_type).__name__)
                continue

            components = list(self.system.get_components(component_type))
            if not components:
                logger.debug(f"No components found for type: {component_type.__name__}, skipping.")
                continue

            logger.debug(f"Adding {len(components)} {component_type.__name__} components")

            # Sort components by category to group them
            components.sort(key=lambda x: x.category or "")  # type: ignore

            # Group components by category and add each group in one call
            for category, group in groupby(components, key=lambda x: x.category or ""):  # type: ignore
                names = [comp.name for comp in group]
                try:
                    if category:
                        self.db.add_objects(class_enum, *names, category=category)
                    else:
                        self.db.add_objects(class_enum, *names)
                except KeyError as e:
                    logger.error(f"Failed to add {class_enum} objects with category '{category}': {e}")
                    logger.debug(f"Component type: {component_type.__name__}, names: {names[:5]}")
                    raise

            self.db._db.execute(f"UPDATE t_class SET is_enabled=1 WHERE t_class.name='{class_enum}'")
            logger.debug(f"Enabled class: {class_enum.name}")

        return Ok(None)

    def postprocess_export(self) -> Result[None, str]:
        """Add properties and memberships to the database.

        This method:
        1. Adds component properties using bulk insert from system.to_records()
        2. Adds component memberships (relationships between components)
        3. Exports time series to CSV files

        Components without properties (PLEXOSDatafile, PLEXOSMembership) are filtered out.
        """
        if self.db is None:
            return Err("Database not initialized")

        logger.info("Exporting time series")
        if self.should_export_time_series:
            ts_result = self.export_time_series()
            if isinstance(ts_result, Err):
                logger.error("Failed to export time series: {}", ts_result.error)
                return ts_result

        logger.info("Creating DataFile objects from exported CSVs")
        self._add_component_datafile_objects()

        logger.info("Adding component properties and memberships")
        self._add_component_properties()
        self._add_component_memberships()

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        base_folder = Path(self.output_path) if self.output_path else output_dir.parent
        xml_filename = f"{self.config.model_name}.xml"
        xml_path = base_folder / xml_filename

        logger.info(f"Exporting XML to {xml_path}")
        self.db.to_xml(xml_path)

        if not self._validate_xml(str(xml_path)):
            logger.error(f"Exported XML at {xml_path} is not valid!")
            return Err(f"Exported XML at {xml_path} is not valid!")
        else:
            logger.success("Exported XML was correctly validated.")

        return Ok(None)

    def _add_component_properties(self) -> None:
        """Add properties for all components, including DataFile objects first."""
        if self.db is None:
            logger.error("Database not initialized")
            return

        logger.info("Adding component properties...")

        for component in self.system.get_components(PLEXOSDatafile):
            relative_path = f"Data/{component.name}.csv"

            self.db.add_property(
                ClassEnum.DataFile,
                object_name=component.name,
                name="Filename",
                value=0,
                datafile_text=relative_path,
            )
            logger.debug(f"Added Filename property for DataFile: {component.name} -> {relative_path}")

        skip_types = {PLEXOSModel, PLEXOSHorizon, PLEXOSDatafile, PLEXOSMembership}

        for component_type in self.system.get_component_types():
            if component_type in skip_types:
                continue

            class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], component_type))
            if not class_enum:
                continue

            collection = get_default_collection(class_enum)
            plexos_records = []

            required_for_type = REQUIRED_PROPERTIES.get(component_type, set())

            for comp in self.system.get_components(component_type):
                aliased_dict = comp.model_dump(by_alias=True, exclude_defaults=self.exclude_defaults)

                if self.exclude_defaults and required_for_type:
                    for prop_name in required_for_type:
                        field = comp.__class__.model_fields.get(prop_name)
                        if field:
                            alias_name = getattr(field, "alias", prop_name)
                            if alias_name not in aliased_dict:
                                value = getattr(comp, prop_name, None)
                                if value is not None:
                                    aliased_dict[alias_name] = value

                metadata_fields = {"name", "category", "uuid", "label", "description", "object_id"}
                properties: dict[str, Any] = {}

                for k, v in aliased_dict.items():
                    if k in metadata_fields or v is None:
                        continue
                    if isinstance(v, (int, float, str, bool)):
                        properties[k] = {"value": v, "band": 1}
                    elif isinstance(v, dict) and "text" in v:
                        properties[k] = v

                if properties:
                    plexos_record = {"name": comp.name, "properties": properties}
                    plexos_records.append(plexos_record)

            if not plexos_records:
                continue

            logger.debug(f"Adding properties for {len(plexos_records)} {component_type.__name__} components")
            self.db.add_properties_from_records(
                plexos_records,
                object_class=class_enum,
                parent_class=ClassEnum.System,
                collection=collection,
                scenario=self.plexos_scenario,
            )

    def _add_component_memberships(self) -> None:
        """Add membership relationships to the database."""
        if self.db is None:
            logger.error("Database not initialized")
            return

        memberships = list(self.system.get_supplemental_attributes(PLEXOSMembership))

        if not memberships:
            logger.warning("No memberships found in system")
            return

        records = []
        seen_memberships = set()  # Track unique (parent_object_id, collection_id, child_object_id)
        duplicate_count = 0

        for membership in memberships:
            if not membership.parent_object or not membership.child_object:
                logger.debug("Skipping membership with missing parent or child object")
                continue

            parent_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.parent_object))
            child_class = PLEXOS_TYPE_MAP_INVERTED.get(type(membership.child_object))

            if not parent_class or not child_class or not membership.collection:
                logger.info("Skipping membership with unmapped classes or missing collection")
                continue

            if parent_class in (ClassEnum.Model, ClassEnum.Horizon) or child_class in (
                ClassEnum.Model,
                ClassEnum.Horizon,
            ):
                continue

            try:
                parent_object_id = self.db.get_object_id(parent_class, membership.parent_object.name)
                child_object_id = self.db.get_object_id(child_class, membership.child_object.name)
                collection_id = self.db.get_collection_id(
                    membership.collection,
                    parent_class_enum=parent_class,
                    child_class_enum=child_class,
                )

                # Check for duplicates based on the unique constraint
                membership_key = (parent_object_id, collection_id, child_object_id)

                if membership_key in seen_memberships:
                    duplicate_count += 1
                    continue

                seen_memberships.add(membership_key)

                record = {
                    "parent_class_id": self.db.get_class_id(parent_class),
                    "parent_object_id": parent_object_id,
                    "collection_id": collection_id,
                    "child_class_id": self.db.get_class_id(child_class),
                    "child_object_id": child_object_id,
                }
                records.append(record)

            except Exception:
                logger.debug("Failed to process membership: {}", membership)
                continue

        if not records:
            logger.warning("No valid membership records to add")
            return

        self.db.add_memberships_from_records(records)
        logger.success(f"Successfully added {len(records)} memberships")

    def _add_component_datafile_objects(self) -> None:
        """Add PLEXOSDatafile objects from the system to the database."""
        if self.db is None:
            logger.error("Database not initialized")
            return

        self._create_datafile_objects()
        datafiles = list(self.system.get_components(PLEXOSDatafile))
        if not datafiles:
            logger.info("No PLEXOSDatafile objects to add to DB.")
            return

        logger.debug(f"Adding {len(datafiles)} PLEXOSDatafile objects to DB.")

        names = [df.name for df in datafiles]
        self.db.add_objects(ClassEnum.DataFile, *names, category="CSV")

        for data_file in datafiles:
            object_id = self.db.get_object_id(ClassEnum.DataFile, data_file.name)
            data_file.object_id = object_id
            logger.debug(f"Set object_id={object_id} for DataFile: {data_file.name}")

        self._link_datafiles_to_components()

    def _link_datafiles_to_components(self) -> None:
        """Link DataFile objects to component properties that reference time series CSVs.

        This method finds components with time series, matches them to the exported CSV files,
        and updates the component's property to reference the DataFile object via datafile_text
        and t_tag table entries.
        """
        import re

        if self.db is None:
            logger.error("Database not initialized")
            return

        logger.info("Linking DataFiles to component properties...")

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)

        for component_type in self.system.get_component_types():
            components = list(self.system.get_components(component_type))
            components_with_ts = [c for c in components if self.system.has_time_series(c)]

            for component in components_with_ts:
                ts_keys = self.system.list_time_series_keys(component)

                category = getattr(component, "category", "").lower()

                if "hydro" in category:
                    ts_keys = [key for key in ts_keys if key.name == "max_active_power"]

                if "head" in category or "tail" in category:
                    ts_keys = [key for key in ts_keys if key.name in ("natural_inflow", "inflow")]

                if any(
                    x in category for x in ("renewable-dispatch", "renewable-non-dispatch", "solar", "wind")
                ):
                    ts_keys = [key for key in ts_keys if key.name in ("max_active_power", "active_power")]

                for ts_key in ts_keys:
                    property_name = self._get_time_series_property_name(component)
                    if not property_name:
                        logger.debug(f"No property mapping for {type(component).__name__}.{ts_key.name}")
                        continue

                    component_class = type(component).__name__
                    pattern = re.compile(rf"{re.escape(component_class)}_{re.escape(ts_key.name)}_.*\.csv")

                    matched_file = None
                    for filename in os.listdir(output_dir):
                        if pattern.match(filename):
                            matched_file = filename
                            break

                    if not matched_file:
                        logger.warning(f"No CSV file found for {component.name}.{ts_key.name}")
                        continue

                    datafile_name = matched_file.removesuffix(".csv")
                    datafile = self.system.get_component(PLEXOSDatafile, name=datafile_name)

                    if not datafile or datafile.object_id is None:
                        logger.warning(f"No DataFile object found for {matched_file}")
                        continue

                    class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], type(component)))
                    if not class_enum:
                        continue

                    try:
                        self.db.add_property(
                            class_enum,
                            object_name=component.name,
                            name=property_name,
                            value=0,
                            datafile_text=datafile_name,
                            scenario=self.plexos_scenario,
                        )

                        logger.debug(
                            f"Linked {component.name}.{property_name} to DataFile {datafile.name} "
                            f"for time series '{ts_key.name}'"
                        )

                    except Exception as e:
                        logger.error(
                            f"Failed to link {component.name}.{property_name} to {datafile.name}: {e}"
                        )

    def _get_time_series_property_name(self, component: Any) -> str | None:
        """Get the PLEXOS property name that should reference the time series CSV."""
        from .models import (
            PLEXOSGenerator,
            PLEXOSRegion,
            PLEXOSStorage,
        )

        if isinstance(component, PLEXOSReserve):
            ts_keys = self.system.list_time_series_keys(component)
            if ts_keys:
                ts_key = ts_keys[0]
                variable_name = ts_key.name
                if variable_name == "min_provision":
                    return "Min Provision"
            return "Min Provision"

        elif isinstance(component, PLEXOSRegion):
            return "Load"

        elif isinstance(component, PLEXOSStorage):
            ts_keys = self.system.list_time_series_keys(component)
            if ts_keys:
                ts_key = ts_keys[0]
                variable_name = ts_key.name

                if variable_name in ("natural_inflow"):
                    return "Natural Inflow"
                elif variable_name in ("fixed_load"):
                    return "Fixed Load"

            return "Natural Inflow"

        elif isinstance(component, PLEXOSGenerator):
            category = getattr(component, "category", "").lower()

            if "hydro" in category:
                ts_keys = self.system.list_time_series_keys(component)
                if ts_keys:
                    ts_key = ts_keys[0]
                    variable_name = ts_key.name

                    if variable_name in ("fixed_load", "max_capacity", "rating"):
                        return "Fixed Load"

                return "Fixed Load"

            ts_keys = self.system.list_time_series_keys(component)
            if ts_keys:
                ts_key = ts_keys[0]
                variable_name = ts_key.name

                if variable_name in ("rating", "max_capacity"):
                    return "Rating"
                elif variable_name in ("fixed_load"):
                    return "Fixed Load"

            return "Rating"

        return None

    def export_time_series(self) -> Result[None, str]:
        """Export time series to CSV files and update property references.

        Returns
        -------
        Result[None, ExporterError]
            Ok(None) on success, Err(ExporterError) on failure
        """
        # Get ALL components with time series, not just PLEXOSObject
        all_components_with_ts = []
        for component_type in self.system.get_component_types():
            components = list(
                self.system.get_components(
                    component_type, filter_func=lambda c: self.system.has_time_series(c)
                )
            )
            all_components_with_ts.extend(components)

        if not all_components_with_ts:
            logger.warning("No components with time series found")
            return Ok(None)

        logger.debug(f"Found {len(all_components_with_ts)} components with time series")

        ts_metadata: list[tuple[Any, Any]] = []
        for component in all_components_with_ts:
            ts_keys = self.system.list_time_series_keys(component)
            ts_metadata.extend((component, ts_key) for ts_key in ts_keys)

        logger.debug(f"Found {len(ts_metadata)} time series keys total")

        def _grouping_key(item: tuple[Any, Any]) -> tuple[str, tuple[tuple[str, Any], ...]]:
            """Sort by component_type."""
            _, ts_key = item
            return (ts_key.name, tuple(sorted(ts_key.features.items())))

        ts_metadata_sorted = sorted(ts_metadata, key=_grouping_key)

        csv_filepaths: list[Path] = []
        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)

        for group_key, group_items in groupby(ts_metadata_sorted, key=_grouping_key):
            field_name, features_tuple = group_key
            metadata_dict = dict(features_tuple)
            if self.config.model_name is not None:
                metadata_dict["model_name"] = self.config.model_name
            if self.weather_year is not None:
                metadata_dict["weather_year"] = self.weather_year
            if self.solve_year is not None:
                metadata_dict["solve_year"] = self.solve_year
            group_list = list(group_items)

            first_component = group_list[0][0]
            component_class = type(first_component).__name__

            filename = generate_csv_filename(field_name, component_class, metadata_dict)
            filepath = output_dir / filename
            csv_filepaths.append(filepath)

            time_series_data: list[tuple[str, Any]] = []
            for component, ts_key in group_list:
                ts = self.system.get_time_series_by_key(component, ts_key)
                time_series_data.append((component.name, ts))

            result = export_time_series_csv(filepath, time_series_data)

            if result.is_err():
                assert isinstance(result, Err)
                logger.error(f"Failed to export time series: {result.error}")
                return Err(f"Time series export failed: {result.error}")

        logger.info(f"Exported {len(csv_filepaths)} time series files to {output_dir}")

        return Ok(None)

    def _create_datafile_objects(self) -> None:
        """Create DataFile objects for the CSVs that are being created."""
        logger.info("Creating DataFile objects...")

        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        time_series_dir = str(output_dir)

        if not os.path.exists(time_series_dir):
            logger.info(f"No time series directory found at {time_series_dir}")
            return

        for filename in os.listdir(time_series_dir):
            if filename.endswith(".csv"):
                file_path = os.path.join("Data", filename)
                datafile_obj = PLEXOSDatafile(
                    name=filename.removesuffix(".csv"),
                    filename=PLEXOSPropertyValue.from_dict({"datafile_name": file_path}),
                )
                if not self.system.has_component(datafile_obj):
                    self.system.add_component(datafile_obj)
                    logger.debug(f"Created DataFile object: {datafile_obj.name}")

    def _validate_xml(self, xml_path: str) -> bool:
        """Validate XML file structure."""
        try:
            import xml.etree.ElementTree as ET

            tree = ET.parse(xml_path)
            _ = tree.getroot()
            return True
        except ET.ParseError:
            return False
