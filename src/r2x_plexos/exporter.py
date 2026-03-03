"""Export PLEXOS system to XML."""

import os
from itertools import groupby
from pathlib import Path
from typing import Any, cast

from loguru import logger
from plexosdb import ClassEnum, PlexosDB
from plexosdb.enums import CollectionEnum, get_default_collection

from r2x_core import Err, Ok, Plugin, Result

from .models import (
    PLEXOSDatafile,
    PLEXOSGenerator,
    PLEXOSHorizon,
    PLEXOSMembership,
    PLEXOSModel,
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
    get_enum_from_string,
    ingest_simulation_to_plexosdb,
)

NESTED_ATTRIBUTES = {"ext", "bus", "services"}
DEFAULT_XML_TEMPLATE = "master_9.2R6_btu.xml"


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
        self.defaults: dict[str, Any] = PLEXOSConfig.load_defaults()

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

            self.plexos_scenario = self.plexos_scenario or self.config.model_name

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

            self._add_reports()

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
        """
        Set up the simulation configuration in the PlexosDB.

        Loads static model and horizon definitions from JSON files, merges them,
        and builds the simulation configuration for the specified year and resolution.
        Also applies simulation configuration objects (Performance, Production, etc.)
        if provided. Returns Ok(None) on success or Err with an error message.

        Returns
        -------
        Result[None, str]
            Ok(None) if setup is successful, Err(error message) otherwise.
        """
        if self.db is None:
            return Err("Database not initialized")

        logger.info("Setting up simulation configuration")

        static_model_defaults = PLEXOSConfig.load_static_models()
        static_horizon_defaults = PLEXOSConfig.load_static_horizons()
        defaults = {**static_model_defaults, **static_horizon_defaults}

        simulation_config_dict = getattr(self.config, "simulation_config", None)
        if simulation_config_dict is None:
            logger.debug("Using default simulation configuration")
            simulation_config_dict = get_default_simulation_config()

        horizon_year = getattr(self.config, "horizon_year", None) or getattr(
            self.config, "solve_year", None
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
            defaults=defaults,
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

        ingest_result = ingest_simulation_to_plexosdb(self.db, build_result, validate=False, scenario_name=self.plexos_scenario)
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

        # Define base and output diectories
        output_dir = get_output_directory(self.config, self.system, output_path=self.output_path)
        datafile_prefix = output_dir.name
        base_folder = Path(self.output_path) if self.output_path else output_dir.parent

        logger.info("Exporting time series")
        if self.should_export_time_series:
            ts_result = self.export_time_series()
            if isinstance(ts_result, Err):
                logger.error("Failed to export time series: {}", ts_result.error)
                return ts_result

        logger.info("Creating DataFile objects from exported CSVs")
        self._add_component_datafile_objects()

        logger.info("Adding component properties and memberships")
        self._add_component_properties(datafile_prefix=datafile_prefix)
        self._add_component_memberships()

        suffix = (
            f"_{self.weather_year}_{self.solve_year}"
            if self.weather_year is not None and self.solve_year is not None
            else ""
        )
        xml_filename = f"{self.config.model_name}{suffix}.xml"
        xml_path = base_folder / xml_filename

        logger.info(f"Exporting XML to {xml_path}")
        self.db.to_xml(xml_path)

        if not self._validate_xml(str(xml_path)):
            logger.error(f"Exported XML at {xml_path} is not valid!")
            return Err(f"Exported XML at {xml_path} is not valid!")
        else:
            logger.success("Exported XML was correctly validated.")

        return Ok(None)

    def _get_required_properties_for_component(self, comp: Any, type_name: str) -> set[str]:
        """Resolve required properties for a component using category-group aware lookup.

        Resolution order:
        1. Check if component category belongs to a category-group (e.g. 'renewable-dispatch')
        2. If so, use '{TypeName}.{group_name}' required properties if defined
        3. Otherwise fallback to base '{TypeName}' required properties

        Parameters
        ----------
        comp : Any
            The component instance.
        type_name : str
            The class name of the component (e.g. 'PLEXOSGenerator').

        Returns
        -------
        set[str]
            Set of required property names.
        """
        required_properties = self.defaults.get("required-properties", {})
        category_groups = self.defaults.get("category-groups", {})
        category = getattr(comp, "category", None)

        if category:
            category_to_group = {
                cat: group_name
                for group_name, categories in category_groups.items()
                for cat in categories
            }
            group_name = category_to_group.get(category)
            if group_name:
                group_key = f"{type_name}.{group_name}"
                if group_key in required_properties:
                    value = required_properties[group_key]
                    if isinstance(value, str):
                        value = required_properties.get(value, [])
                    return set(value)

        return set(required_properties.get(type_name, []))

    def _add_component_properties(self, datafile_prefix: str = "Data") -> None:
        """
        Add properties for all components in the system to the database.

        This method performs the following:
        1. Adds 'Filename' properties for all PLEXOSDatafile objects, referencing their CSV files.
        2. Iterates over all component types (excluding models, horizons, datafiles, and memberships).
        3. For each component, collects its properties, ensuring that all required properties (as defined in defaults.json)
        are included, even if their values are not set by default.
        4. Handles time series properties and excludes metadata fields.
        5. Performs a bulk insert of properties for each component type into the database.

        Parameters
        ----------
        datafile_prefix : str, optional
            The prefix to use for datafile paths (default is "Data").
        """
        if self.db is None:
            logger.error("Database not initialized")
            return

        logger.info("Adding component properties...")

        for component in self.system.get_components(PLEXOSDatafile):
            relative_path = f"{datafile_prefix}/{component.name}.csv"

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

            type_name = component_type.__name__
            for comp in self.system.get_components(component_type):
                has_time_series = self.system.has_time_series(comp)
                ts_property_name = None
                if has_time_series:
                    ts_property_name = self._get_time_series_property_name(comp)

                aliased_dict = comp.model_dump(by_alias=True, exclude_defaults=self.exclude_defaults)

                if self.exclude_defaults:
                    required_property_for_comp = self._get_required_properties_for_component(comp, type_name)

                    for prop_name in required_property_for_comp:
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
                    if ts_property_name and k == ts_property_name:
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
        """
        Add membership relationships between components to the database.

        This method collects all PLEXOSMembership objects from the system and adds their relationships
        to the database. It performs the following steps:
        1. Retrieves all supplemental membership attributes from the system.
        2. Skips memberships with missing parent or child objects, or with unmapped classes/collections.
        3. Ignores memberships involving Model or Horizon objects, as these are handled elsewhere.
        4. Looks up the database IDs for parent and child objects and their collection.
        5. Avoids inserting duplicate memberships by tracking unique (parent_object_id, collection_id, child_object_id) keys.
        6. Bulk inserts all valid membership records into the database.

        Warnings and errors are logged for missing or invalid memberships, and a summary is logged upon completion.
        """
        if self.db is None:
            logger.error("Database not initialized")
            return

        memberships = list(self.system.get_supplemental_attributes(PLEXOSMembership))

        if not memberships:
            logger.warning("No memberships found in system")
            return

        records = []
        # Track unique (parent_object_id, collection_id, child_object_id)
        seen_memberships = set()
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
            logger.warning("No valid membership records to add.")
            return

        self.db.add_memberships_from_records(records)
        logger.success(f"Successfully added {len(records)} memberships.")

    def _add_component_datafile_objects(self) -> None:
        """
        Add PLEXOSDatafile objects from the system to the database.

        This method:
        1. Calls _create_datafile_objects() to ensure all DataFile objects for exported CSVs exist in the system.
        2. Retrieves all PLEXOSDatafile components from the system.
        3. Adds each DataFile object to the database with the appropriate class and category.
        4. Updates each DataFile object with its assigned database object_id.
        5. Links DataFile objects to component properties that reference time series CSVs.

        Logs progress and warnings for missing or empty DataFile sets.
        """
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
        """
        Link DataFile objects to component properties that reference time series CSVs.

        This method finds all components in the system that have associated time series data,
        matches each time series to its exported CSV file, and updates the corresponding
        component property to reference the correct DataFile object. The linkage is made
        via the `datafile_text` field and, if applicable, the t_tag table in the database.

        Steps performed:
        1. Iterates over all component types and finds those with time series.
        2. For each time series key, matches the exported CSV file using a naming pattern.
        3. Looks up the corresponding PLEXOSDatafile object and ensures it has a valid object_id.
        4. Determines the correct property name(s) for the time series (handling special cases).
        5. Adds a property to the database linking the component property to the DataFile.
        6. Logs all linkages and any errors encountered.

        This ensures that all time series properties in the exported XML reference the correct
        DataFile objects, enabling PLEXOS to locate and use the time series CSVs.
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

            if not components_with_ts:
                continue

            for component in components_with_ts:
                ts_keys = self.system.list_time_series_keys(component)

                if not ts_keys:
                    continue

                for ts_key in ts_keys:
                    component_class = type(component).__name__
                    pattern = re.compile(rf"{re.escape(component_class)}_{re.escape(ts_key.name)}_.*\.csv")

                    matched_file = None
                    for filename in os.listdir(output_dir):
                        if pattern.match(filename):
                            matched_file = filename
                            break

                    if not matched_file:
                        continue

                    datafile_name = matched_file.removesuffix(".csv")
                    datafile = self.system.get_component(PLEXOSDatafile, name=datafile_name)

                    if not datafile or datafile.object_id is None:
                        continue

                    class_enum = PLEXOS_TYPE_MAP_INVERTED.get(cast(type[PLEXOSObject], type(component)))
                    if not class_enum:
                        continue

                    property_names = []
                    # This two-property time series is for VariableGenerators (solar/wind)
                    if isinstance(component, PLEXOSGenerator) and ts_key.name == "max_active_power":
                        property_names = ["Rating", "Load Subtracter"]
                    else:
                        property_name = self._get_time_series_property_name(component, ts_key_name=ts_key.name)
                        if property_name:
                            property_names = [property_name]

                    csv_relative_path = str(output_dir.relative_to(output_dir.parent) / matched_file)
                    for property_name in property_names:
                        try:
                            self.db.add_property(
                                class_enum,
                                object_name=component.name,
                                name=property_name,
                                value=0,
                                datafile_text=csv_relative_path,
                                scenario=self.plexos_scenario,
                            )
                            logger.debug(f"Linked {component.name}.{property_name} to {csv_relative_path}")

                        except Exception as e:
                            logger.error(
                                f"Failed to link {component.name}.{property_name} to {csv_relative_path}: {e}"
                            )

    def _get_time_series_property_name(self, component: Any, ts_key_name: str | None = None) -> str | None:
        """
        Get the PLEXOS property name that should reference the time series CSV.

        This method determines the correct property name in PLEXOS that should be linked to a time series CSV
        for a given component and time series key. It uses specific mappings for common component types and
        time series variable names, and falls back to generic mappings if needed.

        Parameters
        ----------
        component : Any
            The component to get the property name for.
        ts_key_name : str | None, optional
            The specific time series key name (e.g., 'max_active_power').
            If None, will use the first time series key found for the component.

        Returns
        -------
        str | None
            The property name to use for the time series, or None if not applicable.
        """
        if isinstance(component, PLEXOSReserve):
            return "Min Provision"

        if isinstance(component, PLEXOSRegion):
            return "Load"

        if isinstance(component, PLEXOSStorage):
            return "Natural Inflow"

        if isinstance(component, PLEXOSGenerator):
            if ts_key_name:
                variable_name = ts_key_name
            else:
                ts_keys = self.system.list_time_series_keys(component)
                if not ts_keys:
                    return None
                variable_name = ts_keys[0].name

            if variable_name == "hydro_budget":
                return "Max Energy Day"
            return "Rating"
        return None

    def export_time_series(self) -> Result[None, str]:
        """
        Export all time series data from the system to CSV files and update property references.

        This method performs the following:
        1. Finds all components in the system that have associated time series data.
        2. Groups time series by field name and metadata, generating a unique CSV filename for each group.
        3. Exports each group of time series data to a CSV file in the output directory.
        4. Logs the number of exported files and any errors encountered.
        5. Returns Ok(None) on success, or Err with an error message on failure.

        Returns
        -------
        Result[None, str]
            Ok(None) on success, Err(error_message) on failure.
        """
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
        """
        Create PLEXOSDatafile objects for each CSV file being created.

        This method scans the output directory for all CSV files representing exported time series.
        For each CSV file found, it creates a corresponding PLEXOSDatafile object (if it does not already exist in the system)
        with the appropriate name and filename property. These DataFile objects are then available for linking to component
        properties that reference time series data.

        Steps performed:
        1. Determines the output directory for time series CSVs.
        2. Checks for the existence of the directory and logs if missing.
        3. Iterates over all CSV files in the directory.
        4. For each CSV, creates a PLEXOSDatafile object with the correct name and filename.
        5. Adds the DataFile object to the system if it does not already exist.

        Logs each DataFile object created.
        """
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
        """
        Validate the structure of an XML file.

        Attempts to parse the XML file at the given path using ElementTree.
        Returns True if the file is well-formed and can be parsed, otherwise False.

        Parameters
        ----------
        xml_path : str
            Path to the XML file to validate.

        Returns
        -------
        bool
            True if the XML is valid, False otherwise.
        """
        try:
            import xml.etree.ElementTree as ET

            tree = ET.parse(xml_path)
            _ = tree.getroot()
            return True
        except ET.ParseError:
            return False


    def _add_reports(self) -> None:
        """Add report definitions from plexos_reports.json to the PlexosDB."""
        report_objects = PLEXOSConfig.load_reports()
        for report_object in report_objects:
            report_object["collection"] = get_enum_from_string(report_object["collection"], CollectionEnum)
            report_object["parent_class"] = get_enum_from_string(report_object["parent_class"], ClassEnum)
            report_object["child_class"] = get_enum_from_string(report_object["child_class"], ClassEnum)
            self.db.add_report(**report_object)
