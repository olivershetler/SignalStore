from signalstore.store.data_access_objects import *
from signalstore.store.store_errors import *
from signalstore.utilities.tools.strings import contains_regex_characters

from abc import ABC, abstractmethod
import jsonschema
import json
from datetime import datetime
from time import sleep
from concurrent.futures import ThreadPoolExecutor


# ================================
# Base and Support Classes
# ================================

class AbstractRepository(ABC):

    # Data Retrieval Operations

    @abstractmethod
    def get(self):
        """Get a single aggregated object."""
        pass

    @abstractmethod
    def exists(self):
        """Check if an aggregated object exists."""
        pass

    # Operations That Modify the Repository

    @abstractmethod
    def add(self):
        """Add a single aggregated object to all relevant collections."""
        pass

    @abstractmethod
    def remove(self):
        """Mark a single aggregated object for deletion in all relevant collections."""
        pass

    # Tracking Operations That Modify the Repository
    def timestamp(self):
        """Get a timestamp to use for tracking and sorting CRUD operations.
        """
        return datetime.now().astimezone(timezone.utc)


    @abstractmethod
    def undo(self):
        """Undo most recent CRUD operation."""
        pass

    @abstractmethod
    def undo_all(self):
        """Undo all CRUD operations in self._operation_history."""
        pass

    @abstractmethod
    def clear_operation_history(self):
        """Clear the history of tracked operations."""
        pass

    # Purging removed Objects

    @abstractmethod
    def list_marked_for_deletion(self):
        """List aggregated objects marked for deletion."""
        pass

    @abstractmethod
    def purge(self):
        """Purge (permanently delete) aggregated objects marked for deletion."""
        pass

    @abstractmethod
    def _validate(self):
        """Validate a single aggregated object prior to adding it into the repository."""
        pass


class AbstractQueriableRepository(AbstractRepository):

    @abstractmethod
    def find(self):
        """Apply filtering to get multiple aggregated objects fitting a description."""
        pass

# Operation History Entry

class OperationHistoryEntry:
    def __init__(self, timestamp: datetime, collection_name: str, operation: str, **kwargs):
        assert isinstance(timestamp, datetime)
        self.timestamp = timestamp
        self.collection_name = collection_name
        if not operation in ["added", "removed"]:
            raise OperationHistoryEntryValueError(f"operation must be one of 'added' or 'removed', not '{operation}'")
        self.operation = operation
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        repstr = f"OperationHistoryEntry(timestamp={self.timestamp}, repository={self.collection_name}, operation={self.operation}"
        for attr in self.__dict__:
            if attr not in ["timestamp", "collection_name", "operation"]:
                repstr += f", {attr}={getattr(self, attr)}"

    def __eq__(self, other):
        for attr in self.__dict__:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return self.timestamp > other.timestamp

    def __geq__(self, other):
        return self.timestamp >= other.timestamp

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    def __leq__(self, other):
        return self.timestamp <= other.timestamp

    def dict(self):
        return self.__dict__

class OperationHistoryEntryValueError(ValueError):
    pass


# ================================
# Domain Model Repository
# ================================



class DomainRepositoryModelAlreadyExistsError(AlreadyExistsError):
    pass

class DomainRepositoryModelNotFoundError(NotFoundError):
    pass

class DomainRepositoryRangeError(RangeError):
    pass

class DomainRepositoryTypeError(ArgumentTypeError):
    pass

class DomainRepositoryValidationError(ValidationError):
    pass

class DomainRepositoryUncaughtError(UncaughtError):
    pass

data_identifier_regex = "^(?!.*__.*)(?!.*time_of_removal.*)(?!.*time_of_save.*)[a-z][a-z0-9_]*[a-z0-9]$"

model_identifier_regex = "^(?!.*__.*)[a-z][a-z0-9_]*[a-z0-9]$"



domain_model_json_schema = {
  "title": "Model of Model Records",
  "description": "A schema for validating model records which includes properties of different model types",
  "type": "object",
  "properties": {
    "schema_name": {
      "type": "string",
      "pattern": model_identifier_regex
    },
    "schema_title": {
      "type": "string",
      "pattern": "^[A-Za-z0-9][A-Za-z0-9 ]+[A-Za-z0-9]$"
    },
    "schema_description": {
      "type": "string",
      # pattern enforcing description cannot be empty or contain trailing whitespace
      "pattern": "^\\S(.*\\S)?$"
    },
    "schema_type": {
      "type": "string",
      "enum": ["property_model", "data_model", "metamodel"]
    },
    "json_schema": {
    "type": "object",
    "required": ["type"],
    "properties": {
        "type": {
        "oneOf": [
            {
                "type": "string"
            },
            {
                "type": "array",
                "items": {
                    "type": "string"
                        }
                    }
                ]
            }
        }
    },
    "metamodel_ref": {
      "type": ["string", "null"],
      "pattern": model_identifier_regex
    },
    "version_timestamp": {
        "type": ["datetime", "integer"],
        "if": { "type": "integer" },
        "then": {
            "const": 0
        }
        },
    "time_of_save": {
      "type": "datetime"
    },
    "time_of_removal": {
      "type": ["datetime", "null"]
    }
  },
  "required": ["schema_name", "schema_title", "schema_description", "schema_type", "json_schema"],
  "allOf": [
                {
                    "if": { "allOf": [
                            {"properties": {"schema_type": {"const": "data_model"}}},
                            {"required": ["schema_type"]},
                                ]
                        },
                    "then": {
                        "required": ["metamodel_ref"],
                        }
                    },
                # if the schema_type is metamodel or data_model, then the json schema must have type property equal to 'object'
                {
                    "if": { "anyOf": [
                            {"properties": {"schema_type": {"const": "metamodel"}}},
                            {"properties": {"schema_type": {"const": "data_model"}}},
                                ]
                        },
                    "then": {
                        "properties": {
                            "json_schema": {
                                "properties": {
                                    "type": {
                                        "const": "object"
                                    }
                                }
                            }
                        }
                    }
                }
            ],
  "additionalProperties": False
}

def is_datetime(checker, instance):
    return isinstance(instance, type(datetime.now().astimezone()))
# Create a new type checker that adds 'datetime' as a new type
type_checker = jsonschema.Draft7Validator.TYPE_CHECKER.redefine("datetime", is_datetime)
# Create a new validator class using the new type checker
CustomValidator = jsonschema.validators.extend(
    jsonschema.Draft7Validator, type_checker=type_checker
)

class DomainModelRepository(AbstractQueriableRepository):
    """A repositroy for storing Domain Model Objects such as a Controlled Vocabulary or a Object Type Schema collection.
    """
    def __init__(self, model_dao, model_metaschema=domain_model_json_schema):
        self._dao = model_dao
        self._operation_history = []
        self._model_metaschema = model_metaschema
        self._arg_options = {
            "schema_name": (str),
            "model": (dict),
            "filter": (dict, type(None)),
            "projection": (dict, type(None)),
        }
        self._validator = CustomValidator

    def get(self, schema_name):
        """Get a single domain model object."""
        self._check_args(schema_name=schema_name)
        # get the model
        model = self._dao.get(schema_name=schema_name)
        if model is None:
            return None
        self._validate(model)
        # return the model
        return model

    def find(self, filter=None, projection=None, **kwargs):
        """Apply filtering to get multiple domain model objects fitting a description."""
        self._check_args(filter=filter, projection=projection)
        models = self._dao.find(filter=filter, projection=projection, **kwargs)
        # validate the models

        for model in models:
            self._validate(model)
        # return the models
        return models

    def exists(self, schema_name):
        """Check if a domain model object exists."""
        self._check_args(schema_name=schema_name)
        return self._dao.exists(schema_name=schema_name)

    def add(self, model):
        """Add a single domain model object to the repository."""
        self._check_args(model=model)
        ohe = OperationHistoryEntry(self.timestamp(), self._dao.collection_name, "added", schema_name=model["schema_name"], has_file=False)
        # validate the model
        self._validate(model)
        if self._dao.exists(schema_name=model["schema_name"]):
            raise DomainRepositoryModelAlreadyExistsError(f"A model with schema_name '{model['schema_name']}' already exists in the repository.")
        try:
            self._dao.add(document=model, timestamp=ohe.timestamp)
        except Exception as e:
            raise DomainRepositoryUncaughtError(f"An uncaught error occurred while adding the model to the repository.\n\nTraceback: {e}")
        self._operation_history.append(ohe)
        return ohe

    def remove(self, schema_name):
        """Mark a single domain model object for deletion; remove it from the scope of get and list searches."""
        self._check_args(schema_name=schema_name)
        ohe = OperationHistoryEntry(self.timestamp(), self._dao.collection_name, "removed", schema_name=schema_name, has_file=False)
        if not self._dao.exists(schema_name=schema_name):
            raise DomainRepositoryModelNotFoundError(f"A model with schema_name '{schema_name}' does not exist in the repository.")
        try:
            self._dao.mark_for_deletion(schema_name=schema_name, timestamp=ohe.timestamp)
        except Exception as e:
            raise DomainRepositoryUncaughtError(f"An uncaught error occurred while marking the model for deletion.\n\nTraceback: {e}")
        self._operation_history.append(ohe)
        return ohe

    def undo(self):
        """Undo most recent CRUD operation."""
        try:
            ohe = self._operation_history[-1]
            if ohe is None:
                return None
        except IndexError:
            return None
        now = self.timestamp()
        if ohe.operation=="removed":
            self._dao.restore(
                schema_name = ohe.schema_name,
                nth_most_recent = 1
                )
        elif ohe.operation=="added":
            self._dao.mark_for_deletion(schema_name = ohe.schema_name,
                                        timestamp = ohe.timestamp)
        # remove the operation history entry after successfully undoing the operation
        self._operation_history.pop()
        return ohe

    def undo_all(self):
        """Undo all CRUD operations in self._operation_history."""
        undone_operations = []
        while len(self._operation_history) > 0:
            operation = self.undo()
            undone_operations.append(operation)
        return undone_operations

    def clear_operation_history(self):
        """Clear the history of CRUD operations."""
        self._operation_history = []

    def list_marked_for_deletion(self):
        """List domain model objects marked for deletion."""
        try:
            return self._dao.list_marked_for_deletion()
        except Exception as e:
            raise DomainRepositoryUncaughtError(f"An uncaught error occurred while listing the terms marked for deletion.\n\nTraceback: {e}")

    def purge(self, time_threshold=None):
        """Purge (permanently delete) domain model objects marked for deletion."""
        try:
            self._dao.purge(time_threshold)
        except Exception as e:
            raise DomainRepositoryUncaughtError(f"An uncaught error occurred while purging the repository.\n\nTraceback: {e}")

    def _validate(self, model):
        """Validate a single domain model object prior to adding it into the repository."""
        try:
            validator = self._get_validator(self._model_metaschema)
            validator.validate(model)
        except jsonschema.exceptions.ValidationError as e:
            message = self._validation_error_message(e, model, self._model_metaschema)
            raise DomainRepositoryValidationError(message)
        # if the model has a metamodel_ref property
        # check that the metamodel_ref exists in the repository
        # and if so, validate the model against its metamodel
        if model.get("metamodel_ref") is not None:
            metamodel_ref = model.get("metamodel_ref")
            if not self._dao.exists(schema_name=metamodel_ref):
                raise DomainRepositoryValidationError(f"The metamodel_ref '{metamodel_ref}' does not exist in the repository.")
            metamodel = self._dao.get(schema_name=metamodel_ref)
            metaschema = metamodel.get("json_schema")
            try:
                validator = self._get_validator(metaschema)
                validator.validate(model)
            except jsonschema.exceptions.ValidationError as e:
                message = self._validation_error_message(e, model, metaschema)
                raise DomainRepositoryValidationError(message)


    def _get_validator(self, schema):
        """Get a validator for a schema."""
        return self._validator(schema)


    def _validation_error_message(self, e, model, schema):
        """Get an enhanced validation error message."""

        message = f"\n\nValidation Error\n-------------------\n\n"
        message += f"Message: {e.message}\n\n"
        message += f"Instance: {e.instance}\n\n"
        message += f"Path: {e.path}\n\n"
        message += f"Relative Path: {e.relative_path}\n\n"
        message += f"Absolute Path: {e.absolute_path}\n\n"
        message += f"Schema Path: {e.schema_path}\n\n"
        message += f"Local Schema: {e.schema}\n\n"
        message += f"Args: {e.args}\n\n"
        message += f"Cause: {e.cause}\n\n"
        message += f"Context: {e.context}\n\n"
        message += f"Validator: {e.validator}\n\n"
        message += f"Validator Value: {e.validator_value}\n\n"
        message += f"Model: {model}\n\n"
        message += f"Full Schema: {schema}\n\n"

        return message

    def _check_args(self, **kwargs):
        for key, value in kwargs.items():
            if not isinstance(value, self.arg_options[key]):
                raise DomainRepositoryTypeError(f"{key} must be of type {self._arg_options[key]}, not {type(value)}")

    @property
    def arg_options(self):
        return self._arg_options

# ================================
# Data Repository
# ================================

class DataRepositoryAlreadyExistsError(AlreadyExistsError):
    pass

class DataRepositoryNotFoundError(NotFoundError):
    pass

class DataRepositoryRangeError(RangeError):
    pass

class DataRepositoryTypeError(ArgumentTypeError):
    pass

class DataRepositoryValidationError(ValidationError):
    pass

class DataRepositoryUncaughtError(UncaughtError):
    pass

class DataRepository(AbstractQueriableRepository):
    # only indexes on schema_ref,  data_name, and version_timestamp
    """A repository for records such as session metadata, data array metadata and object state metadata."""
    def __init__(self, record_dao, file_dao, domain_repo):
        self._records = record_dao
        self._data = file_dao
        self._domain_models = domain_repo
        self._operation_history = []
        self._validator = CustomValidator


    def get(self, schema_ref, data_name, nth_most_recent=None, version_timestamp=0, data_adapter=None, validate=True):
        """Get a single record."""
        # if argument is a dict, try unpacking it
        if not nth_most_recent is None and nth_most_recent < 1:
            raise DataRepositoryRangeError(f"nth_most_recent must be an integer greater than 0, not {nth_most_recent}.")
        self._check_args(
            schema_ref=schema_ref,
            data_name=data_name,
            version_timestamp=version_timestamp
            )
        if nth_most_recent is not None and version_timestamp==0:
            record = self._records.find(filter={"schema_ref": schema_ref, "data_name": data_name}, sort=[("version_timestamp", 1)], limit=nth_most_recent)[-1]
            version_timestamp = record.get("version_timestamp")
        else:
            record = self._records.get(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp)
        if record is None:
            return None
        if validate:
            self._validate(record)
        has_file = record.get("has_file")
        if has_file:
            data = self._data.get(
                schema_ref=schema_ref,
                data_name=data_name,
                version_timestamp=version_timestamp,
                data_adapter=data_adapter
                )
            if data is None:
                raise DataRepositoryNotFoundError(f"Data for record with schema_ref '{schema_ref}', data_name '{data_name}', and version_timestamp '{version_timestamp}' is missing its file. The record exists and has the 'has_file' attribute set to True, but the file data access object returned None.")
            # check that data.attrs is a subset of the record's attrs
            attr_keys = set(data.attrs.keys())
            record_keys = set(record.keys())
            if not attr_keys.issubset(record_keys):
                raise DataRepositoryValidationError(f"The data.attrs keys {attr_keys} are not a subset of the record keys {record_keys}. The difference is {attr_keys.difference(record_keys)}.")
            #data.attrs.update(record)
            return data
        else:
            return record

    def find(self, filter=None, projection=None, sort=None, limit=None, get_data=False, validate=True):
        """Apply filtering to get multiple records fitting a description."""
        self._check_args(
            filter=filter,
            projection=projection)
        if sort is not None and limit is not None:
            records = self._records.find(filter=filter, projection=projection).sort(sort).limit(limit)
        elif sort is not None:
            records = self._records.find(filter=filter, projection=projection).sort(sort)
        elif limit is not None:
            records = self._records.find(filter=filter, projection=projection).limit(limit)
        else:
            records = self._records.find(filter=filter, projection=projection)
        # validate the records
        if validate:
            for record in records:
                self._validate(record)
        if get_data:
            data = []
            for record in records:
                if record.get("has_file"):
                    schema_ref = record.get("schema_ref")
                    data_name = record.get("data_name")
                    version_timestamp = record.get("version_timestamp")
                    data_object = self._data.get(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp)
                    data.append(data_object)
                else:
                    data.append(record)
            return data
        else:
            return records

    def exists(self, schema_ref, data_name, version_timestamp=0):
        """Check if a record exists.
        Caveats:
            Does not check if data files have been lost.
            """
        self._check_args(
            schema_ref=schema_ref,
            data_name=data_name,
            version_timestamp=version_timestamp)
        record_exists = self._records.exists(version_timestamp=version_timestamp, schema_ref=schema_ref, data_name=data_name)
        return record_exists

    def has_file(self, schema_ref, data_name, version_timestamp=0):
        """Check if a record has data."""
        self._check_args(
            schema_ref=schema_ref,
            data_name=data_name,
            version_timestamp=version_timestamp)
        has_file = self._data.exists(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp)
        return has_file

    def add(self, object, data_adapter=None, versioning_on=False):
        """Add a single object to the repository."""
        add_timestamp = self.timestamp()
        dttype = type(datetime.now().astimezone())
        if isinstance(object, dict):
            if versioning_on and not isinstance(object.get("version_timestamp"), dttype):
                object["version_timestamp"] = add_timestamp
            elif not versioning_on:
                object["version_timestamp"] = 0
            elif not isinstance(object.get("version_timestamp"), dttype):
                raise DataRepositoryTypeError(f"'version_timestamp' must be a {dttype} object or the integer 0, not {type(object.get('version_timestamp'))}.")
            ohe = self._add_record(
                object=object,
                add_timestamp=add_timestamp,
                versioning_on=versioning_on
                )
            return ohe
        elif hasattr(object, "attrs"):
            if versioning_on and not isinstance(object.attrs.get("version_timestamp"), dttype):
                object.attrs["version_timestamp"] = add_timestamp
            elif not versioning_on:
                object.attrs["version_timestamp"] = 0
            elif not isinstance(object.attrs.get("version_timestamp"), dttype):
                raise DataRepositoryTypeError(f"'version_timestamp' must be a {dttype} object or the integer 0, not {type(object.attrs.get('version_timestamp'))}.")
            ohe = self._add_data_with_file(
                object=object,
                add_timestamp=add_timestamp,
                versioning_on=versioning_on,
                data_adapter=data_adapter
                )
            return ohe
        else:
            raise DataRepositoryTypeError(f"object must be a dict or an object with an 'attrs' attribute, not {type(object)}")

    def _add_record(self, object, add_timestamp, versioning_on):
        ohe = OperationHistoryEntry(
                add_timestamp,
                self._records.collection_name,
                "added",
                schema_ref=object["schema_ref"],
                data_name=object["data_name"],
                version_timestamp=object["version_timestamp"],
                data_adapter = None,
                has_file = False)
        if object.get("has_file") is None:
            object['has_file'] = False
        self._validate(object)
        self._records.add(
                    document=object,
                    timestamp=add_timestamp,
                    versioning_on=versioning_on
                    )
        self._operation_history.append(ohe)
        return ohe

    def _add_data_with_file(self, object, add_timestamp, versioning_on, data_adapter=None):
        if data_adapter is None:
            data_adapter = self._data._default_data_adapter
        ohe = OperationHistoryEntry(
            add_timestamp,
            self._records.collection_name, "added",
            schema_ref=object.attrs["schema_ref"],
            data_name=object.attrs["data_name"],
            has_file = True,
            data_adapter = data_adapter,
            version_timestamp=object.attrs["version_timestamp"]
            )
        if object.attrs.get("has_file") is None:
            object.attrs["has_file"] = True
        self._validate(object.attrs)
        self._records.add(
            document=object.attrs,
            timestamp=ohe.timestamp,
            versioning_on=versioning_on
            )
        self._data.add(
            data_object=object,
            data_adapter=data_adapter
            )
        self._operation_history.append(ohe)
        return ohe

    def remove(self, schema_ref, data_name, version_timestamp=0, data_adapter=None):
        """Mark a single record for deletion; remove it from the scope of get and list searches."""
        self._check_args(
            schema_ref=schema_ref,
            data_name=data_name,
            version_timestamp=version_timestamp)
        if not self._records.exists(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp):
            raise DataRepositoryNotFoundError(f"A record with schema_ref '{schema_ref}', data_name '{data_name}', and version_timestamp '{version_timestamp}' does not exist in the repository.")
        has_file = self._data.exists(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp, data_adapter=data_adapter)

        ohe = OperationHistoryEntry(
            self.timestamp(),
            self._records.collection_name,
            "removed", schema_ref=schema_ref,
            data_name=data_name,
            version_timestamp=version_timestamp,
            has_file=has_file,
            data_adapter=data_adapter
            )

        if has_file:
            if data_adapter is None:
                data_adapter = self._data._default_data_adapter
            self._data.mark_for_deletion(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp, time_of_removal=ohe.timestamp, data_adapter=data_adapter)
        self._records.mark_for_deletion(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp, timestamp=ohe.timestamp)
        self._operation_history.append(ohe)
        return ohe


    def undo(self):
        try:
            ohe = self._operation_history[-1]
        except IndexError:
            return None
        if ohe.operation=="removed":
            self._records.restore(
                schema_ref = ohe.schema_ref,
                data_name = ohe.data_name,
                version_timestamp = ohe.version_timestamp,
                nth_most_recent = 1
                )
            if ohe.has_file:
                self._data.restore(
                    schema_ref = ohe.schema_ref,
                    data_name = ohe.data_name,
                    version_timestamp = ohe.version_timestamp,
                    data_adapter = ohe.data_adapter,
                    nth_most_recent = 1
                    )
        elif ohe.operation=="added":
            self._records.mark_for_deletion(schema_ref = ohe.schema_ref,
                                            data_name = ohe.data_name,
                                            version_timestamp = ohe.version_timestamp,
                                            timestamp = ohe.timestamp
                                            )
            if ohe.has_file:
                self._data.mark_for_deletion(
                    schema_ref = ohe.schema_ref,
                    data_name = ohe.data_name,
                    version_timestamp = ohe.version_timestamp,
                    time_of_removal = ohe.timestamp,
                    data_adapter = ohe.data_adapter
                    )
        # remove the operation history entry after successfully undoing the operation
        self._operation_history.pop()
        return ohe

    def undo_all(self):
        """Undo all CRUD operations in self._operation_history."""
        undone_operations = []
        while len(self._operation_history) > 0:
            operation = self.undo()
            undone_operations.append(operation)
        return undone_operations

    def clear_operation_history(self):
        """Clear the history of CRUD operations."""
        self._operation_history = []

    def list_marked_for_deletion(self, time_threshold=None):
        """List records marked for deletion."""
        self._check_args(time_threshold=time_threshold)
        records = self._records.list_marked_for_deletion(time_threshold=time_threshold)
        paths = self._data.list_marked_for_deletion(time_threshold=time_threshold)
        records_with_paths = []
        records_without_paths = []
        for record in records:
            if record.get("has_file"):
                records_with_paths.append(record)
            else:
                records_without_paths.append(record)
        tuples_with_paths = list(zip(records_with_paths, paths))
        tuples_without_paths = list(zip(records_without_paths, [None]*len(records_without_paths)))
        tuples = tuples_with_paths + tuples_without_paths
        tuples.sort(key=lambda x: x[0].get("time_of_removal"))
        return tuples

    def purge(self, time_threshold=None):
        """Purge (permanently delete) records marked for deletion."""
        self._records.purge(time_threshold=time_threshold)
        self._data.purge(time_threshold=time_threshold)

    def _validate(self, record):
        """Validate a single object prior to adding it into the repository."""
        schema_ref = record.get("schema_ref")
        # get teh main domain model using the schema_ref
        domain_model = self._domain_models.get(schema_name=schema_ref)
        # check that the schema_ref exists in the repository
        if domain_model is None:
            raise DataRepositoryValidationError(f"The schema_ref '{schema_ref}' does not exist in the repository. The original record is\n\n{record}.")
        # get the json schema from the domain model and try to validate the record
        record_json_schema = domain_model.get("json_schema")
        try:
            validator = self._get_validator(record_json_schema)
            validator.validate(record)
        except jsonschema.exceptions.ValidationError as e:
            message = self._validation_error_message(e, record, record_json_schema)
            raise DataRepositoryValidationError(message)
        # if the record has passed overall validation, then check that each property is valid
        # each property should have a corresponding domain model with the same schema_name as the property name
        def validate_property(property_name, value):
            # special case: if the property name ends with "_data_ref", then we use the "data_ref" domain property model
            # we do not expect a specific for each *_data_ref property name
            if property_name.endswith("_data_ref"):
                property_model = self._domain_models.get(schema_name="data_ref")
            else:
                # if this property is not a special case, then we expect a domain model with the same schema_name as the property name
                property_model = self._domain_models.get(schema_name=property_name)
            if property_model is None:
                raise DataRepositoryValidationError(f"The property '{property_name}' does not exist in the controlled vocabulary. The original record is\n\n{record}.")
            property_json_schema = property_model.get("json_schema")
            try:
                validator = self._get_validator(property_json_schema)
                validator.validate(value)
            except jsonschema.exceptions.ValidationError as e:
                message = self._validation_error_message(e, record, property_json_schema, property_name)
                raise DataRepositoryValidationError(message)
        for key, value in record.items():
            validate_property(key, value)

    def _check_args(self, **kwargs):
        for key, value in kwargs.items():
            if not isinstance(value, self._arg_options[key]):
                raise DataRepositoryTypeError(f"{key} must be of type {self._arg_options[key]}, not {type(value)}")

    @property
    def _arg_options(self):
        return {
            "schema_ref": (str),
            "data_name": (str),
            "version_timestamp": (datetime, int),
            "time_threshold": (datetime, type(None)),
            "filter": (dict, type(None)),
            "projection": (dict, type(None)),
        }

    def _get_validator(self, schema):
        """Get a validator for a schema."""
        return self._validator(schema)

    def _validation_error_message(self, e, record, property_json_schema, property_name=None):
        """Get an enhanced validation error message."""

        message = f"\n\nDataRepositoryValidationError\n-------------------\n\n"
        if property_name is not None:
            message += f"Property Name: {property_name}\n\n"
        message += f"Message: {e.message}\n\n"
        message += f"Instance: {e.instance}\n\n"
        message += f"Path: {e.path}\n\n"
        message += f"Relative Path: {e.relative_path}\n\n"
        message += f"Absolute Path: {e.absolute_path}\n\n"
        message += f"Schema Path: {e.schema_path}\n\n"
        message += f"Local Schema: {e.schema}\n\n"
        message += f"Args: {e.args}\n\n"
        message += f"Cause: {e.cause}\n\n"
        message += f"Context: {e.context}\n\n"
        message += f"Validator: {e.validator}\n\n"
        message += f"Validator Value: {e.validator_value}\n\n"
        message += f"Record: {record}\n\n"
        message += f"Full Schema: {property_json_schema}\n\n"

        return message


# ================================
# In Memory Repository
# ================================

class InMemoryRepositoryAlreadyExistsError(AlreadyExistsError):
    pass

class InMemoryRepositoryNotFoundError(NotFoundError):
    pass

class InMemoryRepositoryRangeError(RangeError):
    pass

class InMemoryRepositoryTypeError(ArgumentTypeError):
    pass

class InMemoryRepositoryValidationError(ValidationError):
    pass

class InMemoryRepositoryUncaughtError(UncaughtError):
    pass

class InMemoryObjectRepository(AbstractRepository):
    """Repository for storing objects in memory."""
    def __init__(self, memory_dao):
        self._dao = memory_dao
        self._operation_history = []

    def get(self, schema_ref, object_name):
        """Get a single object."""
        self._check_args(schema_ref=schema_ref, object_name=object_name)
        return self._dao.get(schema_ref, object_name)

    def exists(self, schema_ref, object_name):
        """Check if an object exists."""
        self._check_args(schema_ref=schema_ref, object_name=object_name)
        return self._dao.exists(schema_ref, object_name)

    def add(self, schema_ref, object_name, object):
        """Add a single object to the repository."""
        self._check_args(schema_ref=schema_ref, object_name=object_name, object=object)
        if self._dao.exists(schema_ref, object_name):
            raise InMemoryRepositoryAlreadyExistsError(f"An object with data_name '{object_name}' already exists in the repository.")
        ohe = OperationHistoryEntry(self.timestamp(), self._dao.collection_name, "added", schema_ref=schema_ref, object_name=object_name, has_file=False)
        self._dao.add(schema_ref, object_name, object)
        self._operation_history.append(ohe)
        return ohe

    def remove(self, schema_ref, object_name):
        """Mark a single object for deletion; remove it from the scope of get and list searches."""
        self._check_args(schema_ref=schema_ref, object_name=object_name)
        ohe = OperationHistoryEntry(self.timestamp(), self._dao.collection_name, "removed", schema_ref=schema_ref, object_name=object_name, has_file=False)
        self._dao.remove(schema_ref, object_name)
        self._operation_history.append(ohe)
        return ohe

    def list_marked_for_deletion(self, time_threshold=None):
        return super().list_marked_for_deletion(time_threshold=time_threshold)

    def undo(self):
        """Undo most recent CRUD operation."""
        try:
            ohe = self._operation_history[-1]
        except IndexError:
            raise InMemoryRepositoryRangeError(f"There are no operations in the operations history to undo. self._operations_history == {self._operation_history}")
        if ohe.operation=="removed":
            self._dao.restore(schema_ref=ohe.schema_ref, object_name=ohe.object_name)
        elif ohe.operation=="added":
            self.delete(schema_ref=ohe.schema_ref, object_name=ohe.object_name)
        # remove the operation history entry after successfully undoing the operation
        self._operation_history.pop()

    def undo_all(self):
        """Undo all CRUD operations in self._operation_history."""
        while len(self._operation_history) > 0:
            self.undo()

    def clear_operation_history(self):
        """Clear the history of CRUD operations."""
        self._operation_history = []

    def purge(self, time_threshold=None):
        """Purge (permanently delete) objects marked for deletion."""
        self._dao.purge(time_threshold)

    def _validate(self, obj):
        """Validate a single object prior to adding it into the repository."""
        return obj



