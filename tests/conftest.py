import pytest
import yaml
import json
import upath
import mongomock
import xarray as xr
import numpy as np
import fsspec
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from upath import UPath
from time import sleep

from signalstore.store.data_access_objects import (
    MongoDAO,
    FileSystemDAO,
    InMemoryObjectDAO,
    datetime_to_microseconds,
    microseconds_to_datetime,
)

from signalstore.store.repositories import (
    DomainModelRepository, domain_model_json_schema,
    DataRepository,
    InMemoryObjectRepository,
)

from signalstore.store.datafile_adapters import (
    XarrayDataArrayNetCDFAdapter,
    XarrayDataArrayZarrAdapter,
    AbstractDataFileAdapter,
)

from signalstore.store import UnitOfWorkProvider

from signalstore.operations.helpers.abstract_helper import AbstractMutableHelper

from datetime import datetime, timezone, timedelta




# ==========================================================
# ==========================================================
# Utility Fixtures
# ==========================================================
# ==========================================================

@pytest.fixture
def timestamp():
    return datetime.now(tz=timezone.utc)

@pytest.fixture
def project():
    return "testproject"

# ==========================================================
# ==========================================================
# Client
# ==========================================================
# ==========================================================

@pytest.fixture
def empty_client():
    return mongomock.MongoClient()

# ==========================================================
# ==========================================================
# Data Access Objects
# ==========================================================
# ==========================================================

test_data_path = upath.UPath(__file__).parent / "data" / "valid_data"
invalid_test_data_path = upath.UPath(__file__).parent / "data" / "invalid_data"

# ===================
# Model MongoDAO
# ===================

# Property Models
# -------------------
# Load the JSON file into a list of dictionaries
property_models_path =  test_data_path / "models" / "property_models.json"
with open(property_models_path, 'r') as file:
    raw_property_models = json.load(file)

# generate fixtures that provide individual property models
for property_model in raw_property_models:
    schema_name = property_model["schema_name"]
    namestring = f"{schema_name}_property_model"
    @pytest.fixture(name=namestring)
    def _property_model_fixture(timestamp, property_model=property_model):
        property_model["time_of_save"] = timestamp
        property_model["time_of_removal"] = None
        return property_model
    globals()[namestring] = _property_model_fixture

# generate a fixture that provides all the property models
@pytest.fixture(name="property_models")
def _property_models_fixture(timestamp):
    property_models = []
    for property_model in raw_property_models:
        property_model = property_model.copy()
        property_model["time_of_save"] = timestamp
        property_model["time_of_removal"] = None
        property_models.append(property_model)
    return property_models

# Metamodels
# -------------------
# Load the JSON files from folder into a list of dictionaries
metamodels_dir =  test_data_path / "models" / "metamodels"
metamodel_filepaths = list(metamodels_dir.glob("*.json"))
raw_metamodels = []
for filepath in metamodel_filepaths:
    with open(filepath, 'r') as file:
        raw_metamodels.append(json.load(file))

# generate fixtures that provide individual metamodels
for metamodel in raw_metamodels:
    schema_name = metamodel["schema_name"]
    namestring = f"{schema_name}_metamodel"
    @pytest.fixture(name=namestring)
    def _metamodel_fixture(timestamp, metamodel=metamodel):
        metamodel["time_of_save"] = timestamp
        metamodel["time_of_removal"] = None
        return metamodel
    globals()[namestring] = _metamodel_fixture

# generate a fixture that provides all the metamodels
@pytest.fixture(name="metamodels")
def _metamodels_fixture(timestamp):
    metamodels = []
    for metamodel in raw_metamodels:
        metamodel = metamodel.copy()
        metamodel["time_of_save"] = timestamp
        metamodel["time_of_removal"] = None
        metamodels.append(metamodel)
    return metamodels


# Data Models
# -------------------
# Load the JSON files from folder into a list of dictionaries
data_models_dir =  test_data_path / "models" / "data_models"
data_model_filepaths = list(data_models_dir.glob("*.json"))
raw_data_models = []
for filepath in data_model_filepaths:
    with open(filepath, 'r') as file:
        raw_data_models.append(json.load(file))

# generate fixtures that provide individual data models
for data_model in raw_data_models:
    schema_name = data_model["schema_name"]
    namestring = f"{schema_name}_data_model"
    @pytest.fixture(name=namestring)
    def _data_model_fixture(timestamp, data_model=data_model):
        data_model["time_of_save"] = timestamp
        data_model["time_of_removal"] = None
        return data_model
    globals()[namestring] = _data_model_fixture

# generate a fixture that provides all the data models
@pytest.fixture(name="data_models")
def _data_models_fixture(timestamp):
    data_models = []
    for data_model in raw_data_models:
        data_model = data_model.copy()
        data_model["time_of_save"] = timestamp
        data_model["time_of_removal"] = None
        data_models.append(data_model)
    return data_models

# Invalid Property Models
# -------------------
# Load the JSON file into a list of dictionaries
invalid_property_models_path = invalid_test_data_path / "models" / "property_models.json"
with open(invalid_property_models_path, 'r') as file:
    raw_invalid_property_models = json.load(file)

# generate fixtures that provide individual property models
for property_model in raw_invalid_property_models:
    schema_name = property_model["schema_name"]
    namestring = f"{schema_name}_property_model"
    @pytest.fixture(name=namestring)
    def _invalid_property_model_fixture(timestamp, property_model=property_model):
        property_model["time_of_save"] = timestamp
        property_model["time_of_removal"] = None
        return property_model
    globals()[namestring] = _invalid_property_model_fixture

@pytest.fixture(name="models")
def _models_fixture(timestamp, property_models, metamodels, data_models):
    models = []
    for property_model in property_models:
        property_model = property_model.copy()
        property_model["time_of_save"] = timestamp
        property_model["time_of_removal"] = None
        models.append(property_model)
    for metamodel in metamodels:
        metamodel = metamodel.copy()
        metamodel["time_of_save"] = timestamp
        metamodel["time_of_removal"] = None
        models.append(metamodel)
    for data_model in data_models:
        data_model = data_model.copy()
        data_model["time_of_save"] = timestamp
        data_model["time_of_removal"] = None
        models.append(data_model)
    return models

# generate a fixture that provides all the property models
@pytest.fixture(name="invalid_property_models")
def _invalid_property_models_fixture(timestamp, unit_of_measure_property_model):
    invalid_property_models = []
    for property_model in raw_invalid_property_models:
        property_model = property_model.copy()
        property_model["time_of_save"] = timestamp
        property_model["time_of_removal"] = None
        invalid_property_models.append(property_model)
    # add property models from the valid set that are just missing a required field
    required_fields = ["schema_title", "schema_description", "schema_type", "json_schema"]
    for field in required_fields:
        invalid_property_model = unit_of_measure_property_model.copy()
        invalid_property_model.pop(field)
        invalid_property_model["time_of_save"] = timestamp
        invalid_property_model["time_of_removal"] = None
        # rename the schema_name to make it unique
        invalid_property_model["schema_name"] = f"invalid_property_model_missing_{field}"
        invalid_property_models.append(invalid_property_model)
    return invalid_property_models

# Invalid Data Models
# -------------------
# Load the JSON files from folder into a list of dictionaries
invalid_data_models_dir =  invalid_test_data_path / "models" / "data_models"
invalid_data_model_filepaths = list(invalid_data_models_dir.glob("*.json"))
raw_invalid_data_models = []
for filepath in invalid_data_model_filepaths:
    with open(filepath, 'r') as file:
        raw_invalid_data_models.append(json.load(file))

# generate fixtures that provide individual data models
for data_model in raw_invalid_data_models:
    schema_name = data_model["schema_name"]
    namestring = f"{schema_name}_data_model"
    @pytest.fixture(name=namestring)
    def _invalid_data_model_fixture(timestamp, data_model=data_model):
        data_model["time_of_save"] = timestamp
        data_model["time_of_removal"] = None
        return data_model
    globals()[namestring] = _invalid_data_model_fixture

# generate a fixture that provides all the data models
@pytest.fixture(name="invalid_data_models")
def _invalid_data_models_fixture(timestamp, session_data_model):
    invalid_data_models = []
    for data_model in raw_invalid_data_models:
        data_model = data_model.copy()
        data_model["time_of_save"] = timestamp
        data_model["time_of_removal"] = None
        invalid_data_models.append(data_model)
    # add data models from the valid set that are just missing a required field
    required_fields = ["schema_title", "schema_description", "schema_type", "json_schema"]
    for field in required_fields:
        invalid_data_model = session_data_model.copy()
        invalid_data_model.pop(field)
        invalid_data_model["time_of_save"] = timestamp
        invalid_data_model["time_of_removal"] = None
        # rename the schema_name to make it unique
        invalid_data_model["schema_name"] = f"invalid_{invalid_data_model['schema_name']}_missing_{field}"
        invalid_data_models.append(invalid_data_model)
    return invalid_data_models


# DomainModelDAOs

@pytest.fixture
def populated_domain_model_dao(empty_client, project, property_models, metamodels, data_models, timestamp):
    dao = MongoDAO(empty_client, project, "domain_models", ["schema_name"])
    for property_model in property_models:
        dao.add(property_model, timestamp=timestamp)
    for metamodel in metamodels:
        dao.add(metamodel, timestamp=timestamp)
    for data_model in data_models:
        dao.add(data_model, timestamp=timestamp)
    return dao

@pytest.fixture
def populated_domain_model_dao_with_invalid_models(empty_client, project, property_models, invalid_property_models, metamodels, data_models, invalid_data_models, timestamp):
    dao = MongoDAO(empty_client, project, "domain_models", ["schema_name"])
    for property_model in property_models:
        dao.add(property_model, timestamp=timestamp)
    for metamodel in metamodels:
        dao.add(metamodel, timestamp=timestamp)
    for data_model in data_models:
        dao.add(data_model, timestamp=timestamp)
    for property_model in invalid_property_models:
        dao.add(property_model, timestamp=timestamp)
    # add a property model with an invalid (text) json schema
    invalid_json_schema_property_model = {
        "schema_name": "invalid_json_schema",
        "schema_title": "Invalid JSON Schema Model",
        "schema_description": "A JSON Schema Model with invalid JSON",
        "schema_type": "property_model",
        "json_schema": "invalid json schema",
        "time_of_save": timestamp,
        "time_of_removal": None
    }
    dao._collection.insert_one(invalid_json_schema_property_model)
    for data_model in invalid_data_models:
        dao.add(data_model, timestamp=timestamp)
    return dao

@pytest.fixture
def invalid_model_schema_names(invalid_property_models, invalid_data_models):
    required_fields = ["schema_title", "schema_description", "schema_type", "json_schema"]
    missing_field_names = [f"invalid_property_model_missing_{field}" for field in required_fields]
    other_invalid_property_model_names = [property_model.get("schema_name") for property_model in invalid_property_models if property_model.get("schema_name") is not None]
    invalid_data_model_names = [data_model.get("schema_name") for data_model in invalid_data_models if data_model.get("schema_name") is not None]
    return missing_field_names + other_invalid_property_model_names + invalid_data_model_names

# ===================
# Records
# ===================
# Load the JSON file into a list of dictionaries
# valid records
records_path =  test_data_path / "records.json"
with open(records_path, 'r') as file:
    raw_records = json.load(file)

# invalid records
invalid_records_path = invalid_test_data_path / "records.json"
with open(invalid_records_path, 'r') as file:
    raw_invalid_records = json.load(file)

# generate fixtures that provide individual records
for record in raw_records:
    schema_ref, data_name = record["schema_ref"], record["data_name"]
    namestring = f"{schema_ref}_{data_name}_record"
    @pytest.fixture(name=namestring)
    def _record_fixture(timestamp, record=record):
        record["time_of_save"] = datetime_to_microseconds(timestamp)
        record["time_of_removal"] = None
        return record
    globals()[namestring] = _record_fixture

@pytest.fixture(name="test_record")
def _test_record_fixture(timestamp):
    record = {"schema_ref": "test_schema", "name": "test_name", "key1": "value1", "key2": "value2"}
    record["time_of_save"] = datetime_to_microseconds(timestamp)
    record["time_of_removal"] = None
    return record

# generate a fixture that provides all the records
@pytest.fixture(name="records")
def _records_fixture(timestamp):
    records = []
    for record in raw_records:
        record = record.copy()
        record["time_of_save"] = datetime_to_microseconds(timestamp)
        record["time_of_removal"] = None
        records.append(record)
    return records

# generate a fixture that provides all the invalid records
@pytest.fixture(name="invalid_records")
def _invalid_records_fixture(timestamp):
    invalid_records = []
    for record in raw_invalid_records:
        record = record.copy()
        record["time_of_save"] = datetime_to_microseconds(timestamp)
        record["time_of_removal"] = None
        invalid_records.append(record)
    return invalid_records

def record_dao_factory(client, project):
    return MongoDAO(client, project, "records", ["schema_ref", "data_name", "version_timestamp"])

@pytest.fixture
def populated_record_dao(empty_client, project, records, timestamp):
    dao = record_dao_factory(empty_client, project)
    for record in records:
        dao.add(record, timestamp=timestamp, versioning_on=False)
    return dao

@pytest.fixture
def populated_record_dao_with_invalid_records(empty_client, project, records, invalid_records, timestamp):
    dao = record_dao_factory(empty_client, project)
    for record in records:
        dao.add(record, timestamp=timestamp, versioning_on=False)
    for record in invalid_records:
        dao.add(record, timestamp=timestamp, versioning_on=False)
    return dao

# ===================
# FileSystemDAO
# ===================
# Load NetCDF files into list of xarray DataArray objects

def deserialize_dataarray(data_object):
        """Deserializes a data object.
        Arguments:
            data_object {dict} -- The data object to deserialize.
        Returns:
            dict -- The deserialized data object.
        """
        attrs = data_object.attrs.copy()
        for key, value in attrs.items():
            if isinstance(value, str):
                if value == 'True':
                    attrs[key] = True
                elif value == 'False':
                    attrs[key] = False
                elif value == 'None':
                    attrs[key] = None
                elif value.startswith('{'):
                    attrs[key] = json.loads(value)
            if isinstance(value, np.ndarray):
                attrs[key] = value.tolist()
        data_object.attrs = attrs
        return data_object

netcdf_dir = test_data_path / "data_arrays"
netcdf_files = list(netcdf_dir.glob("*.nc"))
dataarrays = []
for filepath in netcdf_files:
    dataarray = xr.open_dataarray(filepath)
    dataarray = deserialize_dataarray(dataarray)
    dataarrays.append(dataarray)

@pytest.fixture(name="dataarrays")
def _dataarrays_fixture():
    return dataarrays

# make a fixture that provides a single dataarray for each file of the form {schema_ref}_{name}_dataarray
for dataarray in dataarrays:
    try:
        schema_ref, data_name = dataarray.attrs["schema_ref"], dataarray.attrs["data_name"]
    except:
        raise ValueError(f"DataArray.attrs {dataarray.attrs} does not have schema_ref and data_name attributes")
    namestring = f"{schema_ref}__{data_name}__dataarray"
    @pytest.fixture(name=namestring)
    def _dataarray_fixture(dataarray=dataarray):
        return dataarray

    globals()[namestring] = _dataarray_fixture

# make a fixture that provides a new dataarray with a different name
@pytest.fixture(name="test_dataarray")
def _test_dataarray_fixture(timestamp):
    dataarray = xr.DataArray([[1, 2, 3], [4, 5, 6]], dims=("x", "y"), coords={"x": [10, 20]})
    dataarray.attrs["schema_ref"] = "test"
    dataarray.attrs["data_name"] = "test"
    dataarray.attrs["version_timestamp"] = 0
    dataarray.attrs["time_of_save"] = datetime_to_microseconds(timestamp)
    dataarray.attrs["time_of_removal"] = ""
    return dataarray


class MockMutableModelHelper(AbstractMutableHelper):
    def __init__(self, schema_ref: str, data_name: str, version_timestamp: datetime=None):
        state = np.array([0])
        attrs = {"schema_ref": schema_ref,
                "data_name": data_name,
                "version_timestamp": version_timestamp
                }
        super().__init__(attrs=attrs, state=state)

    def train(self, iterations: int=1):
        # add a number to the state
        for i in range(iterations):
            self.state = np.append(self.state, self.state[-1] + 1)

class MockMutableModelNumpyAdapter(AbstractDataFileAdapter):

    def read_file(self, path):
        with self.filesystem.open(path, mode='rb') as f:
            idkwargs = self.path_to_id_kwargs(path)
            helper = MockMutableModelHelper(**idkwargs)
            helper.state = np.load(f)
        return helper

    def write_file(self, path, data_object):
        with self.filesystem.open(path, mode='wb') as f:
            np.save(f, data_object.state)

    def get_id_kwargs(self, data_object):
        return {"schema_ref": data_object.attrs.get("schema_ref"),
                "data_name": data_object.attrs.get("data_name"),
                "version_timestamp": data_object.attrs.get("version_timestamp") or 0
                }

    def path_to_id_kwargs(self, path):
        base_name = upath.UPath(path).stem
        schema_ref, data_name, version_string = base_name.split("__")
        version_timestamp = int(version_string.split("_")[1])
        return {"schema_ref": schema_ref,
                "data_name": data_name,
                "version_timestamp": microseconds_to_datetime(version_timestamp)
                }

    @property
    def file_extension(self):
        return ".npy"

    @property
    def file_format(self):
        return "Numpy"

    @property
    def data_object_type(self):
        return type(MockMutableModelHelper("", ""))


# make a fixture that provides a MutableModelHelper object
@pytest.fixture(name="mutable_model_helper")
def _mutable_model_helper_fixture():
    return MockMutableModelHelper(schema_ref="test", data_name="test")

@pytest.fixture(name="xarray_netcdf_adapter")
def _default_data_adapter_fixture():
    return XarrayDataArrayNetCDFAdapter()

@pytest.fixture(name="xarray_zarr_adapter")
def _zarr_data_adapter_fixture():
    return XarrayDataArrayZarrAdapter()

@pytest.fixture(name="model_numpy_adapter")
def _numpy_model_adapter_fixture():
    return MockMutableModelNumpyAdapter()


# make a fixture that provides a populated file DAO
@pytest.fixture(name="populated_netcdf_file_dao")
def _populated_netcdf_file_dao_fixture(tmpdir, xarray_netcdf_adapter, dataarrays):
    project_dir = str(tmpdir) + "/netcdf"
    filesystem = LocalFileSystem(root=str(project_dir))
    dao = FileSystemDAO(filesystem=filesystem,
                        project_dir=project_dir,
                        default_data_adapter=xarray_netcdf_adapter)
    for dataarray in dataarrays:
        dao.add(data_object=dataarray)
    #TODO: add versioned objects with different data adapters
    return dao

@pytest.fixture(name="populated_zarr_file_dao")
def _populated_zarr_file_dao_fixture(tmpdir, xarray_zarr_adapter, dataarrays):
    project_dir = str(tmpdir) + "/zarr"
    filesystem = LocalFileSystem(root=str(project_dir))
    dao = FileSystemDAO(filesystem=filesystem,
                        project_dir=project_dir,
                        default_data_adapter=xarray_zarr_adapter)
    for dataarray in dataarrays:
        dao.add(data_object=dataarray)
    #TODO: add versioned objects with different data adapters
    return dao

@pytest.fixture(name="populated_numpy_file_dao")
def _populated_numpy_file_dao_fixture(tmpdir, model_numpy_adapter, mutable_model_helper, timestamp):
    project_dir = str(tmpdir) + "/numpy"
    filesystem = LocalFileSystem(root=str(project_dir))
    dao = FileSystemDAO(filesystem = filesystem,
                        project_dir=project_dir,
                        default_data_adapter=model_numpy_adapter)
    for i in range(1, 11):
        mutable_model_helper.attrs["version_timestamp"] = timestamp + timedelta(seconds=i)
        mutable_model_helper.train(i)
        data_name = mutable_model_helper.attrs["data_name"]
        schema_ref = mutable_model_helper.attrs["schema_ref"]
        vts = mutable_model_helper.attrs["version_timestamp"]
        if not dao.exists(data_name=data_name, schema_ref=schema_ref, version_timestamp=vts):
            dao.add(data_object=mutable_model_helper)
    return dao

@pytest.fixture(name="populated_file_dao")
def _populated_file_dao_fixture(tmpdir, dataarrays, xarray_netcdf_adapter, model_numpy_adapter, mutable_model_helper, timestamp):
    project_dir = str(tmpdir)
    filesystem = LocalFileSystem(root=str(project_dir))
    dao = FileSystemDAO(filesystem=filesystem,
                        project_dir=project_dir,
                        default_data_adapter=xarray_netcdf_adapter)
    for dataarray in dataarrays:
        deserialize_dataarray(dataarray)
        dao.add(data_object=dataarray)
    for i in range(1, 11):
        mutable_model_helper.attrs["version_timestamp"] = timestamp + timedelta(seconds=i)
        mutable_model_helper.train(i)
        dao.add(data_object=mutable_model_helper,  data_adapter=model_numpy_adapter)
    return dao

@pytest.fixture(name="file_dao_options")
def _file_dao_options_fixture(populated_netcdf_file_dao, populated_zarr_file_dao, populated_numpy_file_dao):
    return {
        "netcdf": populated_netcdf_file_dao,
        "zarr": populated_zarr_file_dao,
        "numpy": populated_numpy_file_dao
    }

@pytest.fixture(name="data_adapter_options")
def _data_adapter_options_fixture(xarray_netcdf_adapter, xarray_zarr_adapter, model_numpy_adapter):
    return {
        "netcdf": xarray_netcdf_adapter,
        "zarr": xarray_zarr_adapter,
        "numpy": model_numpy_adapter
    }

@pytest.fixture(name="new_object_options")
def _new_object_options_fixture(timestamp):
    xarray = xr.DataArray([[1, 2, 3], [4, 5, 6]], dims=("x", "y"), coords={"x": [10, 20]}, attrs={"schema_ref": "new", "data_name": "new", "version_timestamp": None})
    model = MockMutableModelHelper(schema_ref="new", data_name="new", version_timestamp=timestamp)
    return {
        "netcdf": xarray,
        "zarr": xarray,
        "numpy": model
    }


# ===================
# InMemoryObjectDAO
# ===================

# make a fixture that provides a bunch of objects of varying types to be stored in memory

objects = [{"key1": "value1", "key2": "value2"},
           [1, 2, 3],
           "string",
            True,
            1,
            1.0,
            np.array([1, 2, 3]),
            xr.DataArray([[1, 2, 3], [4, 5, 6]], dims=("x", "y"), coords={"x": [10, 20]}),
]

@pytest.fixture(name="objects")
def _objects_fixture():
    return objects

def make_typestring(object):
    return type(object).__name__.lower()

# make fixtures for each object type
for object in objects:
    typestring = make_typestring(object)
    namestring = typestring + "_object"
    @pytest.fixture(name=namestring)
    def _object_fixture(object=object):
        return object
    globals()[namestring] = _object_fixture

# make a populated in memory object DAO
@pytest.fixture
def populated_memory_dao(objects):
    dao = InMemoryObjectDAO(dict())
    for object in objects:
        typestring = make_typestring(object)
        dao.add(object, 'test_' + typestring )
    return dao

@pytest.fixture
def test_object():
    return np.zeros((3, 3))


# ==========================================================
# ==========================================================
# Repositories
# ==========================================================
# ==========================================================

# ===================
# DomainModelRepository
# ===================

@pytest.fixture
def populated_domain_repo(populated_domain_model_dao_with_invalid_models):
    domain_repo = DomainModelRepository(model_dao=populated_domain_model_dao_with_invalid_models,
                                 model_metaschema=domain_model_json_schema)
    return domain_repo

@pytest.fixture
def populated_valid_only_domain_repo(populated_domain_model_dao):
    domain_repo = DomainModelRepository(model_dao=populated_domain_model_dao,
                                 model_metaschema=domain_model_json_schema)
    return domain_repo

@pytest.fixture
def empty_domain_repo(empty_client, project):
    dao = MongoDAO(empty_client, project, "domain_models", ["schema_name"])
    domain_repo = DomainModelRepository(model_dao=dao,
                                    model_metaschema=domain_model_json_schema)
    return domain_repo


# ===================
# DataRepository
# ===================
@pytest.fixture
def populated_data_repo(populated_domain_repo, populated_record_dao, populated_file_dao, mutable_model_helper, model_numpy_adapter, timestamp):
    data_repo = DataRepository(record_dao=populated_record_dao,
                          file_dao=populated_file_dao,
                          domain_repo=populated_domain_repo)
    for i in range(1, 11):
        mutable_model_helper.attrs["version_timestamp"] = timestamp + timedelta(seconds=i)
        mutable_model_helper.attrs["schema_ref"] = "numpy_test"
        mutable_model_helper.attrs["data_name"] = "numpy_test"
        mutable_model_helper.attrs["has_file"] = True
        mutable_model_helper.train(i)
        data_repo.add(object=mutable_model_helper, data_adapter=model_numpy_adapter, versioning_on=True)
    data_repo.clear_operation_history()


    return data_repo

@pytest.fixture
def populated_data_repo_with_invalid_records(populated_domain_repo, populated_record_dao_with_invalid_records, populated_file_dao, mutable_model_helper, model_numpy_adapter, timestamp):
    data_repo = DataRepository(record_dao=populated_record_dao_with_invalid_records,
                          file_dao=populated_file_dao,
                          domain_repo=populated_domain_repo)
    for i in range(1, 11):
        mutable_model_helper.attrs["version_timestamp"] = timestamp + timedelta(seconds=i)
        mutable_model_helper.attrs["schema_ref"] = "numpy_test"
        mutable_model_helper.attrs["data_name"] = "numpy_test"
        mutable_model_helper.attrs["has_file"] = True
        mutable_model_helper.train(i)
        data_repo.add(object=mutable_model_helper, data_adapter=model_numpy_adapter, versioning_on=True)
        sleep(0.001)
    data_repo.clear_operation_history()

    return data_repo



# ===================
# InMemoryObjectRepository
# ===================
@pytest.fixture
def populated_memory_repo(populated_record_repository, populated_memory_dao):
    return InMemoryObjectRepository(populated_record_repository, populated_memory_dao)

# ===================
# Unit of Work
# ===================
@pytest.fixture(name="unit_of_work")
def _unit_of_work_provider_fixture(tmpdir):
    mongo_client = mongomock.MongoClient()
    og_filesystem = LocalFileSystem(root=str(tmpdir))
    filesystem = DirFileSystem(tmpdir, og_filesystem)
    memory_store = dict()
    uow_provider = UnitOfWorkProvider(mongo_client, filesystem, memory_store)
    unit_of_work = uow_provider("testproject")
    with unit_of_work as uow:
        for property_model in raw_property_models:
            uow.domain_models.add(property_model)
        for metamodel in raw_metamodels:
            uow.domain_models.add(metamodel)
        for data_model in raw_data_models:
            uow.domain_models.add(data_model)
        for record in raw_records:
            if not record.get("has_file"):
                uow.data.add(record)
        for dataarray in dataarrays:
            if not dataarray.attrs.get("schema_ref") == "test":
                uow.data.add(dataarray)
        uow.commit()
    return uow



# ==========================================================
# ==========================================================
# Helpers
# ==========================================================
# ==========================================================
# TODO: add helpers

# ==========================================================
# ==========================================================
# Handlers
# ==========================================================
# ==========================================================
# TODO: add handlers

# ==========================================================
# ==========================================================
# Dependency Injection
# ==========================================================
# ==========================================================

# ===================
# Handler Factory
# ===================
# TODO: add handler factory

# ===================
# Project Container
# ===================
# TODO: add project container
