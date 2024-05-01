import xarray as xr
import numpy as np
import copy
from datetime import datetime, timezone
from signalstore.utilities.testing.helper_mocks import *

def record_with_timestamps(record_dict):
    record_dict_cpy = copy.deepcopy(record_dict)
    record_dict_cpy.update({"time_of_save": time_old.timestamp(), "time_of_removal": None})
    return record_dict_cpy

# DataArrays

spike_waveforms_dataarray = xr.DataArray(
    name = "test1",
    data = np.zeros(shape=(100,3,5)),
    dims = ["time", "probe", "channel"],
    coords = {
        "time": np.array(range(100)),
        "probe": np.array(range(3)),
        "channel": np.array(range(5))
    },
    attrs = {
        "name": "test1",
        "schema_ref": "spike_waveforms",
        "animal_data_ref": "A10",
        "session_data_ref": "20230810-A10-box-0"
    }
)

spike_labels_dataarray = xr.DataArray(
    name = "test2",
    data = np.zeros(shape=(100,1)),
    dims = ["time", "label"],
    coords = {
        "time": np.array(range(100)),
    },
    attrs = {
        "name": "test2",
        "schema_ref": "spike_labels",
        "animal_data_ref": "A10",
        "session_data_ref": "20230810-A10-box-0"
    }
)
dataarrays = [spike_waveforms_dataarray, spike_labels_dataarray]


# Datasets

spike_waveforms_dataset = xr.Dataset(
    data_vars = {
        "test1arr": spike_waveforms_dataarray,
        "test2arr": spike_labels_dataarray
    }
)

# Records

session_record = {
    "name": "test0",
    "schema_ref": "session",
    "has_file": False,
    "animal_data_ref": "A10",
    "session_date": "2023-08-10",
    "session_time": "12:00:00",
    "session_duration": "00:30:00",
    "session_notes": "This is a test session"
}

spike_waveforms_record = {
    "name": "test1",
    "schema_ref": "spike_waveforms",
    "has_file": True,
    "data_dimensions": ["time", "probe", "channel"],
    "coordinates": ["time", "probe", "channel"],
    "shape": [100, 3, 5],
    "dtype": "float64",
    "unit_of_measure": "microvolts",
    "dimension_of_measure": "charge",
    "animal_data_ref": "A10",
    "session_data_ref": "20230810-A10-box-0"
}

spike_labels_record = {
    "name": "test2",
    "schema_ref": "spike_labels",
    "has_file": True,
    "data_dimensions": ["time", "label"],
    "coordinates": ["time"],
    "shape": [100, 1],
    "dtype": "int64",
    "unit_of_measure": "neurons",
    "dimension_of_measure": "nominal",
    "animal_data_ref": "A10",
    "session_data_ref": "20230810-A10-box-0"
}

records = [spike_waveforms_record, spike_labels_record, session_record]

# Schemata

xarray_schema = {
    "schema_ref": "xarray",
    "schema_description": "A record of an xarray DataArray object",
    "json_schema": {
        "type": "object",
        "properties": {
            "data_dimensions": {
                "type": "array"
            },
            "coordinates": {
                "type": "array"
            }
        },
        "required": [
            "data_dimensions"
        ]
    },
}

units_schema = {
    "schema_ref": "units",
    "schema_description": "A record of an xarray DataArray object",
    "json_schema": {
        "type": "object",
        "properties": {
            "unit_of_measure": {
                "type": "string",
            },
            "dimension_of_measure": {
                "type": "array",
                "items": {
                    "type": "string",
                }
            }
        }
    },
}


base_record_schema = {
    "schema_ref": "base_record",
    "schema_description": "A record of a measurement",
    "json_schema": {
        "type": "object",
        "properties": {
                "name": {
                    "type": "string",
                },
                "schema_ref": {
                    "type": "string",
                },
                "has_file": {
                    "type": "boolean"
                }
        },
        "required": ["name", "schema_ref"]
    },
}

data_record_schema = {
    "schema_ref": "data_record",
    "schema_description": "A record of a measurement",
    "json_schema": {
        "type": "object",
        "allOf": [
            {
                "_schema_ref": "base_record"
            },
            {
                "_schema_ref": "xarray"
            },
            {
                "_schema_ref": "units"
            },
            {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "is_acquisition": { "const": True },
                            "acquisition_description": { "type": "string" },
                            "acquisition_brand": { "type": "string" },
                            "acquisition_settings_name": { "type": "string" }
                        },
                        "required": ["is_acquisition", "acquisition_description", "acquisition_brand", "acquisition_settings_name"],
                        "additionalProperties": True
                    },
                    {
                        "type": "object",
                        "properties": {
                            "is_acquisition": { "const": False },
                            "creation_report_key": { "type": "string" }
                        },
                        "required": ["is_acquisition", "creation_report"],
                        "additionalProperties": True
                    }
                ]
            }
        ]
    },
}

pure_record_schema = {
    "schema_ref": "pure_record",
    "schema_description": "A record of a measurement",
    "json_schema": {
        "type": "object",
        "allOf": [
            {
                "_schema_ref": "base_record"
            },
            {
                "type": "object",
                "properties": {
                    "has_file": { "const": False },
                },
                "required": ["has_file"]
            }
        ]
    },
}

session_schema = {
    "schema_ref": "session",
    "schema_description": "A record of a recording session, where multiple data recordings may have been taken. A session includes metadata about the session such as the date, time, duration, foriegn keys to records of things used and notes.",
    "json_schema": {
        "allOf": [
            {'schema_ref': 'pure_record'},
            {
                "type": "object",
                "properties": {
                    "animal_data_ref": {
                        "type": "string",
                    },
                    "session_date": {
                        "type": "string",
                    },
                    "start_time": {
                        "type": "string",
                    },
                    "session_duration": {
                        "type": "string",
                    },
                    "session_notes": {
                        "type": "string",
                    }
                },
                "required": ["session_date", "start_time", "session_duration"]
            }
        ]
    },
}

animal_schema = {
    "schema_ref": "animal",
    "schema_description": "A record of an animal",
    "json_schema": {
        "allOf": [
            {'schema_ref': 'pure_record'},
            {
                "type": "object",
                "properties": {
                    "sex": {
                        "type": "string",
                    },
                    "species": {
                        "type": "string",
                    },
                    "strain": {
                        "type": "string",
                    },
                    "genotype": {
                        "type": "string",
                    },
                    "age": {
                        "type": "numeric",
                    },
                    "age_unit": {
                        "type": "string",
                    },
                    "weight": {
                        "type": "numeric",
                    },
                    "weight_unit": {
                        "type": "string",
                    },
                    "animal_notes": {
                        "type": "string",
                    }
                },
                # species and strain are required
                "required": ["species", "strain"],
                # if age is present, age_unit must be present
                # if weight is present, weight_unit must be present
                "if": {
                    "oneOf": [
                        {"properties": {"age": {"type": "numeric"}}},
                        {"properties": {"age_unit": {"type": "string"}}}
                    ]
                },
                "then": {
                    "required": ["age", "age_unit"]
                },
                "if": {
                    "oneOf": [
                        {"properties": {"weight": {"type": "numeric"}}},
                        {"properties": {"weight_unit": {"type": "string"}}}
                    ]
                },
                "then": {
                    "required": ["weight", "weight_unit"]
                }
            }
        ]
    },
}



spike_waveforms_schema = {
    "schema_ref": "spike_waveforms",
    "schema_description": "A record of an xarray DataArray object",
    "json_schema": {
        "type": "object",
        "allOf": [
            {"_schema_ref": "data_record"},
            {"type": "object",
            "properties": {
                # dimensions should also have a specific order to them
                "data_dimensions": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["time", "probe", "channel"]
                    },
                    "minItems": 3,
                    "maxItems": 3,
                    "uniqueItems": True
                },
                "coordinates": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["time", "probe", "channel"]
                },
                "uniqueItems": True
                },
                "unit": {
                    "type": "string"
                },
                "dimension_of_measure": {
                    "type": "string",
                    "const": "charge"
                },
                "animal_data_ref": {
                    "type": "string"
                },
                "session_data_ref": {
                    "type": "string"
                }
            },
        "required": ["data_dimensions", "coordinates", "unit", "dimension_of_measure", "animal_data_ref", "session_data_ref"]
            },
        ]
    },
}
schemata = [spike_waveforms_schema, xarray_schema, units_schema, base_record_schema, data_record_schema, pure_record_schema, session_schema, animal_schema]

# Vocabulary terms
name_term = {
    "name": "name",
    "title": "Human Readable Identifier",
    "description": "A unique record or object identifier that is intended to be human readable."
}
schema_name_term = {
    "name": "schema_ref",
    "title": "Schema Name",
    "description": "The name of a schema used to identify the type of record or data object. Also used as a unique identifier for schemas when they are loaded into the database and queried."
}
json_schema_term = {
    "name": "json_schema",
    "title": "Schema Body",
    "description": "The body of a schema. This is a JSON Schema that describes the structure of a record or data object."
}
schema_description_term = {
    "name": "schema_description",
    "title": "Schema Description",
    "description": "A description of a schema. This is a string that describes the purpose of a schema."
}
has_file_term = {
    "name": "has_file",
    "title": "Has Data",
    "description": "A boolean term (true or false) that says whether or not a record has data associated with it. If a record has data, then it is a data record. If a record does not have data, then it is a metadata record."
}
unit_of_measure_term = {
    "name": "unit_of_measure",
    "title": "Unit of Measure",
    "description": "A unit of measure says what a quantity is counting or measuring. Most units of measure are SI Units such as centimeters, volts, ect. However, in the context of this domain, there may be units of measure such as neurons (nominal scale), number of spikes (integer scale) or any other number of specific unit types."
}
dimension_of_measure_term = {
    "name": "dimension_of_measure",
    "title": "Dimension of Measure",
    "description": "A Dimension of Measure refers to the dimensional property of a Unit of Measure. For example, if my unit of measure is centimeters then my dimension of measure is length. Likewise for seconds and time. More exotic examples exist as well. Spikes have the dimension of measure count and neurons have the dimension of measure nominal (nominal refers to a category or label)."
}
acquisition_term = {
    "name": "acquisition",
    "title": "Acquisition",
    "description": "An acquisition is a boolearn term (true or false) that says whether or not a data object was acquired from a source outside of the data analysis process. All of the data objects read from external data are flagged as being acquisitions. Readers never do preprocessing so that they may reflect the exact numerical values from the original data source."
}
acquisition_date_term = {
    "name": "acquisition_date",
    "title": "Acquisition Date",
    "description": "The date when an acquisition was originally produced (usually taken from the metadata of a recording file.)"
}
import_date_term = {
    "name": "import_date",
    "title": "Import Date",
    "description": "The date when an acquisition (an imported data set) was imported."
}
acquisition_notes_term = {
    "name": "acquisition_notes",
    "title": "Acquisition Notes",
    "description": "Notes on the progeny of an acquisition. This field is usually automatically populated with an explanation of what the acquisition is by an import adapter within signalstore."
}
dimensions_term = {
    "name": "data_dimensions",
    "title": "data_dimensions",
    "description": "The named dimensions of a data object. This is a list of strings. It lists the dimension names that would go into the .dims attribute of an xarray DataArray."
}
coordinates_term = {
    "name": "coordinates",
    "title": "Coordinates",
    "description": "The named coordinates of a data object. This is a list of strings that is a subset or equal to the dimensions of the data object. It lists the coordinate names that would go into the .coords attribute of an xarray DataArray."
}
shape_term = {
    "name": "shape",
    "title": "Shape",
    "description": "The shape of a data object. This is a list of integers that is equal to the shape of the data object. It lists the shape that would go into the .shape attribute of an xarray DataArray."
}
dtype_term = {
    "name": "dtype",
    "title": "Data Type",
    "description": "The data type of a data object. This is a string that is equal to the dtype of the data object. It lists the dtype that would go into the .dtype attribute of an xarray DataArray."
}
session_date = {
    "name": "session_date",
    "title": "Session Date",
    "description": "The date when a session was taken."
}
session_time = {
    "name": "session_time",
    "title": "Session Time",
    "description": "The time when a session was started."
}
session_duration = {
    "name": "session_duration",
    "title": "Session Duration",
    "description": "The duration of a session."
}
session_notes = {
    "name": "session_notes",
    "title": "Session Notes",
    "description": "Notes about a session."
}
time_of_removal = {
    "name": "time_of_removal",
    "title": "Time of Deletion",
    "description": "The timestamp at which the record was deleted."
}
time_of_save = {
    "name": "time_of_save",
    "title": "Time of Creation",
    "description": "The timestamp at which the record was created."
}
vocabulary_terms = [name_term, schema_name_term, json_schema_term, schema_description_term, has_file_term, unit_of_measure_term, dimension_of_measure_term, acquisition_term, acquisition_date_term, import_date_term, acquisition_notes_term, dimensions_term, coordinates_term, shape_term, dtype_term, session_date, session_time, session_duration, session_notes, time_of_save, time_of_removal]

# Time

class MockDatetime:
    """
    This class is a dropin replacement for the entire `datetime` module, to be used in tests. This
    allows us to have a predictable value for `now`.
    """

    def __init__(self, now_time):
        self._now_time = now_time

    def now(self, *args, **kwargs):
        return self._now_time

time_newest = datetime(2023, 5, 1, 1, 1, 1, 1)
time_default = datetime(2023, 1, 1, 1, 1, 1, 1)
time_old = datetime(1990, 1, 1, 1, 1, 1, 1)
time_older = datetime(1975, 1, 1, 1, 1, 1, 1)

# Handlers

class MockHandler:
    def execute(self, *args, **kwargs):
        pass

class MockDataHandlerFactory:

    def create(self, handler_name, service_bundle=None, **kwargs):
        return MockHandler()

class MockDomainHandlerFactory:

    def create(self, handler_name, service_bundle=None, **kwargs):
        return MockHandler()


