

# README

SignalStore is a Python package for storing and retrieving large labeled data arrays using MongoDB and xarray compatible formats such as netCDF4 and Zarr. It also has a framework for accomodating data adapters for arbitrary formats such as PyTorch model weights, NumPy arrays, and pandas DataFrames. It is designed to be used as a cloud-agnostic common library for dockerized data analysis micro-services. To use the package, you need a MongoDB client and a fsspec FileSystem object (compatible with all local filesystems, Google Cloud, Amazon Web Services, etc.).

## Cite

[![DOI](https://zenodo.org/badge/788602046.svg)](https://doi.org/10.5281/zenodo.13958872)

## Installation

Provisional pre-release installation:

To install the pre-release, clone this repository into your project directory and use `pip install -e .`.

To update the package, cd into the sub-repository and use `git pull`, then cd into the main repository and `pip install -e .` again.

## Usage

The main class in SignalStore is the UnitOfWork. You use the UnitOfWorkProvider to create instances of the UnitOfWork. The UnitOfWork is used to store and retrieve data in a safe manner.

```
from signalstore import UnitOfWorkProvider
import fsspec
from pymongo import MongoClient

og_filesystem = fsspec.filesystem('file')
root = '.'
filesystem = fsspec.DirFileSystem(root, og_filesystem)
client = MongoClient('localhost', 27017)

memory_store = dict()


uow_provider = UnitOfWorkProvider(
    mongo_client=client,
    filesystem=filesystem,
    memory_store=memorystore
)

input_dir = 'path/to/my/input/data

with uow_provider('myproject') as uow:
    data_glob = f'{input_dir}/*.nc'
    for data_file in filesystem.glob(data_glob):
        dataarray = xr.open_dataarray(data_file)
        uow.data.add(dataarray)
    # before the commit, everything will be rolled back if an exception is raised
    uow.commit()

with uow_provider('myproject') as uow:
    session_ref = {
        'schema_ref': 'session',
        'data_name': '2024-04-28-AM-Animal1'
            }
    query = {'session_ref': session_ref}
    metadata = uow.data.find(query) # find all data from specified recording session
    # load the data-arrays from found meta-data
    for record in metadata:
        dataarray = uow.data.get(
            schema_ref=record['schema_ref'],
            data_name=record['data_name'],
        )
        print(dataarray)
```

# Caveats

1D data (e.g. spike labels or spike times) have to be saved as 2D with an extra dimension e.g. (index, 1)
This is because of the xarray function "is_list_of_strings" that requires the extra dimension
1D data will be encoded as 2D with the extra dimension termed "1"

MongoDB stores datetime objects as UTC, so when you query for a datetime object, you need to convert it to UTC first.

MongoDB stores datetime objects at millisecond precision. You can use a more precise datetime object for queries, but it will be truncated to millisecond precision. You will not get an exact match if you assert that the original datetime object is equal to the one stored in the database. For speed, the filesystem stores time_of_removal and version_timestamp as microsecond precision integers in filenames, but the metadata in MongoDB is stored as millisecond precision datetime objects. Use full precision for queries to get the right results, but beware that adding to MongoDB and getting back from MongoDB will truncate the precision to milliseconds.
