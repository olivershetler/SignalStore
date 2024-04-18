from abc import ABC, abstractmethod

import xarray as xr

class AbstractDataFileAdapter(ABC):

    def __init__(self, filesystem=None):
        self.filesystem = filesystem

    def set_filesystem(self, filesystem):
        self.filesystem = filesystem

    @abstractmethod
    def read_file(self, path):
        pass

    @abstractmethod
    def write_file(self, path, data_object):
        pass

    @abstractmethod
    def get_id_kwargs(self, data_object):
        pass

    @property
    @abstractmethod
    def file_extension(self):
        pass

    @property
    @abstractmethod
    def file_format(self):
        pass

    @property
    @abstractmethod
    def data_object_type(self):
        pass

class XarrayDataArrayNetCDFAdapter(AbstractDataFileAdapter):
    """Adapter for reading and writing xarray DataArrays to netcdf files."""

    @property
    def file_extension(self):
        return ".nc"

    @property
    def file_format(self):
        return "NetCDF"

    @property
    def data_object_type(self):
        return type(xr.DataArray())

    def get_id_kwargs(self, data_object):
        return {"schema_ref": data_object.attrs.get("schema_ref"),
                "data_name": data_object.attrs.get("data_name"),
                "version_timestamp": data_object.attrs.get("version_timestamp")
                }

    def read_file(self, path):
        with self.filesystem.open(path, mode='rb') as f:
            data_object = xr.open_dataarray(f, engine="scipy")
        return data_object

    def write_file(self, path, data_object):
        # make file if it doesn't exist
        data_object = self._clean_attributes(data_object)
        with self.filesystem.open(path, mode='wb') as f:
            data_object.to_netcdf(f, engine="scipy")

    def _clean_attributes(self, data_object):
        """Clean up attributes to ensure they are serializable to netcdf."""
        # clean name
        if data_object.name is None:
            schema_ref = data_object.attrs.get("schema_ref")
            data_name = data_object.attrs.get("data_name")
            data_object.name = f"{schema_ref}__{data_name}"
        # make sure dims are strings
        data_object = data_object.rename({k: str(k) for k in data_object.dims})
        # make sure attrs are strings
        data_object.attrs = {str(k): str(v) for k, v in data_object.attrs.items()}
        return data_object



class XarrayDataArrayZarrAdapter(AbstractDataFileAdapter):
    """Adapter for reading and writing xarray DataArrays to zarr files."""

    @property
    def file_extension(self):
        return ""

    @property
    def file_format(self):
        return "Zarr"

    @property
    def data_object_type(self):
        return type(xr.DataArray())

    def get_id_kwargs(self, data_object):
        return {"schema_ref": data_object.attrs.get("schema_ref"),
                "data_name": data_object.attrs.get("data_name"),
                "version_timestamp": data_object.attrs.get("version_timestamp")
                }

    def read_file(self, path):
        store = self.filesystem.get_mapper(path)
        data_object = xr.open_dataarray(store, engine="zarr")
        return data_object

    def write_file(self, path, data_object):
        # make zarr dir if it doesn't exist
        store = self.filesystem.get_mapper(path)
        data_object = self._clean_attributes(data_object)
        data_object.to_zarr(store, consolidated=True)
        return data_object

    def _clean_attributes(self, data_object):
        """Clean up attributes to ensure they are serializable to netcdf."""
        # clean name
        if data_object.name is None:
            schema_ref = data_object.attrs.get("schema_ref")
            data_name = data_object.attrs.get("data_name")
            data_object.name = f"{schema_ref}__{data_name}"
        # make sure dims are strings
        data_object = data_object.rename({k: str(k) for k in data_object.dims})
        # make sure attrs are strings
        data_object.attrs = {str(k): str(v) for k, v in data_object.attrs.items()}
        return data_object