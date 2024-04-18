"""This module generates fake data for testing purposes."""

import numpy as np
import xarray as xr
import json
import pathlib


def main():
    # get records from tests/data/records.json
    data_dir = pathlib.Path(__file__).parent.parent
    records_path = data_dir / "records.json"
    data_arrays_dir = data_dir / "data_arrays"
    with open(records_path) as f:
        records = json.load(f)

    # try making one xarray from scratch
    da = xr.DataArray(
        name="test",
        data=np.random.rand(10, 10),
        dims=("x", "y"),
        attrs={"schema_ref": "test", "data_name": "test"},
    )
    da_path = data_arrays_dir / "test__test.nc"
    da.to_netcdf(da_path)

    # generate fake data
    for record in records:
        if record.get("has_file") == True:
            # make xarray from record, generating numpy random floats to go in a numpy arrray of shape record["shape"]
            array = np.random.rand(*record["shape"])
            dataarray = xr.DataArray(
                name=record["schema_ref"] + "__" + record["data_name"],
                data=array,
                dims=tuple(record["data_dimensions"]),
            )
            # every field other than "shape", "data_dimensions" and "data_name" is an attribute
            for field, value in record.items():
                dataarray.attrs[field] = adapt(value)

            # save xarray to netcdf file
            print(f"Saving {dataarray.name} to {data_arrays_dir}")
            schema_ref, data_name = record["schema_ref"], record["data_name"]
            data_path = data_arrays_dir / f"{schema_ref}__{data_name}.nc"
            dataarray.to_netcdf(data_path)

def adapt(value):
    """Adapt value to be json serializable."""
    # boolians are strings
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, dict):
        return json.dumps(value)
    else:
        return value


if __name__ == "__main__":
    main()