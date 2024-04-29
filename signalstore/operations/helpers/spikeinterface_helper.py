from neo.rawio.mearecrawio import MEArecRawIO
from signalstorestore.data_access_objects import DataArrayDAO, RecordDAO

import numpy as np
import xarray as xr
import neo
import pickle
from datetime import datetime, timezone


class SpikeinterfaceMEArecHelper:

    def __init__(self):
        self._mearec_rawio = None
        self._recordings_dataarray = None
        self._sampling_rate_dataarray = None
        self._recording_info_record = None
        self._spiketrain_records = []

    @property
    def recordings_dataarray(self):
        return self._recordings_dataarray

    @property
    def sampling_rate_dataarray(self):
        return self._sampling_rate_dataarray

    @property
    def recording_info_record(self):
        return self._recording_info_record

    @property
    def spiketrain_records(self):
        return self._spiketrain_records

    def load(self, mearec_rawio: MEArecRawIO, hrid: str):
        self._mearec_rawio = mearec_rawio
        self._mearec_rawio._parse_header()

        self._recordings_dataarray = xr.DataArray(
                name = "mearec_recordings",
                data = np.array(self._mearec_rawio._recordings),
                dims = ["x", "y"],
                coords = {
                    "x": np.array(range(self._mearec_rawio._recordings.shape[0])),
                    "y": np.array(range(self._mearec_rawio._recordings.shape[1])),
                    },
                attrs = {
                    "hrid": hrid,
                    "schema_name": "spikeinterface_recordings",
                    }
                )

        self._sampling_rate_dataarray = xr.DataArray(
                name = "mearec_sampling_rate",
                data = self._mearec_rawio._recgen.info['recordings']['fs'],
                dims = ["x"],
                coords = {
                    "x": np.array(range(1)),
                    },
                attrs = {
                    "hrid": hrid,
                    "schema_name": "spikeinterface_sampling_rate",
                    }
                )

        self._recording_info_record = {
                "hrid": hrid,
                "schema_name": "spikeinterface_recording_info",
                "has_data": False,
                "info_pickle": pickle.dumps(self._mearec_rawio._recgen.info),
                }

        self._spiketrain_records = list(map(self._build_spiketrain_record(hrid), enumerate(self._mearec_rawio._spiketrains)))

    def _build_spiketrain_record(self, hrid):
        def build_fn(idx_and_spiketrain):
            idx, spiketrain = idx_and_spiketrain
            return {
                    "hrid": f"{hrid}-{idx}",
                    "schema_name": "spikeinterface_spiketrain",
                    "has_data": False,
                    "times": pickle.dumps(spiketrain.as_quantity()),
                    "t_stop": pickle.dumps(spiketrain.t_stop),
                    "units": pickle.dumps(spiketrain.units),
                    "dtype": pickle.dumps(spiketrain.dtype),
                    "copy": True,
                    "sampling_rate": pickle.dumps(spiketrain.sampling_rate),
                    "t_start": pickle.dumps(spiketrain.t_start),
                    "waveforms": pickle.dumps(spiketrain.waveforms),
                    "left_sweep": spiketrain.left_sweep,
                    "name": spiketrain.name,
                    "file_origin": spiketrain.file_origin,
                    "description": spiketrain.description,
                    "array_annotations": spiketrain.array_annotations,
                    "annotations_pickle": pickle.dumps(spiketrain.annotations),
                    }
        return build_fn


