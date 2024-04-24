from spikeinterface.extractors.neoextractors.neobaseextractor import NeoBaseRecordingExtractor, NeoBaseSortingExtractor
from neo import rawio
from signalstore.operations.helpers.neuroscikit_rawio import NeuroSciKitRawIO
from signalstore.store.data_access_objects import DataArrayDAO, RecordDAO


# Because of how the NeoBaseRecordingExtractor works, our rawio class needs to be a member of the
# neo.rawio module.
rawio.NeuroSciKitRawIO = NeuroSciKitRawIO


class NeuroSciKitExtractor(NeoBaseRecordingExtractor):
    mode = 'folder' # Not sure what, if anything, this does.
    NeoRawIOClass = 'NeuroSciKitRawIO'
    name = "neuroscikit"

    def __init__(self, data_dao: DataArrayDAO, record_dao: RecordDAO, dataarray_hrid: str, stream_id=None, stream_name=None, all_annotations=False):
        neo_kwargs = self.map_to_neo_kwargs(data_dao, record_dao, dataarray_hrid)
        NeoBaseRecordingExtractor.__init__(self, stream_id=stream_id,
                                           stream_name=stream_name,
                                           all_annotations=all_annotations,
                                           **neo_kwargs)
        self._kwargs.update(dict(data_dao=data_dao, record_dao=record_dao, dataarray_hrid=dataarray_hrid))

    @classmethod
    def map_to_neo_kwargs(cls, data_dao, record_dao, dataarray_hrid):
        neo_kwargs = {
                'data_dao': data_dao,
                'record_dao': record_dao,
                'dataarray_hrid': dataarray_hrid,
                }
        return neo_kwargs

