import pytest

import matplotlib.pyplot as plt
from pprint import pprint

import spikeinterface as si  # import core only
import spikeinterface.extractors as se
import spikeinterface.preprocessing as spre
import spikeinterface.sorters as ss
import spikeinterface.postprocessing as spost
import spikeinterface.qualitymetrics as sqm
import spikeinterface.comparison as sc
import spikeinterface.exporters as sexp
import spikeinterface.curation as scur
import spikeinterface.widgets as sw

from src.utilities.testing.data_mocks import *
# from src.store.data_access_objects import DataArrayDAO, RecordDAO
from neo.rawio.mearecrawio import MEArecRawIO

import numpy as np

import json
import pickle
import neo

# from src.operations.helpers.neuroscikit_rawio import NeuroSciKitRawIO
# from src.operations.helpers.spikeinterface_helper import SpikeinterfaceMEArecHelper
# from src.operations.helpers.neuroscikit_extractor import NeuroSciKitExtractor

global_job_kwargs = dict(n_jobs=4, chunk_duration="1s")
si.set_global_job_kwargs(**global_job_kwargs)


class TestSpikeinterfaceExtraction:

    @pytest.mark.skip()
    @pytest.mark.slow()
    def test_load_and_retrieve_recordings(self, tmpdir, empty_client, project):

        # local_path = si.download_dataset(remote_path='mearec/mearec_test_10s.h5')

        file_path = '../../data/mearec/recordings/recordings.h5'
        mearec_rawio = MEArecRawIO(file_path)

        si_helper = SpikeinterfaceMEArecHelper()
        si_helper.load(mearec_rawio, 'test1')

        data_dao = DataArrayDAO(tmpdir)
        record_dao = RecordDAO(empty_client, project)

        data_dao.add(si_helper.recordings_dataarray, datetime.utcnow())
        data_dao.add(si_helper.sampling_rate_dataarray, datetime.utcnow())
        record_dao.add(si_helper.recording_info_record, datetime.utcnow())
        for spiketrain_record in si_helper.spiketrain_records:
            record_dao.add(spiketrain_record, datetime.utcnow())

        nsk_rawio = NeuroSciKitRawIO(data_dao, record_dao, 'test1')
        nsk_rawio._parse_header()

        got_recordings = nsk_rawio._recordings
        got_sampling_rate = nsk_rawio._sampling_rate
        got_info = nsk_rawio._info
        got_spiketrains = nsk_rawio._spiketrains

        print(f'got_recordings: {got_recordings}')
        print(f'got_sampling_rate: {got_sampling_rate}')
        print(f'got_info: {got_info}')
        print(f'got_spiketrains: {got_spiketrains}')

        nsk_extractor = NeuroSciKitExtractor(data_dao, record_dao, 'test1')
        print(f'nsk_extractor.get_channel_ids(): {nsk_extractor.get_channel_ids()}')

