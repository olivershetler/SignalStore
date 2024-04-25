from neo.rawio.baserawio import (BaseRawIO, _signal_channel_dtype, _signal_stream_dtype,
                _spike_channel_dtype, _event_channel_dtype)

import numpy as np
from copy import deepcopy

import pickle
import neo
import re


class NeuroSciKitRawIO(BaseRawIO):
    """
    Class for "reading" fake data from the neuroscikit repos, meant to provide data equivalent to
    that provided by MEArecRawIO for h5 files.
    """
    extensions = ['h5']
    rawmode = 'one-file'

    def __init__(self, unit_of_work, dataarray_hrid: str):
        BaseRawIO.__init__(self)
        self._uow = unit_of_work
        # check that the uow is open (i.e. has been called using with UnitOfWork(project) as uow:)
        
        self._dataarray_hrid = dataarray_hrid

    def _source_name(self):
        return self._dataarray_hrid

    def _parse_header(self):
        self._sampling_rate = self._data_dao.get('spikeinterface_sampling_rate', self._dataarray_hrid).data
        self._recordings = self._data_dao.get('spikeinterface_recordings', self._dataarray_hrid).data

        self._num_frames, self._num_channels = self._recordings.shape

        signal_streams = np.array([('Signals', '0')], dtype=_signal_stream_dtype)

        sig_channels = []
        for c in range(self._num_channels):
            ch_name = 'ch{}'.format(c)
            chan_id = str(c + 1)
            sr = self._sampling_rate  # Hz
            dtype = self._recordings.dtype
            units = 'uV'
            gain = 1.
            offset = 0.
            stream_id = '0'
            sig_channels.append((ch_name, chan_id, sr, dtype, units, gain, offset, stream_id))
        sig_channels = np.array(sig_channels, dtype=_signal_channel_dtype)

        # creating units channels
        spike_channels = []

        hrid_pat = re.compile(fr'{self._dataarray_hrid}-[0-9]+')
        spiketrain_records = self._record_dao.find({'schema_name': 'spikeinterface_spiketrain', 'hrid': {'$regex': hrid_pat}, 'time_of_removal': None})
        self._spiketrains = [
                neo.SpikeTrain(
                    pickle.loads(spiketrain_record["times"]),
                    pickle.loads(spiketrain_record["t_stop"]),
                    units=pickle.loads(spiketrain_record["units"]),
                    dtype=pickle.loads(spiketrain_record["dtype"]),
                    copy=spiketrain_record["copy"],
                    sampling_rate=pickle.loads(spiketrain_record["sampling_rate"]),
                    t_start=pickle.loads(spiketrain_record["t_start"]),
                    waveforms=pickle.loads(spiketrain_record["waveforms"]),
                    left_sweep=spiketrain_record["left_sweep"],
                    name=spiketrain_record["name"],
                    file_origin=spiketrain_record["file_origin"],
                    description=spiketrain_record["description"],
                    array_annotations=spiketrain_record["array_annotations"],
                    **pickle.loads(spiketrain_record["annotations_pickle"]),
                    )
                for spiketrain_record in spiketrain_records]

        for c in range(len(self._spiketrains)):
            unit_name = 'unit{}'.format(c)
            unit_id = '#{}'.format(c)
            # if spiketrains[c].waveforms is not None:
            wf_units = ''
            wf_gain = 1.
            wf_offset = 0.
            wf_left_sweep = 0
            wf_sampling_rate = self._sampling_rate
            spike_channels.append((unit_name, unit_id, wf_units, wf_gain,
                                  wf_offset, wf_left_sweep, wf_sampling_rate))
        spike_channels = np.array(spike_channels, dtype=_spike_channel_dtype)

        event_channels = []
        event_channels = np.array(event_channels, dtype=_event_channel_dtype)

        self.header = {}
        self.header['nb_block'] = 1
        self.header['nb_segment'] = [1]
        self.header['signal_streams'] = signal_streams
        self.header['signal_channels'] = sig_channels
        self.header['spike_channels'] = spike_channels
        self.header['event_channels'] = event_channels

        info_record = self._record_dao.get('spikeinterface_recording_info', self._dataarray_hrid)
        self._info = pickle.loads(info_record['info_pickle'])

        self._generate_minimal_annotations()
        for block_index in range(1):
            bl_ann = self.raw_annotations['blocks'][block_index]
            bl_ann['mearec_info'] = deepcopy(self._info)

    def _segment_t_start(self, block_index, seg_index):
        all_starts = [[0.]]
        return all_starts[block_index][seg_index]

    def _segment_t_stop(self, block_index, seg_index):
        t_stop = self._num_frames / self._sampling_rate
        all_stops = [[t_stop]]
        return all_stops[block_index][seg_index]

    def _get_signal_size(self, block_index, seg_index, stream_index):
        assert stream_index == 0
        return self._num_frames

    def _get_signal_t_start(self, block_index, seg_index, stream_index):
        assert stream_index == 0
        return self._segment_t_start(block_index, seg_index)

    def _get_analogsignal_chunk(self, block_index, seg_index, i_start, i_stop,
                                stream_index, channel_indexes):
        if i_start is None:
            i_start = 0
        if i_stop is None:
            i_stop = self._num_frames

        if channel_indexes is None:
            channel_indexes = slice(self._num_channels)
        if isinstance(channel_indexes, slice):
            raw_signals = self._recordings[i_start:i_stop, channel_indexes]
        else:
            # sort channels because h5py neeeds sorted indexes
            if np.any(np.diff(channel_indexes) < 0):
                sorted_channel_indexes = np.sort(channel_indexes)
                sorted_idx = np.array([list(sorted_channel_indexes).index(ch)
                                       for ch in channel_indexes])
                raw_signals = self._recordings[i_start:i_stop, sorted_channel_indexes]
                raw_signals = raw_signals[:, sorted_idx]
            else:
                raw_signals = self._recordings[i_start:i_stop, channel_indexes]
        return raw_signals

    def _spike_count(self, block_index, seg_index, unit_index):
        return len(self._spiketrains[unit_index])

    def _get_spike_timestamps(self, block_index, seg_index, unit_index, t_start, t_stop):
        spike_timestamps = self._spiketrains[unit_index].times.magnitude
        if t_start is None:
            t_start = self._segment_t_start(block_index, seg_index)
        if t_stop is None:
            t_stop = self._segment_t_stop(block_index, seg_index)
        timestamp_idxs = np.where((spike_timestamps >= t_start) & (spike_timestamps < t_stop))

        return spike_timestamps[timestamp_idxs]

    def _rescale_spike_timestamp(self, spike_timestamps, dtype):
        return spike_timestamps.astype(dtype)

    def _get_spike_raw_waveforms(self, block_index, seg_index,
                                 spike_channel_index, t_start, t_stop):
        return None
