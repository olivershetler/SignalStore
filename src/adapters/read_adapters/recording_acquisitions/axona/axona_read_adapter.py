from src.operations.importers.adapters.abstract_read_adapter import AbstractReadAdapter

class AxonaReadAdapter(AbstractReadAdapter):
    def __init__(self, directory):
        self.data_directory = Path(directory)
        self.session_paths = self._assemble_session_paths()

    # has __iter__ and __next__ methods inherited from AbstractReadAdapter.
    # The methods use self.read() to get the data generator. There is no need to
    # call read when using the axona read adapter. It is preferred to use
    # data = AxonaReadAdapter(directory)
    # and then use list(data) to get the data as a list.
    # or next(data) to get the next item in the generator.
    # or for item in data: to iterate over the generator.
    def read(self):
        for name in self.session_paths:
            data = self._read_session_data(name)
            yield data

    def _read_session_data(self, session_key):
        session_paths = self.session_paths[session_key]
        data = []
        data.extend(read_position_data_set(session_paths['pos'], session_key))
        # data.extend(read_settings_data_set(session_paths['set'],
        #                                    session_key))
        tetrode_keys = list(set(session_paths.keys()) - {'pos', 'set'})
        for tetrode_key in tetrode_keys:
            tetrode_paths = session_paths[tetrode_key]
            for name in tetrode_paths:
                if name == 'cut':
                    continue
                if name == 'eeg' or name == 'egf':
                    data.extend(
                        read_lfp_data_set(
                            tetrode_paths[name],
                            session_key, tetrode_key))
                elif name == 'tet':
                    data.extend(
                        read_spike_data_set(
                            tetrode_paths[name],
                            tetrode_paths['cut'],
                            session_key, tetrode_key))
        for record in data:
            if isinstance(record, xr.DataArray):
                record.attrs.update(session_key=session_key)
            elif isinstance(record, dict):
                record.update(session_key=session_key)
        return data

    def _assemble_session_paths(self):
        paths = {}
        for path in self.data_directory.glob('*'):
            try:
                keys = self._path_key_tuple(path)
            except AxonaReaderFileExtensionError:
                continue
            if keys[0] not in paths.keys():
                paths[keys[0]] = {}
            if len(keys) == 2:
                paths[keys[0]][keys[1]] = path
            elif len(keys) == 3:
                if keys[1] not in paths[keys[0]].keys():
                    paths[keys[0]][keys[1]] = {}
                paths[keys[0]][keys[1]][keys[2]] = path
        return paths

    def _path_key_tuple(self, path):
        if self._ispos(path):
            return self._pos_session_key(path), 'pos'
        elif self._isset(path):
            return self._set_session_key(path), 'set'
        elif self._iscut(path):
            return self._cut_session_key(
                path), self._cut_tetrode_key(path), 'cut'
        elif self._isclu(path):
            return self._clu_session_key(
                path), self._clu_tetrode_key(path), 'clu'
        elif self._istet(path):
            return self._tet_session_key(
                path), self._tet_tetrode_key(path), 'tet'
        elif self._iseeg(path):
            return self._eeg_session_key(
                path), self._eeg_tetrode_key(path), 'eeg'
        elif self._isegf(path):
            return self._egf_session_key(
                path), self._egf_tetrode_key(path), 'egf'
        else:
            raise AxonaReaderFileExtensionError(
                f'This file has an unsupported axona extension: {path}')

    def _ispos(self, path):
        return path.suffix == '.pos'

    def _pos_session_key(self, path):
        return path.stem

    def _isset(self, path):
        return path.suffix == '.set'

    def _set_session_key(self, path):
        return path.stem

    def _iscut(self, path):
        return path.suffix == '.cut'

    def _cut_session_key(self, path):
        # get the pathname before the last underscore
        name = path.stem.split('_')[:-1]
        if len(name) == 1:
            return name[0]
        elif len(name) > 1:
            return '_'.join(name)
        else:
            raise AxonaReaderFileExtensionError(
                f'This .cut file has an invalid extension: {path}')

    def _cut_tetrode_key(self, path):
        # get the number after the last underscore but before the extension
        return int(path.stem.split('_')[-1])

    def _isclu(self, path):
        return Path(path.stem).suffix == '.clu'

    def _clu_session_key(self, path):
        # get the pathname before the last underscore
        return Path(path.stem).stem

    def _clu_tetrode_key(self, path):
        # get the number after the last underscore but before the extension
        return int(path.suffix[1:])

    def _istet(self, path):
        # check if the path extension is an integer
        return path.suffix[1:].isnumeric()

    def _tet_session_key(self, path):
        # get the pathname before the last period
        return path.stem

    def _tet_tetrode_key(self, path):
        return int(path.suffix[1:])

    def _iseeg(self, path):
        return path.suffix[:4] == '.eeg'

    def _eeg_session_key(self, path):
        return path.stem

    def _eeg_tetrode_key(self, path):
        if path.suffix == '.eeg':
            return 1
        elif path.suffix.replace('.eeg', '').isnumeric():
            return int(path.suffix.replace('.eeg', ''))
        else:
            raise AxonaReaderFileExtensionError(
                f'This .eeg* file has an invalid extension: {path}')

    def _isegf(self, path):
        return path.suffix[:4] == '.egf'

    def _egf_session_key(self, path):
        return path.stem

    def _egf_tetrode_key(self, path):
        if path.suffix == '.egf':
            return 1
        elif path.suffix.replace('.egf', '').isnumeric():
            return int(path.suffix.replace('.egf', ''))
        else:
            raise AxonaReaderFileExtensionError(
                f'This .egf* file has an invalid extension: \n{path}')


class AxonaReaderFileExtensionError(ValueError):
    pass


# =============================================================================
# EEG or EGF Helpers
# =============================================================================

# Internal Dependencies
import contextlib  # for closing the file
import mmap

# from core.data_voltage import EphysSeries

# A+ Grade Dependencies
import numpy as np
import xarray as xr
from pathlib import Path

# A Grade Dependencies

# Other Dependencies


def read_lfp_data_set(file_path: Path, session_key: str, tetrode_key: str):
    with open(file_path, 'rb') as eeg_or_egf_file:
        if 'eeg' in file_path.suffix:
            file_type = 'eeg'
        elif 'egf' in file_path.suffix:
            file_type = 'egf'
        else:
            raise ValueError(
                        'The file extension must be either "eeg*" or "egf*".'
                        + f'The current file extension is {file_path.suffix}'
                        )
        records = read_eeg_or_egf(eeg_or_egf_file, file_type, session_key, tetrode_key)
        for record in records:
            record.attrs['session_key'] = session_key
        return records


def read_eeg_or_egf(opened_eeg_or_egf_file, file_type: str, session_key, tetrode_key) -> np.ndarray:
    """input:
    opened_eeg_or_egf_file: an open file object for the .eeg or .egf file
    Output:
    The EEG waveform, and the sampling frequency
    """
    is_eeg = False
    is_egf = False
    if 'eeg' == file_type:
        is_eeg = True
    elif 'egf' == file_type:
        is_egf = True
    else:
        raise ValueError('The file extension must be either "eeg" or "egf".')

    if is_eeg == is_egf:
        raise LFPFileTypeConflict('The file extension must be EITHER "eeg"'
                                  + 'XOR "egf".\nCurrently the is_eeg ='
                                  + f'{is_eeg}, and is_egf = {is_egf}')

    mp = mmap.mmap(opened_eeg_or_egf_file.fileno(), 0, access=mmap.ACCESS_READ)
    with contextlib.closing(mp) as memory_map:
        # find the data_start
        start_index = int(
            memory_map.find(b'data_start') +
            len('data_start'))  # start of the data
        stop_index = int(memory_map.find(b'\r\ndata_end'))  # end of the data

        sample_rate_start = memory_map.find(b'sample_rate')
        sample_rate_end = memory_map[sample_rate_start:].find(b'\r\n')
        Fs = float(memory_map[sample_rate_start:sample_rate_start +
                   sample_rate_end].decode('utf-8').split(' ')[1])

        data_string = memory_map[start_index:stop_index]

        if is_eeg and not is_egf:
            assert Fs == 250
            lfp_data = np.frombuffer(data_string, dtype='>b')
        elif is_egf and not is_eeg:
            assert Fs == 4.8e3
            lfp_data = np.frombuffer(data_string, dtype='<h')
        else:
            raise ValueError(
                'The file extension must be either "eeg" or "egf"')

        lfp_signal = xr.DataArray(lfp_data,
                                  name = f"{session_key}_lfp_tetrode_{str(tetrode_key)}_{int(Fs)}_hz",
                                  dims=['time'],
                                  attrs={'type': 'lfp',
                                         'units': 'uV',
                                         'dimensionality': 'voltage',
                                         'sample_rate': Fs
                                         }
                                  )

        lfp_time = xr.DataArray(np.arange(lfp_data.size) / Fs,
                                name=f"{session_key}_lfp_tetrode_{str(tetrode_key)}_time_{int(Fs)}_hz",
                                dims=['time'],
                                attrs={'type': 'lfp',
                                       'units': 's',
                                       'dimensionality': 'time',
                                       'sample_rate': Fs
                                       }
                                )

        return [lfp_signal, lfp_time]


class LFPFileTypeConflict(ValueError):
    pass

# =============================================================================
# pos (position) Helpers
# =============================================================================

import numpy as np
import scipy
import struct
import xarray as xr

# A Grade Dependencies

# Other Dependencies


def read_position_data_set(pos_path: str, session_key, ppm=None) -> list:
    data = read_position_data(pos_path, ppm=ppm)
    pos_x = xr.DataArray(data['x'],
                         name=session_key + '_pos_x',
                         dims='time',
                         attrs={'type': 'position',
                                'units': 'meters',
                                'dimensionality': 'length',
                                'orientation': 'horizontal',
                                'session_key': session_key})
    pos_y = xr.DataArray(data['y'],
                         dims='time',
                         name=session_key + '_pos_y',
                         attrs={'type': 'position',
                                'units': 'meters',
                                'dimensionality': 'length',
                                'orientation': 'vertical',
                                'session_key': session_key})
    pos_t = xr.DataArray(data['t'],
                         name=session_key + '_pos_t',
                         dims='time',
                         attrs={'type': 'position',
                                'units': 'seconds',
                                'dimensionality': 'time',
                                'session_key': session_key})
    return [pos_x, pos_y, pos_t]


def read_position_data(pos_path: str, ppm=None) -> tuple:

    '''
        Extracts position data from .pos file

        Params:
            pos_path (str):
                Directory of where the position file is stored
            ppm (float):
                Pixel per meter value

        Returns:
            Tuple: pos_x,pos_y,pos_t,(pos_x_width,pos_y_width)
            --------
            pos_x, pos_y, pos_t (np.ndarray):
                Array of x, y coordinates, and timestamps
            pos_x_width (float):
                max - min x coordinate value (arena width)
            pos_y_width (float)
                max - min y coordinate value (arena length)
    '''

    pos_data = _get_position(pos_path, ppm=ppm)

    # Correcting pos_t data in case of bad position file
    new_pos_t = np.copy(pos_data[2])
    if len(new_pos_t) < len(pos_data[0]):
        while len(new_pos_t) != len(pos_data[0]):
            new_pos_t = np.append(new_pos_t, float(new_pos_t[-1] + 0.02))
    elif len(new_pos_t) > len(pos_data[0]):
        while len(new_pos_t) != len(pos_data[0]):
            new_pos_t = np.delete(new_pos_t, -1)

    Fs_pos = pos_data[3]

    file_ppm = pos_data[-1]

    if file_ppm is None:
        raise AxonaPosAdapterError('PPM must be in position file or settings'
                                   + ' dictionary to proceed')

    pos_x = pos_data[0]
    pos_y = pos_data[1]
    pos_t = new_pos_t

    # Rescale coordinate values with respect to a center point
    # (i.e arena center = origin (0,0))
    center = _center_box(pos_x, pos_y)
    pos_x = pos_x - center[0]
    pos_y = pos_y - center[1]

    # Correct for bad tracking
    pos_data_corrected = _rem_bad_track(pos_x, pos_y, pos_t, 2)
    pos_x = pos_data_corrected[0]
    pos_y = pos_data_corrected[1]
    pos_t = pos_data_corrected[2]

    # Remove NaN values
    nonNanValues = np.where(np.isnan(pos_x) == False)[0]
    pos_t = pos_t[nonNanValues]
    pos_x = pos_x[nonNanValues]
    pos_y = pos_y[nonNanValues]

    # Smooth data using boxcar convolution
    B = np.ones((int(np.ceil(0.4 * Fs_pos)), 1)) / np.ceil(0.4 * Fs_pos)
    pos_x = scipy.ndimage.convolve(pos_x, B, mode='nearest')
    pos_y = scipy.ndimage.convolve(pos_y, B, mode='nearest')

    pos_x_width = max(pos_x) - min(pos_x)
    pos_y_width = max(pos_y) - min(pos_y)

    return {"t": pos_t.flatten(),
            "x": pos_x.flatten(),
            "y": pos_y.flatten(),
            "arena_width": pos_x_width,
            "arena_height": pos_y_width,
            "sample_rate": pos_data[3],
            "ppm": file_ppm}


def _rem_bad_track(x, y, t, threshold):
    """function [x,y,t] = _rem_bad_track(x,y,t,treshold)

    % Indexes to position samples that are to be removed
   """

    remInd = []
    diffx = np.diff(x, axis=0)
    diffy = np.diff(y, axis=0)
    diffR = np.sqrt(diffx ** 2 + diffy ** 2)

    # the MATLAB works fine without NaNs, if there are Nan's just set them
    # to threshold they will be removed later
    diffR[np.isnan(diffR)] = threshold  # setting the nan values to threshold
    ind = np.where((diffR > threshold))[0]

    if len(ind) == 0:  # no bad samples to remove
        return x, y, t

    if ind[-1] == len(x):
        offset = 2
    else:
        offset = 1

    for index in range(len(ind) - offset):
        if ind[index + 1] == ind[index] + 1:
            # A single sample position jump, tracker jumps out one sample and
            # then jumps back to path on the next sample. Remove bad sample.
            remInd.append(ind[index] + 1)
        else:
            ''' Not a single jump. 2 possibilities:
             1. TrackerMetadata jumps out, and stay out at the same place
             for several
             samples and then jumps back.
             2. TrackerMetadata just has a small jump before path continues
             as normal,
             unknown reason for this. In latter case the samples are left
             untouched'''
            con = x[ind[index] + 1:ind[index + 1] + 1 + 1] == x[ind[index] + 1]
            idx = np.where(con)[0]
            if len(idx) == len(x[ind[index] + 1:ind[index + 1] + 1 + 1]):
                remInd.extend(
                    list(range(ind[index] + 1, ind[index + 1] + 1 + 1)))
                # have that extra since range goes to end-1

    # keep_ind = [val for val in range(len(x)) if val not in remInd]
    keep_ind = np.setdiff1d(np.arange(len(x)), remInd)

    x = x[keep_ind].flatten()
    y = y[keep_ind].flatten()
    t = t[keep_ind].flatten()

    return x.reshape((len(x), 1)), y.reshape((len(y), 1)), t.reshape((len(t), 1))


def _find_center(NE, NW, SW, SE):
    """Finds the center point (x,y) of the position boundaries"""

    x = np.mean([np.amax([NE[0], SE[0]]), np.amin([NW[0], SW[0]])])
    y = np.mean([np.amax([NW[1], NE[1]]), np.amin([SW[1], SE[1]])])
    return np.array([x, y])


def _center_box(posx, posy):
    # must remove Nans first because the np.amin will return nan if there is a nan
    posx = posx[~np.isnan(posx)]  # removes NaNs
    posy = posy[~np.isnan(posy)]  # remove Nans

    NE = np.array([np.amax(posx), np.amax(posy)])
    NW = np.array([np.amin(posx), np.amax(posy)])
    SW = np.array([np.amin(posx), np.amin(posy)])
    SE = np.array([np.amax(posx), np.amin(posy)])

    return _find_center(NE, NW, SW, SE)


def _fix_timestamps(post):
    first = post[0]
    N = len(post)
    uniquePost = np.unique(post)

    if len(uniquePost) != N:
        didFix = True
        numZeros = 0
        # find the number of zeros at the end of the file

        while True:
            if post[-1 - numZeros] == 0:
                numZeros += 1
            else:
                break
        last = first + (N-1-numZeros)*0.02
        fixedPost = np.arange(first, last+0.02, 0.02)
        fixedPost = fixedPost.reshape((len(fixedPost), 1))

    else:
        didFix = False
        fixedPost = []

    return didFix, fixedPost


def _arena_config(posx, posy, ppm, center, flip_y=True):
    """
    :param posx:
    :param posy:
    :param arena:
    :param conversion:
    :param center:
    :param flip_y: bool value that will determine if you want to flip y or not. When recording on Intan we inverted the
    positions due to the camera position. However in the virtualmaze you might not want to flip y values.
    :return:
    """
    center = center
    conversion = ppm

    posx = 100 * (posx - center[0]) / conversion

    if flip_y:
        # flip the y axis
        posy = 100 * (-posy + center[1]) / conversion
    else:
        posy = 100 * (posy + center[1]) / conversion

    return posx, posy


def _remove_nan(posx, posy, post):
    """Remove any NaNs from the end of the array"""
    remove_nan = True
    while remove_nan:
        if np.isnan(posx[-1]):
            posx = posx[:-1]
            posy = posy[:-1]
            post = post[:-1]
        else:
            remove_nan = False
    return posx, posy, post


def _get_position(pos_fpath, ppm=None, method='', flip_y=True):
    """
    _get_position function:
    ---------------------------------------------
    variables:
    -pos_fpath: the full path (C:/example/session.pos)

    output:
    t: column numpy array of the time stamps
    x: a column array of the x-values (in pixels)
    y: a column array of the y-values (in pixels)
    """

    with open(pos_fpath, 'rb+') as f:  # opening the .pos file
        headers = ''  # initializing the header string
        for line in f:  # reads line by line to read the header of the file
            # print(line)
            if 'data_start' in str(line):  # if it reads data_start that means the header has ended
                headers += 'data_start'
                break  # break out of for loop once header has finished
            elif 'duration' in str(line):
                headers += line.decode(encoding='UTF-8')
            elif 'num_pos_samples' in str(line):
                num_pos_samples = int(line.decode(encoding='UTF-8')[len('num_pos_samples '):])
                headers += line.decode(encoding='UTF-8')
            elif 'bytes_per_timestamp' in str(line):
                bytes_per_timestamp = int(line.decode(encoding='UTF-8')[len('bytes_per_timestamp '):])
                headers += line.decode(encoding='UTF-8')
            elif 'bytes_per_coord' in str(line):
                bytes_per_coord = int(line.decode(encoding='UTF-8')[len('bytes_per_coord '):])
                headers += line.decode(encoding='UTF-8')
            elif 'timebase' in str(line):
                timebase = (line.decode(encoding='UTF-8')[len('timebase '):]).split(' ')[0]
                headers += line.decode(encoding='UTF-8')
            elif 'pixels_per_metre' in str(line):
                # print('READING PIXELS PER METRE FROM FILE')
                ppm = float(line.decode(encoding='UTF-8')[len('pixels_per_metre '):])
                headers += line.decode(encoding='UTF-8')
            elif 'min_x' in str(line) and 'window' not in str(line):
                min_x = int(line.decode(encoding='UTF-8')[len('min_x '):])
                headers += line.decode(encoding='UTF-8')
            elif 'max_x' in str(line) and 'window' not in str(line):
                max_x = int(line.decode(encoding='UTF-8')[len('max_x '):])
                headers += line.decode(encoding='UTF-8')
            elif 'min_y' in str(line) and 'window' not in str(line):
                min_y = int(line.decode(encoding='UTF-8')[len('min_y '):])
                headers += line.decode(encoding='UTF-8')
            elif 'max_y' in str(line) and 'window' not in str(line):
                max_y = int(line.decode(encoding='UTF-8')[len('max_y '):])
                headers += line.decode(encoding='UTF-8')
            elif 'pos_format' in str(line):
                headers += line.decode(encoding='UTF-8')
                if 't,x1,y1,x2,y2,numpix1,numpix2' in str(line):
                    two_spot = True
                else:
                    two_spot = False
                    print('The position format is unrecognized!')

            elif 'sample_rate' in str(line):
                sample_rate = line.decode(encoding='UTF-8').split(' ')[1]
                sample_rate = float(sample_rate)
                headers += line.decode(encoding='UTF-8')

            else:
                headers += line.decode(encoding='UTF-8')

    assert ppm is not None, 'PPM must be in position file or settings dictionary to proceed'

    if two_spot:
        '''Run when two spot mode is on, (one_spot has the same format so it will also run here)'''
        with open(pos_fpath, 'rb+') as f:
            '''get_pos for one_spot'''
            pos_data = f.read()  # all the position data values (including header)
            pos_data = pos_data[len(headers):-12]  # removes the header values

            byte_string = 'i8h'

            pos_data = np.asarray(struct.unpack('>%s' % (num_pos_samples * byte_string), pos_data))
            pos_data = pos_data.astype(float).reshape((num_pos_samples, 9))  # there are 8 words and 1 time sample

        x = pos_data[:, 1]
        y = pos_data[:, 2]
        t = pos_data[:, 0]

        x = x.reshape((len(x), 1))
        y = y.reshape((len(y), 1))
        t = t.reshape((len(t), 1))

        if method == 'raw':
            return x, y, t, sample_rate

        t = np.divide(t, np.float64(timebase))  # converting the frame number from Axona to the time value

        # values that are NaN are set to 1023 in Axona's system, replace these values by NaN's

        x[np.where(x == 1023)] = np.nan
        y[np.where(y == 1023)] = np.nan

        didFix, fixedPost = _fix_timestamps(t)

        if didFix:
            t = fixedPost

        t = t - t[0]

        x, y = _arena_config(x, y, ppm,
                             center=_center_box(x, y),
                             flip_y=flip_y)

        # remove any NaNs at the end of the file
        x, y, t = _remove_nan(x, y, t)

    else:
        print("Haven't made any code for this part yet.")

    return x.reshape((len(x), 1)), y.reshape((len(y), 1)), t.reshape((len(t), 1)), sample_rate, ppm


class AxonaPosAdapterError(Exception):
    pass


# =============================================================================
# cut and tet (tetrode) Helpers
# =============================================================================

import xarray as xr
import numpy as np
from datetime import datetime


def read_spike_data_set(tetrode_file_path, cut_file_path, session_data_ref, tetrode_name):

    spike_labels = _read_cut_file(
                                    cut_file_path,
                                    session_data_ref
                                    )
    spike_timestamps, spike_waveforms = _read_tetrode_file(
                                            tetrode_file_path,
                                            session_data_ref
                                            )
    session_data_ref, tetrode_name = str(session_data_ref), str(tetrode_name)
    spike_timestamps.name = session_data_ref + '_tetrode_' + tetrode_name + '_spike_timestamps'
    spike_waveforms.name = session_data_ref + '_tetrode_' + tetrode_name + '_spike_waveforms'
    spike_labels.name = session_data_ref + '_tetrode_' + tetrode_name + '_spike_labels'

    return [spike_timestamps, spike_waveforms, spike_labels]


def _read_cut_file(cut_file_path, session_data_ref):
    with open(cut_file_path, 'r') as cut_file:
        lines = cut_file.readlines()
        #print('>>>>>>>>>>>>>> ', lines)
        spike_labels = []
        extract_cut = False
        for line in lines:
            if 'Exact_cut' in line:  # finding the beginning of the cut values
                extract_cut = True
                continue
            if extract_cut:  # read all the cut values
                line_labels = str(line)
                for string_val in ['\\n', ',', "'", '[', ']']:
                    # remove non base10 integer values
                    line_labels = line_labels.replace(string_val, '')
                spike_labels.extend([int(val) for val in line_labels.split()])
            else:
                continue
        if len(spike_labels) == 0:
            raise ValueError('There are no spike labels in this file.')
        return xr.DataArray(
                            data=np.array(spike_labels),
                            dims=['spikes'],
                            attrs={'type': 'spike_labels',
                                'session_data_ref': session_data_ref,
                                'units': 'neuron',
                                'dimensionality': 'nominal',
                            }
                            )


def _read_tetrode_file(tetrode_file_path, session_data_ref):
    raw_data = _get_raw_tetrode_data(tetrode_file_path)

    spike_timestamps = _extract_spike_timestamps(raw_data)

    spike_waveforms = _extract_spike_waveforms(raw_data)

    metadata = {
        'session_data_ref': session_data_ref,
        'session_start': _session_start(
                            raw_data['trial_date'],
                            raw_data['trial_time']
                            ),
        "session_duration": raw_data['duration'],
        "sample_rate": raw_data['sample_rate'],
    }

    spike_timestamps.attrs.update(metadata)
    spike_waveforms.attrs.update(metadata)

    return spike_timestamps, spike_waveforms


def _get_raw_tetrode_data(tetrode_file_path):

    data = {}
    with TetrodeDecoder(tetrode_file_path) as d:
        for line in d.tetrode_file:
            for keyword in d.line_decoders.keys():
                if keyword in str(line):
                    data[keyword] = d.line_decoders[keyword](line)
                    break

    # replace data_start with spike_data name for clarity
    data['spike_data'] = data.pop('data_start')
    return data


class TetrodeDecoder:

    def __init__(self, tetrode_file):
        self.tetrode_file = open(tetrode_file, 'rb')
        self.line_decoders = {
                                'num_spikes': self.numeric,
                                'bytes_per_timestamp': self.numeric,
                                'samples_per_spike': self.numeric,
                                'bytes_per_sample': self.numeric,
                                'timebase': self.numeric,
                                'duration': self.numeric,
                                'sample_rate': self.numeric,
                                'num_chans': self.numeric,
                                'trial_date': self.datestring,
                                'trial_time': self.timestring,
                                'data_start': self.data
                            }

    def __del__(self):
        self.tetrode_file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tetrode_file.close()

    def data(self, line):
        start_index = len('data_start')
        stop_index = -len('\r\ndata_end\r\n')
        data_string = (line + self.tetrode_file.read())[start_index:stop_index]
        spike_data = np.frombuffer(data_string, dtype='uint8')
        return spike_data

    def numeric(self, line):
        return int(line.decode(encoding='UTF-8').split(" ")[1])

    def datestring(self, line):
        return line.decode(encoding='UTF-8').split(" ")[2:]

    def timestring(self, line):
        return line.decode(encoding='UTF-8').split(" ")[1]


def _session_start(trial_date, trial_time):
    day, month, year = trial_date
    month = datetime.strptime(str(month), '%b').month
    hour, minute, second = trial_time.split(':')
    date_time = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    return date_time


def _extract_spike_timestamps(raw_data):
    bytes_per_timestamp = raw_data["bytes_per_timestamp"]
    num_spikes = raw_data["num_spikes"]
    num_channels = raw_data["num_chans"]
    spike_data = raw_data["spike_data"]
    timebase = raw_data["timebase"]

    big_endian_vector = 256 ** np.arange(bytes_per_timestamp - 1, -1, -1)

    t_start_indices = _compute_time_start_indexes(raw_data)

    t_indices = t_start_indices

    for chan in np.arange(1, num_channels):
        t_indices = np.hstack((t_indices, t_start_indices + chan))

    # acquiring the time bytes
    timestamps = spike_data[t_indices].reshape(num_spikes, bytes_per_timestamp)
    # converting from bytes to float values
    timestamps = np.sum(np.multiply(timestamps, big_endian_vector), axis=1) / timebase

    return xr.DataArray(timestamps.flatten(),
                        dims=['spikes'],
                        attrs={'type': 'spike_times',
                                'units': 's',
                                'dimensionality': 'time'})


def _extract_spike_waveforms(raw_data):
    bytes_per_timestamp = raw_data["bytes_per_timestamp"]
    samples_per_spike = raw_data["samples_per_spike"]
    num_spikes = raw_data["num_spikes"]
    num_channels = raw_data["num_chans"]
    spike_data = raw_data["spike_data"]

    t_start_indices = _compute_time_start_indexes(raw_data)

    # read the raw data formatted in the order: t,ch1,t,ch2,t,ch3,t,ch4

    little_endian_matrix = _compute_little_endian_matrix(raw_data)

    channels = []

    for chan in range(num_channels):  # only really care about the first time that gets written
        chan_start_indices = t_start_indices + bytes_per_timestamp * (chan + 1) + samples_per_spike * chan
        # print(chan_start_indices[0:100])
        for spike_sample in np.arange(1, samples_per_spike):
            chan_start_indices = np.hstack((chan_start_indices, t_start_indices + chan * samples_per_spike + bytes_per_timestamp * (chan+1) + spike_sample))

        # acquiring the channel bytes
        bts = spike_data[chan_start_indices]
        bts = bts.reshape(num_spikes, samples_per_spike).astype('int8')
        channels.append(bts)

        channels[chan][np.where(channels[chan][:][:] > 127)] -= 256
        channels[chan] = np.multiply(channels[chan][:][:],
                                     little_endian_matrix,
                                     dtype=np.float16)

    waveform_data = np.stack(channels, axis=1)

    return xr.DataArray(waveform_data,
                        dims=['spikes', 'channels', 'samples'],
                        attrs={'type': 'spike_waveforms',
                                'units': 'uV',
                                'dimensionality': 'voltage'
                               }
                        )


def _compute_time_start_indexes(raw_data):
    """Computes the indices for the first bit of each timestamp in the raw data
    """
    # calculating the timestamps
    bytes_per_timestamp = raw_data["bytes_per_timestamp"]
    bytes_per_sample = raw_data["bytes_per_sample"]
    samples_per_spike = raw_data["samples_per_spike"]
    num_spikes = raw_data["num_spikes"]
    step = (bytes_per_sample * samples_per_spike * 4 + bytes_per_timestamp * 4)
    t_start_indices = np.arange(0, step*num_spikes, step).astype(int)
    t_start_indices = t_start_indices.reshape(num_spikes, 1)
    return t_start_indices


def _compute_little_endian_matrix(raw_data):
    bps = raw_data["bytes_per_sample"]
    sps = raw_data["samples_per_spike"]
    little_endian_matrix = np.arange(0, bps).reshape(bps, 1)
    little_endian_matrix = 256 ** np.tile(little_endian_matrix, (1, sps))
    return little_endian_matrix

# =============================================================================
# set (settings) Helpers
# =============================================================================
# TODO: Add a function to read the settings file