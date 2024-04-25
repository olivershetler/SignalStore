import neo.core as neoc

def to_analog_signal(signal):
    st = to_seconds(signal.attrs['start_time'])
    et = to_seconds(signal.attrs['end_time'])
    sampling_period = (et - st)
    annotations = {key: value for key, value in signal.attrs.copy() if key not in ['start_time', 'end_time', 'sampling_rate']}
    return neoc.AnalogSignal(signal.data,
                           units=None,
                           dtype=type(signal.data),
                           copy=True,
                           t_start=signal.attrs['start_time'],
                           sampling_rate=signal.attrs['sampling_rate'],
                           sampling_period=sampling_period,
                           name=signal.attrs['name'],
                           file_origin=None,
                           description=None,
                           array_annotations=None
                           **annotations)


