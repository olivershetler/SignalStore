import pynwb.ecephys as nwbp

def to_electrical_series(signal):
    """
    Convert a Signal object to a pynwb ElectricalSeries object.
    """

    channel_group = nwbp.channelGroup(...
    )

    return nwbp.ElectricalSeries(
        name=signal.name,
        data=signal.data,
        channels=channel_group,
        channel_conversion=None,
        filtering=None,
        resolution=-1.0,
        conversion=1.0,
        timestamps=None,
        starting_time=None,
        rate=None,
        comments='no comments',
        description='no description',
        control=None,
        control_description=None,
        offset=0.0
    )