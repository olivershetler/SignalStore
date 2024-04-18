from src.operations.importers.adapters.abstract_read_adapter import AbstractReadAdapter

#import pynwb

class NeurodataWithoutBordersReadAdapter(AbstractReadAdapter):
    def __init__(self, path):
        self.path = path

    def read(self):
        """Reads a NWB file and converts each data object into an xarray.DataArray with
        the appropriate dimensions, coordinates and metadata attributes for the
        Neuroscikit data model.
        """
        pass