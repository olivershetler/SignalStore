from signalstore.operations.importers.adapters.abstract_read_adapter import AbstractReadAdapter


class IntanReadAdapter(AbstractReadAdapter):
    def __init__(self, path):
        self.path = path

    def read(self):
        """Reads a set of Intan files (RHD, RHS, Spike) and converts each data object into an xarray.DataArray with the appropriate dimensions, coordinates and metadata attributes for the Neuroscikit data model.
        """
        pass


# ================================================================================
# RHD read helper functions
# ================================================================================



# ================================================================================
# RHS helper functions
# ================================================================================



# ================================================================================
# Spike File helper functions
# ================================================================================

