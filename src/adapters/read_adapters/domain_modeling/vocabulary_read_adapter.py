from src.adapters.read_adapters.abstract_read_adapter import AbstractReadAdapter

import yaml

class VocabularyReadAdapter(AbstractReadAdapter):
    def __init__(self, filepath):
        self.filepath = filepath

    def read(self):
        """Reads a YAML file and converts each data object into an xarray.DataArray with
        the appropriate dimensions, coordinates and metadata attributes for the
        Neuroscikit data model.
        """
        with open(self.filepath) as f:
            yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
        for key, value in yaml_dict.items():
            record = {"name": key}
            record.update(value)
            yield record