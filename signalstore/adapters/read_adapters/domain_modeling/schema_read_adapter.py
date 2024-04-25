from signalstore.adapters.read_adapters.abstract_read_adapter import AbstractReadAdapter
import json
from pathlib import Path

class SchemaReadAdapter(AbstractReadAdapter):
    def __init__(self, directory):
        self.dir = Path(directory)

    def read(self):
        """Reads JSON files that conform to the Neuroscikit data model schemata.
        """
        for json_filepath in self.dir.glob('*.json'):
            with open(json_filepath) as f:
                yield dict(json.load(f))


