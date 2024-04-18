from abc import ABC, abstractmethod

class AbstractReadAdapter(ABC):

    def __iter__(self):
        return self.read().__iter__()

    def __next__(self):
        return self.read().__next__()

    @abstractmethod
    def read(self):
        raise NotImplementedError('AbstractReadAdapter.read() not implemented.')
