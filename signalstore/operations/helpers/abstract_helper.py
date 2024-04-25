from abc import ABC, abstractmethod
from datetime import datetime

class AbstractFunctionalHelper:
    pass

class AbstractMutableHelper:
    def __init__(self, attrs, state):
        if not isinstance(attrs, dict):
            raise TypeError("attrs must be a dictionary")
        if isinstance(state, type(None)):
            raise TypeError("state cannot be None")
        self.attrs = attrs
        self.state = state

    def __copy__(self):
        return self.__class__(**self.__dict__)