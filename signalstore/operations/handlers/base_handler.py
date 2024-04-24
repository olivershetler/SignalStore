from abc import ABC, abstractmethod
from pydantic import BaseModel

class BaseHandler(ABC):
    def __init__(self, unit_of_work_provider, project_name):
        self._uow_provider = unit_of_work_provider
        self._project_name = project_name

    @property
    def unit_of_work(self):
        return self._uow_provider(self._project_name)

    @abstractmethod
    def execute(self):
        # use with self.unit_of_work() as uow:
        pass

class BaseHandlerResponse(BaseModel):
    status: str
    result: dict
    effects: dict
    error: str

class SmallHandlerResponse(BaseHandlerResponse):
    __slots__ = ["status", "result", "effects", "error"]
    def __init__(self, status, result, effects, error):
        self.status = status
        self.result = result # Note that this is a dictionary and the structure of the result must be included in the docstrings of each handler
        self.effects = effects
        self.error = error

    def __repr__(self):
        return f"HandlerResponse(status={self.status}, result={self.result}, effects={self.effects}, error={self.error})"

    def __str__(self):
        return f"HandlerResponse(status={self.status} result={self.result} effects={self.effects} error={self.error})"

class SmallHandlerSuccessResponse(SmallHandlerResponse):
    def __init__(self, result, effects):
        super().__init__("SUCCESS", result, effects, None)

class SmallHandlerFailureResponse(SmallHandlerResponse):
    def __init__(self, error):
        super().__init__("FAILURE", None, None, error)