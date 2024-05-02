

from abc import ABC, abstractmethod

import xarray as xr

class AbstractUnitOfWork(ABC):

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, type, value, traceback):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

class UnitOfWorkContextError(Exception):
    pass

class UnitOfWork:
    def __init__(self, domain_model_repo, data_repo, in_memory_object_repo):
        self._domain_models = domain_model_repo
        self._data = data_repo
        self._memory = in_memory_object_repo
        self._in_context = False

    @property
    def domain_models(self):
        if not self._in_context:
            raise UnitOfWorkContextError("You must use a UnitOfWork as a context manager, i.e. use 'with unit_of_work as uow:'")
        return self._domain_models

    @property
    def data(self):
        if not self._in_context:
            raise UnitOfWorkContextError("You must use a UnitOfWork as a context manager, i.e. use 'with unit_of_work as uow:'")
        return self._data

    @property
    def memory(self):
        if not self._in_context:
            raise UnitOfWorkContextError("You must use a UnitOfWork as a context manager, i.e. use 'with unit_of_work as uow:'")
        return self._memory

    @property
    def in_conxtext(self):
        return self._in_context

    def __enter__(self):
        # reset the operations history for each repository
        self._in_context = True
        self._clear_operation_history()
        return self

    def __exit__(self, type, value, traceback):
        self.rollback()
        self._in_context = False

    def rollback(self):
        self.domain_models.undo_all()
        self.data.undo_all()
        self.memory.undo_all()

    def commit(self):
        operations = self._get_all_operations()
        self._clear_operation_history()
        return operations

    def purge(self, time_threshold=None):
        self.domain_models.purge(time_threshold)
        self.data.purge(time_threshold)
        self.memory.purge(time_threshold)

    def _clear_operation_history(self):
        self.domain_models.clear_operation_history()
        self.data.clear_operation_history()
        self.memory.clear_operation_history()

    def _get_all_operations(self):
        # (timestamp, report{operation, kwargs, result})
        # TODO document operation_history entries more clearly
        return {
            'domain_models': self.domain_models._operation_history,
            'data': self.data._operation_history,
            'memory': self.memory._operation_history,
        }



