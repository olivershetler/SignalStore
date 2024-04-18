import inspect
from datetime import datetime, timezone
from src.utilities.tools.time import timenow_millis

def get_current_kwargs(frame_offset=0):
    """
    This function inspects the stack frames to retrieve the args passed to the calling function, in
    kwargs form.

    You can pass a `frame_offset` to go higher up the stack than just 1 level.
    """
    outerframe = inspect.getouterframes(inspect.currentframe())[1+frame_offset]
    argspec=inspect.getargvalues(outerframe.frame)
    in_kwargs = {arg_name: argspec.locals[arg_name] for arg_name in argspec.args[1:] }
    return in_kwargs

def get_current_func_name(frame_offset=0):
    """
    This function inspects the stack frames to retrieve the name of the current function.

    You can pass a `frame_offset` to go higher up the stack than just 1 level.
    """
    outerframe = inspect.getouterframes(inspect.currentframe())[1+frame_offset]
    return outerframe.function

def get_current_class_name(frame_offset=0):
    """
    This function inspects the stack frames to retrieve the name of the current class.

    You can pass a `frame_offset` to go higher up the stack than just 1 level.
    """
    outerframe = inspect.getouterframes(inspect.currentframe())[1+frame_offset]
    return outerframe.frame.f_locals["self"].__class__.__name__

class OperationResponse:
    """This class encapsulates information about repository operations (add, delete, etc).
    This information allows us to replay / roll back operations when they fail.
    """

    def __init__(self, class_name: str = None, operation: str = None, kwargs: dict = None, timestamp=None, datetime_override=None, result=None):
        # We pass `frame_offset=1` because we need to go up an extra level to get out of __init__.
        self._class_name = class_name or get_current_class_name(frame_offset=1)
        self._operation_name = operation or get_current_func_name(frame_offset=1)
        self._kwargs = kwargs or get_current_kwargs(frame_offset=1)
        self._datetime = datetime_override or datetime
        self._timestamp = timestamp or timenow_millis(self._datetime)

    @property
    def class_name(self):
        return self._class_name

    @property
    def operation_name(self):
        return self._operation_name

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def result(self):
        return self._result

    @property
    def dict(self):
        dictionary = {
            "class": self.class_name,
            "method": self.operation_name,
            "kwargs": self.kwargs,
            "timestamp": self.timestamp,
        }

def operation_response_factory(class_name: str = None, operation: str = None, kwargs: dict = None, timestamp=None, datetime_override=None):
    return OperationResponse(class_name, operation, kwargs, timestamp, datetime_override).dict