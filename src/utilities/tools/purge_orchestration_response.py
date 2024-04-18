from src.utilities.tools.operation_response import OperationResponse

class PurgeOrchestrationResponse:

    def __init__(self, op_success: bool, op_response: OperationResponse = None, op_exception: Exception = None):
        self._op_success = op_success
        self._op_response = op_response
        self._op_exception = op_exception

    @property
    def op_success(self):
        return self._op_success

    @property
    def op_response(self):
        return self._op_response

    @property
    def op_exception(self):
        return self._op_exception

