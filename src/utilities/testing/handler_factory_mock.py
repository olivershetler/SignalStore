from src.utilities.testing.handler_mocks import MockTrainModelHandler, MockPredictModelHandler

class MockHandlerFactory:
    """This class is a mock for the HandlerFactory class. It is used in unit tests to replace the HandlerFactory.
    """
    def __init__(self, uow_provider):
        self._handlers = {
            "train_model": MockTrainModelHandler,
            "predict_model": MockPredictModelHandler
        }
        self._uow_provider = uow_provider

    def create(self, handler_name):
        handler_class = self._handlers.get(handler_name)
        if not handler_class:
            raise Exception(f"Handler {handler_name} not found.")
        return handler_class(self._uow_provider)

    def list_handlers(self):
        """List available handlers."""
        return list(self._handlers.keys())

    def get_handler_help(self, handler_name):
        # returns the doc string for the handler
        handler_class = self._handlers.get(handler_name)
        if handler_class:
            return handler_class.__doc__ or f"No documentation available for {handler_name}"
        else:
            raise Exception(f"Handler {handler_name} not found.")
