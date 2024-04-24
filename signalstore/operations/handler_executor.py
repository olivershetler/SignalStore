class BaseHandlerExecutor:
    def __init__(self, handler_factory):
        self.handler_factory = handler_factory
    def list_handlers(self):
        return self.handler_factory.list_handlers()

    def help(self, handler_name):
        return self.handler_factory.help(handler_name)

    def help_all(self):
        return self.handler_factory.help_all()

    #def validate_request(self, request):
    #    """Validate a request."""
    #    pass

class HandlerExecutor(BaseHandlerExecutor):
    def __init__(self, handler_factory):
        super().__init__(handler_factory)

    def do(self, handler_name, **kwargs):
        return self.handler_factory.create(handler_name).execute(**kwargs)