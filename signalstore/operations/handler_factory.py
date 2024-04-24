import os
import importlib.util

from signalstore.operations.handlers.base_handler import BaseHandler

class HandlerFactory:
    def __init__(self, uow_provider, base_path='signalstore.operations.handlers', base_dir='src/operations/handlers'):
        self._uow_provider = uow_provider
        self.base_path = base_path
        self.base_dir = base_dir
        self._handlers = self._discover_handlers()

    def _discover_handlers(self):
        """Recursively search for handler classes and return a dict of {handler_name: handler_class}."""
        handlers = {}
        for root, dirs, files in os.walk(self.base_dir):
            for file in files:
                if file.endswith('_handler.py') or file.endswith('_handlers.py'):
                    module_path = os.path.join(root, file).replace("/", ".")[:-3]  # Convert UPath to module notation and remove .py
                    for name, obj in vars(importlib.import_module(module_path)).items():
                        if isinstance(obj, type) and issubclass(obj, BaseHandler) and obj != BaseHandler:
                            handlers[name] = obj
        return handlers

    def create(self, handler_name):
        handler_class = self._handlers.get(handler_name)
        if not handler_class:
            raise Exception(f"Handler {handler_name} not found.")
        return handler_class(self._uow_provider)

    def list_handlers(self):
        """List available handlers."""
        return list(self._handlers.keys())

    def get_handler_help(self, handler_name):
        """Get documentation for a specific handler."""
        handler_class = self._handlers.get(handler_name)
        if handler_class:
            return handler_class.__doc__ or f"No documentation available for {handler_name}"
        else:
            raise Exception(f"Handler {handler_name} not found.")