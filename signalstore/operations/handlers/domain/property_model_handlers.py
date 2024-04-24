from signalstoreoperations.handlers.base_handler import BaseHandler, HandlerSuccessResponse, HandlerFailureResponse

import json

class AddPropertyModelFromJSONHandler(BaseHandler):
    """Add a term to the database."""

    def execute(self, term_path):
        with self.uow_provider() as uow:
            try:
                with open(term_path) as f:
                    term = json.load(f)
                    uow.domain_models.add(term)
                effects = uow.commit()
                return HandlerSuccessResponse(None, effects)
            except Exception as e:
                return HandlerFailureResponse(e)

class AddPropertyModelDictHandler(BaseHandler):
    """Add a term to the database."""

    def execute(self, term):
        with self.uow_provider() as uow:
            try:
                uow.domain_models.add(term)
                effects = uow.commit()
                return HandlerSuccessResponse(None, effects)
            except Exception as e:
                return HandlerFailureResponse(e)

class DeletePropertyModelHandler(BaseHandler):
    """Delete a term from the database."""

    def execute(self, name):
        with self.uow_provider() as uow:
            try:
                uow.domain_models.delete(name=name)
                effects = uow.commit()
                return HandlerSuccessResponse(None, effects)
            except Exception as e:
                return HandlerFailureResponse(e)


class GetPropertyModelHandler(BaseHandler):
    """Get a term from the database."""

    def execute(self, name):
        with self.uow_provider() as uow:
            try:
                term = uow.domain_models.get(schema_name=name)
                # check that term is a property model
                if term["schema_type"] != "property_model":
                    term = None
                return HandlerSuccessResponse(term, None)
            except Exception as e:
                return HandlerFailureResponse(e)

class ListPropertyModelsHandler(BaseHandler):
    """List all domain_models in the database."""

    def execute(self):
        with self.uow_provider() as uow:
            try:
                terms = uow.domain_models.query({"schema_type": "property_model"})
                return HandlerSuccessResponse(terms, None)
            except Exception as e:
                return HandlerFailureResponse(e)

class ListPropertyModelsHandler(BaseHandler):
    """List all term names in the database."""

    def execute(self):
        with self.uow_provider() as uow:
            try:
                terms = uow.domain_models.query({"schema_type": "property_model"})
                names = [term["name"] for term in terms]
                return HandlerSuccessResponse(names, None)
            except Exception as e:
                return HandlerFailureResponse(e)
