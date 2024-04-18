from src.operations.handlers.base_handler import BaseHandler
from src.utilities.testing.helper_mocks import MockModelHelper, MockModel

class MockTrainModelHandler(BaseHandler):
    def __init__(self, model_helper):
        self.helper = model_helper

    def _execute(self, uow, save_checkpoint=False, remember_model=False):
        self.helper.train()
        token = None
        if remember_model:
            uow.remember(self.helper)
        if save_checkpoint:
            token = uow.save_checkpoint(self.helper)
        return token

class MockPredictModelHandler(BaseHandler):
    def __init__(self, model_helper):
        self.helper = model_helper

    def _execute(self, uow, load_token=None, load_most_recent=False):
        if load_token:
            self.helper.load(load_token)
        elif load_most_recent:
            self.helper.load_most_recent()
        return self.helper.predict()