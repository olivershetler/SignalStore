from src.operations.helpers.abstract_helper import AbstractMutableHelper

import os

class MockModel:
    def __init__(self, model_state=0):
        self.model_state = model_state

    def train(self):
        self.model_state += 1

    def predict(self):
        return self.model_state


class MockModelHelper(AbstractMutableHelper):
    def __init__(self, model):
        self.model = model

    def train(self):
        self.model.train()

    def predict(self):
        return self.model.predict()

    def save(self, directory, checkpoint_token):
        # save the model state to a text file
        filepath = f'{directory}/{checkpoint_token}'
        with open(filepath, 'w') as f:
            f.write(str(self.model.model_state))

    def load(self, directory, **kwargs):
        # load the model state from a text file
        filepath = f'{directory}/{kwargs["checkpoint_token"]}'
        with open(filepath, 'r') as f:
            self.model.model_state = int(f.read())
        return self.model


class MockObjectHelper(AbstractMutableHelper):
    def __init__(self, object):
        self._object = object

    def save(self, directory, name):
        filepath = f'{directory}/{name}'
        with open(filepath, 'w') as f:
            f.write(str(self._object))

    def load(self, directory, name):
        filepath = f'{directory}/{name}'
        print(filepath)
        with open(filepath, 'r') as f:
            self._object = str(f.read())
        return self._object

