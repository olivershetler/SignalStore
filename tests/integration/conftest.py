from src.dependency_injection.app_container import AppContainer
import pytest
import os
import json
from enum import Enum



@pytest.fixture
def app_container():
    return AppContainer(
        CONFIG={
            "MONGO": {
                "HOST": 'mongodb://mongodb',
                "PORT": '27017',
                },
            "FILESYSTEM": {
                "PROTOCOL": 'gcs',
                "STORAGE_OPTIONS":'{"endpoint": "http://gcs:4443", "bucket": "my-bucket"}'
                },
            })
