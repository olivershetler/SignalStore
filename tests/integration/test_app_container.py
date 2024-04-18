import pytest

@pytest.mark.slow()
def test_app_container_init(app_container):
    assert app_container is not None
    # update app_container.CONFIG with app_config dict
    assert app_container.CONFIG() is not None
    assert app_container.mongo_client() is not None
    assert app_container.filesystem() is not None
    assert app_container.memory_store() is not None
    assert app_container.uow_provider("test") is not None