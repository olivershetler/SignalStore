from signalstore.store.data_access_objects import (
    MongoDAO,
    FileSystemDAO,
    InMemoryObjectDAO,
)

from signalstore.store.repositories import (
    DomainModelRepository,
    DataRepository,
    InMemoryObjectRepository
)

from signalstore.store.unit_of_work import UnitOfWork

class UnitOfWorkProvider:
    def __init__(self, mongo_client, filesystem, memory_store):
        self._mongo_client = mongo_client
        self._filesystem = filesystem
        self._memory_store = memory_store

    def __call__(self, project_name):
        model_dao = MongoDAO(client=self._mongo_client,
                            database_name=project_name,
                            collection_name='domain_models',
                            index_fields=['schema_name'])

        record_dao = MongoDAO(client=self._mongo_client,
                            database_name=project_name,
                            collection_name='records',
                            index_fields=['schema_ref', 'data_name'])

        file_system_dao = FileSystemDAO(filesystem=self._filesystem,
                                        project_dir=project_name)

        in_memory_object_dao = InMemoryObjectDAO(memory_store=self._memory_store)

        domain_model_repo = DomainModelRepository(model_dao=model_dao)

        data_repo = DataRepository(record_dao=record_dao,
                                file_dao=file_system_dao,
                                domain_repo=domain_model_repo)

        in_memory_object_repo = InMemoryObjectRepository(memory_dao=in_memory_object_dao)

        return UnitOfWork(
            domain_model_repo=domain_model_repo,
            data_repo=data_repo,
            in_memory_object_repo=in_memory_object_repo,
        )