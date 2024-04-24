from abc import ABC, abstractmethod
from datetime import datetime
import os
import glob
import re
import xarray as xr
import numpy as np
import fsspec
import gc # import garbage collector for memory management
import json
import traceback

from signalstore.store.store_errors import *

from signalstore.store.datafile_adapters import AbstractDataFileAdapter, XarrayDataArrayNetCDFAdapter

class AbstractDataAccessObject(ABC):

    @abstractmethod
    def get(self):
        """Get a single object."""
        pass

    @abstractmethod
    def exists(self):
        """Check if an object exists."""
        pass

    @abstractmethod
    def add(self):
        """Add a single object."""
        pass

    @abstractmethod
    def mark_for_deletion(self):
        """Mark a single object for deletion."""
        pass

    @abstractmethod
    def restore(self):
        """Restore the most recent version of a single object."""
        pass

    @abstractmethod
    def list_marked_for_deletion(self):
        """List objects marked for deletion."""
        pass

    @abstractmethod
    def purge(self):
        """Purge (permanently delete) objects marked for deletion."""
        pass

class AbstractQueriableDataAccessObject(AbstractDataAccessObject):
    @abstractmethod
    def find(self):
        """Apply filtering to get multiple objects fitting a description."""
        pass


# ===================
# Data Access Objects
# ===================
class MongoDAOTypeError(TypeError):
    pass

class MongoDAODocumentNotFoundError(NotFoundError):
    pass

class MongoDAODocumentAlreadyExistsError(AlreadyExistsError):
    pass

class MongoDAORangeError(RangeError):
    pass

class MongoDAOArgumentNameError(ArgumentNameError):
    pass

class MongoDAOUncaughtError(UncaughtError):
    pass

class MongoDAO(AbstractQueriableDataAccessObject):
    """This class is a base class for accessing MongoDB documents.
    It implements the basic CRUD operations in a way that simplifies the
    implementation of specific data access objects for different types of
    documents.
    """
    def __init__(self,
                 client,
                 database_name: str,
                 collection_name: str,
                 index_fields: list,
                 ):
        """Initializes the data Base MongoDB Data Access Object."""
        self._client = client # mongoDB client
        self._db = client[database_name] # mongoDB database
        self._collection = self._db[collection_name] # mongoDB collection
        # index by index_fields
        self._collection_name = collection_name
        self._index_args = set(index_fields)
        index_field_tuples = [(field, 1) for field in index_fields]
        # add time_of_removal field to index
        index_field_tuples.append(('version_timestamp', 1))
        index_field_tuples.append(('time_of_removal', 1))
        self._collection.create_index(index_field_tuples, unique=True) # create index
        # set argument types dictionary for checking argument types
        self._argument_types = {
            'version_timestamp': (type(datetime.utcnow())),
            'filter': (dict, type(None)),
            'projection': (dict, type(None)),
            'timestamp': (type(datetime.utcnow())),
            'time_threshold': (type(datetime.utcnow()), type(None)),
            'nth_most_recent': (int),
            'override_existing_document': (bool),
            'not_exist_ok': (bool),
            'document': (dict),
        }
        # set all the index names as argument options with string type
        for field in index_fields:
            self._argument_types[field] = (str) # only string type because they can never be None

    def get(self, version_timestamp=None, **kwargs):
        """Gets a document from the repository.
        Arguments:
            **kwargs {dict} -- Only the index fields are allowed as keyword arguments.
        Raises:
            NotFoundError -- If the document is not found.
        Returns:
            dict -- The document.
        """
        self._check_kwargs_are_only_index_args(**kwargs)
        self._check_args(version_timestamp=version_timestamp,**kwargs)
        document = self._collection.find_one({'time_of_removal': None,
                                              'version_timestamp': version_timestamp,
                                              **kwargs},
                                              {'_id': 0})
        if document is None:
            return None
        else:
            return self._deserialize(document)

    def find(self, filter=None, projection=None, **kwargs):
        """Returns a filtered list of documents from the repository.
        Arguments:
            filter {dict} -- The filter to apply to the query.
        Returns:
            list[dict] -- The list of documents.
        """
        self._check_args(filter=filter, projection=projection)
        if filter is None:
            filter = {"time_of_removal": None}
        else:
            filter = filter.copy() # avoid mutations to the input dict
            filter["time_of_removal"] = None
        if projection is None:
            projection = {'_id': 0}
        else:
            projection = projection.copy() # avoid mutations to the input dict
            projection['_id'] = 0
        documents = [self._deserialize(document) for document in self._collection.find(filter, projection, **kwargs)]
        return documents

    def exists(self, version_timestamp=None, **kwargs):
        self._check_kwargs_are_only_index_args(**kwargs)
        self._check_args(**kwargs)
        return self._collection.find_one({'time_of_removal': None,
                                          'version_timestamp':version_timestamp,
                                           **kwargs}) is not None

    def add(self, document, timestamp, versioning_on=False):
        """Adds a document to the repository.
        Arguments:
            document {dict} -- The document to add.
            timestamp {datetime.timestamp} -- The timestamp to add the document with.
        Raises:
            MongoDAODocumentAlreadyExistsError -- If the document already exists.
        Returns:
            None
        """
        self._check_args(document=document, timestamp=timestamp)
        # get the index fields from the document
        document_index_args = {key: value for key, value in document.items() if key in self._index_args}
        if self.exists(**document_index_args):
            raise MongoDAODocumentAlreadyExistsError(
                f'Cannot add document with index fields {document} because it already exists in repository.'
            )
        document = document.copy()
        document['time_of_save'] = timestamp
        document['time_of_removal'] = None
        if versioning_on:
            document['version_timestamp'] = timestamp
        result = self._collection.insert_one(self._serialize(document))
        return None

    def mark_for_deletion(self, timestamp, version_timestamp=None, **kwargs):
        """Marks a document for deletion.
        Arguments:
            timestamp {datetime.timestamp} -- The timestamp to mark the document for deletion with.
            **kwargs {dict} -- Only the index fields are allowed as keyword arguments.
        Raises:
            MongoDAODocumentNotFoundError -- If the document does not exist.
        Returns:
            None
        """
        self._check_args(timestamp=timestamp, version_timestamp=version_timestamp, **kwargs)
        self._check_kwargs_are_only_index_args(**kwargs)
        document = self.get(**kwargs, version_timestamp=version_timestamp)
        if document is None:
            raise MongoDAODocumentNotFoundError(
                    f'Cannot delete document with index fields {kwargs} and version_timestamp {version_timestamp} because it does not exist in repository.'
            )
        else:
            self._collection.update_one({'time_of_removal': None, **kwargs}, {'$set':{"time_of_removal": datetime_to_microseconds(timestamp)}})
            return None

    def list_marked_for_deletion(self, time_threshold=None):
        """Returns a list of all deleted documents from the repository."""
        self._check_args(time_threshold=time_threshold)
        if time_threshold is None:
            documents = [self._deserialize(document) for document in self._collection.find({"time_of_removal": {"$ne": None}}, {'_id': 0}, sort=[('time_of_removal', -1)])]
        else:
            documents = [self._deserialize(document) for document in self._collection.find({"time_of_removal":
            {"$lt": datetime_to_microseconds(time_threshold)}},
            {'_id': 0},
            sort=[('time_of_removal', -1)])]
        return documents

    def restore(self, timestamp, nth_most_recent=1,**kwargs):
        """Restores a document.
        Arguments:
            timestamp {datetime.timestamp} -- The timestamp to restore the document with.
            nth_most_recent {int} -- The nth most recent version of the deleted document to restore.
            override_existing_document {bool} -- If True, overrides an existing document with the same index fields.
            **kwargs {dict} -- Only the index fields are allowed as keyword arguments.
        Raises:
            MongoDAODocumentNotFoundError -- If the document does not exist.
            MongoDAODocumentAlreadyExistsError -- If the document already exists and override_existing_document is False.
            MongoDAORangeError -- If the nth_most_recent argument is out of range.
        Returns:
        """
        self._check_args(timestamp=timestamp, nth_most_recent=nth_most_recent, **kwargs)
        self._check_kwargs_are_only_index_args(**kwargs)
        # get the nth most recent version of the document with a numeric time_of_removal value
        documents = self._collection.find({'time_of_removal': {'$ne': None}, **kwargs}, {'_id': 0}, sort=[('time_of_removal', 1)])
        if len(list(documents)) == 0:
            raise MongoDAORangeError(
                f'Cannot restore document with index fields {kwargs}: no deleted instances of {kwargs} were found in repository.'
            )
        # check for an existing document with the same index fields and no time_of_removal value
        document_exists = self.exists(**kwargs)
        if document_exists:
            raise MongoDAODocumentAlreadyExistsError(
                f'Cannot restore document with index fields {kwargs} because it already exists in repository.'
            )
        # restore the document by adding it with a time_of_removal value of None and a time_of_save value of the current time
        try:
            nth_document = documents[nth_most_recent-1]
            nth_doc_kwargs = {key: value for key, value in nth_document.items() if key in self._index_args}
        except IndexError:
            raise MongoDAORangeError(
                f'Arg nth_most_recent={nth_most_recent} out of range. The record of deleted documents only contains {len(list(documents))} entries.'
                )
        # update the time_of_removal field to None
        self._collection.update_one({'time_of_removal': nth_document['time_of_removal'], **nth_doc_kwargs}, {'$set':{"time_of_removal": None}})
        return None


    def purge(self, time_threshold=None):
        """Purges deleted documents from the repository older than the time threshold."""
        self._check_args(time_threshold=time_threshold)
        if time_threshold is None:
            result = self._collection.delete_many({"time_of_removal": {"$ne": None}})
            count = result.deleted_count
        else:
            result = self._collection.delete_many({"time_of_removal": {"$lt": datetime_to_microseconds(time_threshold)}})
            count = result.deleted_count
        return count

    def _check_args(self, **kwargs):
        for key, value in kwargs.items():
            try:
                arg_types = self._argument_types[key]
            except KeyError:
                raise MongoDAOArgumentNameError(
                    f'Invalid keyword argument name {key}.'
                )
            if not isinstance(value, arg_types) and value is not None:
                raise MongoDAOTypeError(
                    f'Invalid type {type(value)} for argument {key}. Must be one of {arg_types}.'
                )

    def _check_kwargs_are_only_index_args(self, **kwargs):
        if not self._index_args == set(kwargs.keys()):
            raise MongoDAOArgumentNameError(
                f'Invalid keyword arguments.\nRequired arguments: {self._index_args}.\nOptional arguments: None.'
            )

    def _serialize(self, document):
        """Serializes a document object.
        Arguments:
            document {dict} -- The document object to serialize.
        Returns:
            dict -- The serialized document.
        """
        result = document.copy()
        for key, value in document.items():
            serializer = self.property_serializers.get(key)
            if serializer is not None:
                try:
                    result[key] = self.property_serializers[key](value)
                except Exception as e:
                    raise MongoDAOUncaughtError(
                        f'An error occurred while serializing property {key}\n\nof document {document}.\n\nThe error was: {e}'
                    )
        return result

    def _deserialize(self, document):
        """Deserializes a document object.
        Arguments:
            document {dict} -- The document object to deserialize.
        Returns:
            dict -- The deserialized document.
        """
        result = document.copy()
        for key, value in result.items():
            deserializer = self.property_deserializers.get(key)
            if deserializer is not None:
                try:
                    result[key] = self.property_deserializers[key](value)
                except Exception as e:
                    raise MongoDAOUncaughtError(
                        f'An uncaught error occurred while deserializing property: "{key}" from document: \n\n{document}.\n\nThe error was: {e}'
                    )
        return result

    @property
    def property_serializers(self):
        return {
            'time_of_save': datetime_to_microseconds,
            'time_of_removal': datetime_to_microseconds,
            'json_schema': dict_to_json_bytes,
        }

    @property
    def property_deserializers(self):
        return {
            'time_of_save': microseconds_to_datetime,
            'time_of_removal': microseconds_to_datetime,
            'json_schema': json_bytes_to_dict,
        }

    @property
    def collection_name(self):
        return self._collection_name

# ===================

class FileSystemDAOConfigError(ConfigError):
    pass

class FileSystemDAOTypeError(ArgumentTypeError):
    pass

class FileSystemDAOFileNotFoundError(NotFoundError):
    pass

class FileSystemDAOFileAlreadyExistsError(AlreadyExistsError):
    pass

class FileSystemDAORangeError(RangeError):
    pass

class FileSystemDAOArgumentNameError(ArgumentNameError):
    pass

class FileSystemDAOUncaughtError(UncaughtError):
    pass

class FileSystemDAO(AbstractDataAccessObject):
    def __init__(self, filesystem, project_dir, default_data_adapter=XarrayDataArrayNetCDFAdapter()):
        # add / to end of directory if it doesn't already exist
        self._fs = filesystem
        # make sure the project directory exists
        if not self._fs.exists(project_dir):
            self._fs.mkdir(project_dir)
        self._directory = project_dir
        default_data_adapter.set_filesystem(self._fs)
        self._default_data_adapter = default_data_adapter

    def get(self, schema_ref, data_name, version_timestamp=None, nth_most_recent=1, data_adapter=None):
        """Gets an object from the repository.
        Arguments:
            schema_ref {str} -- The type of object to get.
            data_name {str} -- The name of the object to get.
            version_timestamp {str} -- The version_timestamp of the object to get.
        Raises:
            FileSystemDAOFileNotFoundError -- If the object is not found.
        Returns:
            dict -- The object.
        """
        self._check_args(schema_ref=schema_ref,
                         data_name=data_name,
                         nth_most_recent=nth_most_recent,
                         version_timestamp=version_timestamp,
                         data_adapter=data_adapter)
        if data_adapter is None:
            data_adapter = self._default_data_adapter
        else:
            data_adapter.set_filesystem(self._fs)
        path = self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter)
        if not self._fs.exists(path):
            # if the version_timestamp was specified, that's the only version we want to get
            # if it doesn't exist, we return None
            if version_timestamp is not None:
                return None
            basename = self.make_base_filename(schema_ref, data_name)
            pattern = self._directory + '/' + basename + '*_version_*' + data_adapter.file_extension
            glob = self._fs.glob(pattern)
            # filter out the paths that have a time_of_removal value
            paths = list(filter(lambda path: '_time_of_removal_' not in path, glob))
            if len(paths) == 0:
                return None
            else:
                # get the most recent version
                paths.sort()
                try:
                    path = paths[-nth_most_recent]
                except IndexError:
                    return None
        data_object = data_adapter.read_file(path)
        data_object = self._deserialize(data_object)
        return data_object

    def exists(self, schema_ref, data_name, version_timestamp=None, data_adapter=None):
        """Checks if an object exists in the repository.
        Arguments:
            schema_ref {str} -- The type of object to check.
            data_name {str} -- The name of the object to check.
            version_timestamp {str} -- The version of the object to check.
        Returns:
            bool -- True if the object exists, else False.
        """
        self._check_args(schema_ref=schema_ref, data_name=data_name, version_timestamp=version_timestamp, data_adapter=data_adapter)
        path = self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter)
        return self._fs.exists(path)

    def n_versions(self, schema_ref, data_name):
        """Returns the number of versions of an object in the repository."""
        self._check_args(schema_ref=schema_ref, data_name=data_name)
        pattern = self.make_base_filename(schema_ref, data_name) + '*'
        globstring = self._directory + '/' + pattern
        glob = self._fs.glob(globstring)
        # filter out the paths that have a time_of_removal value
        paths = filter(lambda path: '_time_of_removal_' not in path, glob)
        return len(list(paths))

    def add(self, data_object, data_adapter=None):
        """Adds an object to the repository.
        Arguments:
            data_object -- The object to add.
            data_adapter -- The data adapter to use.
                            If none, the default data adapter is used.
                            The default data adapter is given to the constructor.
        Raises:
            FileSystemDAOFileAlreadyExistsError -- If the object already exists.
        Returns:
            None
        """
        self._check_args(data_adapter=data_adapter)
        if data_adapter is None:
            data_adapter = self._default_data_adapter
        else:
            data_adapter.set_filesystem(self._fs)
        # separately check object using data adapter
        if not isinstance(data_object, data_adapter.data_object_type):
            raise FileSystemDAOTypeError(
                f"Type mismatch: Received {type(data_object).__name__}, but expected {data_adapter.data_object_type.__name__}.Ensure 'data_object' matches the type required by the current 'data_adapter'. If you're using the default data adapter, it may not be compatible with 'data_object'. Consider specifying a different data adapter that accepts {type(data_object).__name__}. Current data_adapter type: {type(data_adapter).__name__}."
            )
        idkwargs = data_adapter.get_id_kwargs(data_object) # (schema_ref, data_name, version_timestamp)
        path = self.make_filepath(**idkwargs, data_adapter=data_adapter)
        if self.exists(**idkwargs, data_adapter=data_adapter):
            raise FileSystemDAOFileAlreadyExistsError(
                f'Cannot add object with path "{path}" because it already exists in repository.'
            )
        data_object = self._serialize(data_object)
        data_adapter.write_file(path=path, data_object=data_object)
        self._deserialize(data_object) # undo the serialization in case the object is mutated
        return None

    def mark_for_deletion(self, schema_ref, data_name, time_of_removal, version_timestamp=None, data_adapter=None):
        """Marks an object for deletion.
        Arguments:
            schema_ref {str} -- The type of object to mark for deletion.
            data_name {str} -- The name of the object to mark for deletion.
            time_of_removal {datetime.timestamp} -- The timestamp to mark the object for deletion with.
            version_timestamp {str} -- The version_timestamp of the object to mark for deletion.
            data_adapter {AbstractDataFileAdapter} -- The data adapter to use.
                                                      If none, the default data adapter is used.
                                                      The default data adapter is given to the constructor.
        Raises:
            FileSystemDAOFileNotFoundError -- If the object does not exist.
        Returns:
            None
        """
        self._check_args(time_of_removal=time_of_removal,
                         schema_ref=schema_ref,
                         data_name=data_name,
                         version_timestamp=version_timestamp,
                         data_adapter=data_adapter)
        if not self.exists(schema_ref, data_name, version_timestamp, data_adapter):
            raise FileSystemDAOFileNotFoundError(
                    f'Cannot remove object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp} because it does not exist in repository. The path would have been {self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter)} if this error had not occurred. The exists method returned False.'
            )
        old_path = self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter)
        new_path = self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter, time_of_removal)
        if self._fs.exists(new_path):
            raise FileSystemDAOFileAlreadyExistsError(
                f'Cannot mark object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp} as marked for deletion because the path {new_path} already exists in repository. The time_of_removal may need to be updated to a more recent time.'
            )
        # check if path is file or directory
        try:
            self._fs.mv(path1=str(old_path), path2=str(new_path), recursive=True)
        except Exception as e:
            trace = traceback.format_exc()
            raise FileSystemDAOUncaughtError(
                f'An error occurred while renaming the object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp} as marked for deletion. The old path was {old_path} and the new (trash) path was going to be {new_path} The error was: {e} and the traceback was \n\n{trace}'
            )
        return None

    def list_marked_for_deletion(self, time_threshold=None, data_adapter=None):
        """Returns a list of all deleted objects from the repository.
        Arguments:
            time_threshold {datetime.timestamp} -- The time threshold.
            data_adapter {AbstractDataFileAdapter} -- The data adapter to use.
        Returns:
            list[dict] -- The list of deleted objects.
        """
        self._check_args(time_threshold=time_threshold,
                         data_adapter=data_adapter)
        if data_adapter is None:
            data_adapter = self._default_data_adapter
        glob_pattern = self._directory + '/*_time_of_removal_*' + data_adapter.file_extension
        paths = self._fs.glob(glob_pattern)
        if time_threshold is None:
            result = list(paths)
        else:
            result = []
            for path in paths:
                tor = self._get_time_of_removal_from_path(path)
                if tor <= time_threshold:
                    result.append(path)
        return result


    def restore(self, schema_ref, data_name, version_timestamp=None, nth_most_recent=1, data_adapter=None):
        """Restores an object.
        Arguments:
            schema_ref {str} -- The type of object to restore.
            data_name {str} -- The name of the object to restore.
            version_timestamp {str} -- The version of the object to restore.
            time_of_removal {datetime.timestamp} -- The timestamp to restore the object with.
            nth_most_recent {int} -- The nth most recent version of the object to restore.
            data_adapter {AbstractDataFileAdapter} -- The data adapter to use.
        Raises:
            FileSystemDAOFileNotFoundError -- If the object does not exist.
            FileSystemDAOFileAlreadyExistsError -- If the object already exists and override_existing_object is False.
            FileSystemDAORangeError -- If the nth_most_recent argument is out of range.
        Returns:
            None
        """
        self._check_args(schema_ref=schema_ref,
                         data_name=data_name,
                         version_timestamp=version_timestamp,
                         nth_most_recent=nth_most_recent,
                         data_adapter=data_adapter)
        if not nth_most_recent > 0:
            raise FileSystemDAORangeError(
                f'Arg nth_most_recent={nth_most_recent} out of range. Must be greater than 0.'
            )
        if data_adapter is None:
            data_adapter = self._default_data_adapter
        # get the nth most recent version of the object with a numeric time_of_removal value
        basefilename = self.make_base_filename(schema_ref, data_name, version_timestamp)
        pattern = self._directory + "/" + basefilename + '*_time_of_removal_*' + data_adapter.file_extension
        glob = self._fs.glob(pattern)
        paths = list(sorted(glob))
        if len(paths) == 0:
            raise FileSystemDAORangeError(
                f'Cannot restore object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp}: no deleted instances of {schema_ref}, {data_name}, and {version_timestamp} were found in repository.'
            )
        # check for an existing object with the same data_name and no time_of_removal value
        object_exists = self.exists(schema_ref, data_name, version_timestamp, data_adapter)
        if object_exists:
            raise FileSystemDAOFileAlreadyExistsError(
                f'Cannot restore object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp} because it already exists in repository.'
            )
        # restore the object by adding it with a time_of_removal value of None and a time_of_save value of the current time
        try:
            nth_path = paths[-nth_most_recent]
        except IndexError:
            raise FileSystemDAORangeError(
                f'Arg nth_most_recent={nth_most_recent} out of range. The record of deleted objects only contains {len(paths)} entries.'
            )
        # set path to the new path
        new_path = self.make_filepath(schema_ref, data_name, version_timestamp, data_adapter)
        try:
            self._fs.mv(str(nth_path), str(new_path), recursive=True)
        except Exception as e:
            raise FileSystemDAOUncaughtError(
                f'An error occurred while moving object with schema_ref: {schema_ref}, data_name: {data_name}, and version_timestamp: {version_timestamp} to the trash. The error was: {e}'
            )
        return None

    def purge(self, time_threshold=None, data_adapter=None):
        """Purges deleted objects from the repository older than the time threshold.
        Arguments:
            schema_ref {str} -- The type of object to purge.
            data_name {str} -- The name of the object to purge.
            version_timestamp {str} -- The version of the object to purge.
            time_threshold {datetime.timestamp} -- The time threshold.
        Returns:
            None
        """
        self._check_args(time_threshold=time_threshold)
        paths = self.list_marked_for_deletion(time_threshold, data_adapter)
        count = len(paths)
        for path in paths:
            self._fs.rm(path, recursive=True)
        return count


    def make_filepath(self, schema_ref, data_name, version_timestamp=None, data_adapter = None, time_of_removal=None):
        """Returns the filepath for a data array."""
        if data_adapter is None:
            data_adapter = self._default_data_adapter
        else:
            data_adapter.set_filesystem(self._fs)
        basename = self.make_base_filename(schema_ref, data_name, version_timestamp)
        if time_of_removal is not None:
            basename += f"__time_of_removal_{datetime_to_microseconds(time_of_removal)}"
        filename = f"/{basename}{data_adapter.file_extension}"
        return self._directory + filename

    def make_base_filename(self, schema_ref, data_name, version_timestamp=None):
        """Returns the base filename for a data array."""
        basename = f"{schema_ref}__{data_name}"
        if version_timestamp is not None:
            basename += f"__version_{datetime_to_microseconds(version_timestamp)}"
        return basename

    def _serialize(self, data_object):
        """Serializes a data object.
        Arguments:
            data_object {dict} -- The data object to serialize.
        Returns:
            dict -- The serialized data object.
        """
        attrs = data_object.attrs.copy()
        for key, value in attrs.items():
            if isinstance(value, bool):
                attrs[key] = str(value)
            if isinstance(value, type(None)):
                attrs[key] = 'None'
            if isinstance(value, dict):
                attrs[key] = json.dumps(value)
            if isinstance(value, list):
                attrs[key] = np.array(value)
        data_object.attrs = attrs
        return data_object

    def _deserialize(self, data_object):
        """Deserializes a data object.
        Arguments:
            data_object {dict} -- The data object to deserialize.
        Returns:
            dict -- The deserialized data object.
        """
        attrs = data_object.attrs.copy()
        for key, value in attrs.items():
            if isinstance(value, str):
                if value == 'True':
                    attrs[key] = True
                elif value == 'False':
                    attrs[key] = False
                elif value == 'None':
                    attrs[key] = None
                elif value.startswith('{'):
                    attrs[key] = json.loads(value)
            elif isinstance(value, np.ndarray):
                attrs[key] = value.tolist()
                ak = attrs[key]
                print(f"TOLIST?: {ak}", flush=True)
        data_object.attrs = attrs
        return data_object


    def _get_time_of_removal_from_path(self, path):
        """Returns the time of removal from a path."""
        filename = os.path.basename(path)
        match = re.search(r'__time_of_removal_(\d+)', filename)
        if match is None:
            raise FileSystemDAOUncaughtError(
                f'An error occurred while parsing the time_of_removal from path {path}.'
            )
        return microseconds_to_datetime(int(match.group(1)))

    def _check_args(self, **kwargs):
        for key, value in kwargs.items():
            try:
                arg_types = self._argument_types[key]
            except KeyError:
                raise FileSystemDAOArgumentNameError(
                    f'Invalid keyword argument data_name {key}.'
                )
            if not isinstance(value, arg_types):
                raise FileSystemDAOTypeError(
                    f'Invalid type {type(value)} for argument {key}. Must be one of {arg_types}.'
                )

    @property
    def _argument_types(self):
        nonetype = type(None)
        nowtype = type(datetime.utcnow())
        return {
            'schema_ref': (str),
            'data_name': (str),
            'version_timestamp': (nowtype, nonetype),
            'time_of_removal': (nowtype, nonetype),
            'nth_most_recent': (int),
            'time_threshold': (nowtype, nonetype),
            'data_adapter': (AbstractDataFileAdapter, nonetype),
        }



# ===================

class InMemoryObjectDAOObjectNotFoundError(NotFoundError):
    pass

class InMemoryObjectDAOObjectAlreadyExistsError(Exception):
    pass

class InMemoryObjectDAOTypeError(TypeError):
    pass

class InMemoryObjectDAORangeError(IndexError):
    pass

class InMemoryObjectDAOArgumentNameError(ArgumentNameError):
    pass

class InMemoryObjectDAOUncaughtError(UncaughtError):
    pass

class InMemoryObjectDAO(AbstractQueriableDataAccessObject):
    def __init__(self, memory_store: dict):
        self._collection = memory_store
        self._collection['objects'] = {}
        self._collection['tags'] = {}
        self._collection['removed'] = {}

    @property
    def collection(self):
        return self._collection

    def get(self, tag: str):
        """Gets an object from the repository.
        Arguments:
            tag {str} -- The tag of the object to get.
        Raises:
            InMemoryObjectDAONotFoundError -- If the object is not found.
        Returns:
            dict -- The object.
        """
        self._check_args(tag=tag)
        id = self._collection['tags'].get(tag)
        if id is None:
            return None
        else:
            return self._collection['objects'].get(id)

    def exists(self, tag: str):
        """Checks if an object exists in the repository.
        Arguments:
            schema_ref {str} -- The type of the object to check.
            data_name {str} -- The name of the object to check.
        Returns:
            bool -- True if the object exists, else False.
        """
        self._check_args(tag=tag)
        return self.get(tag) is not None

    def find(self, key_filter=None, value_filter=None, filter_relation: str='and'):
        """Returns a filtered list of objects from the repository.
        Arguments:
            filter {function} -- The filter to apply to the query.
        Returns:
            list[dict] -- The list of objects.
        """
        self._check_args(key_filter=key_filter, value_filter=value_filter, filter_relation=filter_relation)
        if key_filter is None and value_filter is None:
            filter_relation = 'none'
        elif key_filter is None and value_filter is not None:
            filter_relation = 'value'
        elif key_filter is not None and value_filter is None:
            filter_relation = 'key'
        elif key_filter is not None and value_filter is not None:
            filter_relation = filter_relation.lower()
        item_filter_options = {
            'none': lambda item: True,
            'key': lambda item: key_filter(item[0]),
            'value': lambda item: value_filter(item[1]),
            'and': lambda item: key_filter(item[0]) and value_filter(item[1]),
            'or': lambda item: key_filter(item[0]) or value_filter(item[1]),
            'xor': lambda item: key_filter(item[0]) ^ value_filter(item[1]),
            'nand': lambda item: not (key_filter(item[0]) and value_filter(item[1])),
            'nor': lambda item: not (key_filter(item[0]) or value_filter(item[1])),
            'xnor': lambda item: not (key_filter(item[0]) ^ value_filter(item[1])),
        }
        item_filter = item_filter_options[filter_relation]
        filtered_collection = filter(item_filter, self._collection.items())
        return dict(filtered_collection)

    def add(self, object, tag: str):
        """Adds an object to the repository.
        Arguments:
            object {Any} -- The object to add.
            tag {str} -- The tag to add the object with.
        Raises:
            InMemoryObjectDAOObjectAlreadyExistsError -- If the object already exists.
        Returns:
            None
        """
        self._check_args(tag=tag)
        if self.exists(tag):
            raise InMemoryObjectDAOObjectAlreadyExistsError(
                f'Cannot add object with tag {tag} and id {id(object)} because it already exists in object repository.'
            )
        # check if tag is already in use
        if tag in self._collection['tags']:
            raise InMemoryObjectDAOObjectAlreadyExistsError(
                f'Cannot add object with tag {tag} because the tag already exists in object repository. Use a different tag.'
            )
        # check if id(object) is already in use
        if id(object) in self._collection['objects']:
            raise InMemoryObjectDAOObjectAlreadyExistsError(
                f'Cannot add object with id {id(object)} because it already exists in object repository. This tag is not taken, but you must use a different object.'
            )
        self._collection['objects'][id(object)] = object
        self._collection['tags'][tag] = id(object)
        return None

    def mark_for_deletion(self, tag, time_of_removal: datetime):
        """Marks an object for deletion.
        Arguments:
            schema_ref {str} -- The type of the object to mark for deletion.
            data_name {str} -- The name of the object to mark for deletion.
            timestamp {datetime} -- The timestamp to mark the object for deletion with.
            not_exist_ok {bool} -- If True, no error is raised if the object does not exist.
        Raises:
            InMemoryObjectDAONotFoundError -- If the object does not exist.
        Returns:
            None
        """
        self._check_args(tag=tag, time_of_removal=time_of_removal)
        if not self.exists(tag):
            raise InMemoryObjectDAOObjectNotFoundError(
                    f'Cannot delete object with tag {tag} because it does not exist in object repository.'
            )
        # check if key is already in use
        if time_of_removal in self._collection['removed'].keys():
            entry = self._collection['removed'][time_of_removal]
            raise InMemoryObjectDAOUncaughtError(
                f"Cannot mark object with tag {tag} for deletion because {entry['tag']} has already been marked for deletion with the exact same timestamp ({time_of_removal}). If you received this error, something is wrong with the package. Pleae reach out to the package maintainer."
            )
        # Add the object id to the removed dict using the key
        self._collection['removed'][tag] = {'tag': tag, 'time_of_removal': time_of_removal, 'id': self._collection['tags'][tag]}
        # Remove the object id from the objects dict; note that the 'objects' collection is not modified
        del self._collection['tags'][tag]
        return None

    def list_marked_for_deletion(self, time_threshold: datetime=None):
        """Returns an alphabetically, then time sorted list of deleted objects from the repository that are older than the time threshold.
        Arguments:
            time_threshold {datetime} -- The time threshold.
        Returns:
            list[dict] -- The list of deleted objects.
        """
        self._check_args(time_threshold=time_threshold)
        if time_threshold is None:
            return list(sorted(self._collection['removed'].values(), key=lambda x: x['time_of_removal']))
        else:
            return list(sorted(filter(lambda x: x['time_of_removal'] <= time_threshold, self._collection['removed'].values()), key=lambda x: x['time_of_removal']))

    def restore(self, tag):
        """Restores an object specified by a tag and either a specific time_of_removal or the nth most recent time_of_removal.
        Arguments:
            tag {str} -- The tag of the object to restore.
            nth_most_recent {int} -- The nth most recent version of the object to restore.
            time_of_removal {datetime} -- The time_of_removal of the object to restore.
        Raises:
            InMemoryObjectDAOObjectNotFoundError -- If the object does not exist.
            InMemoryObjectDAOObjectAlreadyExistsError -- If the object already exists and override_existing_object is False.
            InMemoryObjectDAORangeError -- If the nth_most_recent argument is out of range.
        """
        self._check_args(tag=tag)
        if self.exists(tag):
            raise InMemoryObjectDAOObjectAlreadyExistsError(
                f"Cannot restore object with tag {tag} because it already exists in object repository."
            )
        elif self._collection['removed'].get(tag) is None:
            raise InMemoryObjectDAOObjectNotFoundError(
                f"Cannot restore object with tag {tag} because it is not present in the cue of removed objects."
            )
        # move the object id from the removed dict to the tags dict
        self._collection['tags'][tag] = self._collection['removed'][tag]['id']
        # remove the object id from the removed dict
        del self._collection['removed'][tag]
        return None

    def purge(self, time_threshold: datetime=None) -> int:
        """Purges deleted objects from the repository older than the time threshold.
        Arguments:
            time_threshold {datetime} -- The time threshold.
        Returns:
            int -- The number of deleted objects purged.
        """
        self._check_args(time_threshold=time_threshold)
        to_delete = self.list_marked_for_deletion(time_threshold)
        count = len(to_delete)
        for entry in to_delete:
            del self._collection['objects'][entry['id']]
            del self._collection['removed'][entry['tag']]
        return count


    def _get_schema_name(self, object):
        # extract a human readable typestring without all the <>'s and stuff
        schema_ref = str(type(object))
        schema_ref = schema_ref.split("'")[1].lower()
        if '.' in schema_ref:
            schema_ref = schema_ref.split('.')[-1]
        return schema_ref

    def _check_args(self, **kwargs):
        for key, value in kwargs.items():
            try:
                arg_types = self._arg_types[key]
            except KeyError:
                raise InMemoryObjectDAOArgumentNameError(
                    f'Invalid argument name {key}. The only valid argument names are {self._arg_types.keys()}.'
                )
            # check if value is one of the allowed types
            if not isinstance(value, arg_types):
                raise InMemoryObjectDAOTypeError(
                    f'Invalid type {type(value)} for argument {key}. Must be one of {arg_types}.'
                )

    @property
    def _arg_types(self):
        nonetype = type(None)
        functiontype = type(lambda x: x)
        return {
            'tag': (str),
            'time_of_removal': (datetime),
            'nth_most_recent': (int),
            'time_threshold': (datetime, nonetype),
            'key_filter': (functiontype, nonetype),
            'value_filter': (functiontype, nonetype),
            'filter_relation': (str),
        }



# ===================
# Helper Functions
# ===================

# Microseconds

def datetime_to_microseconds(timestamp: datetime) -> int:
    """Converts a datetime object to microseconds, with microsecond precition.
    Arguments:
        timestamp {datetime} -- The timestamp to convert.
        Returns:
            int -- The timestamp in microseconds (full precision).
    """
    if timestamp is None:
        return None
    return int(timestamp.timestamp() * 1000000)

def microseconds_to_datetime(timestamp: int) -> datetime.utcnow:
    """Converts microseconds to a datetime object.
    Arguments:
        timestamp {int} -- The timestamp in microsecnods (full precision).
        Returns:
            datetime -- The timestamp as a datetime object.
    """
    if timestamp is None:
        return None
    return datetime.utcfromtimestamp(timestamp / 1000000)

# JSON
def dict_to_json_bytes(dictionary: dict) -> str:
    """Converts a dictionary to a JSON string.
    Arguments:
        dictionary {dict} -- The dictionary to convert.
    Returns:
        str -- The JSON string.
    """
    if dictionary is None:
        return None
    if not isinstance(dictionary, dict):
        raise PropertySerializerArgumentTypeError(f'Invalid dict_to_json_bytes property serializer argument; argument type: {type(dictionary)}. Must be dict.')
    return json.dumps(dictionary).encode('utf-8')

class PropertySerializerArgumentTypeError(TypeError):
    pass

def json_bytes_to_dict(json_string: str) -> dict:
    """Converts a JSON string to a dictionary.
    Arguments:
        json_string {str} -- The JSON string to convert.
    Returns:
        dict -- The dictionary.
    """
    if not isinstance(json_string, bytes):
        raise TypeError(f'Invalid type {type(json_string)} for argument json_string. Must be str.')
    if json_string is None:
        return None
    return json.loads(json_string.decode('utf-8'))

# String

def datetime_to_string(timestamp: datetime) -> str:
    """Converts a datetime object to a string with microsecond precision.
    Arguments:
        timestamp {datetime} -- The timestamp to convert.
    Returns:
        str -- The timestamp as a string.
    """
    return timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')

def string_to_datetime(timestamp: str) -> datetime:
    """Converts a string to a datetime object with microsecond precision.
    Arguments:
        timestamp {str} -- The timestamp to convert.
    Returns:
        datetime -- The timestamp as a datetime object.
    """
    return datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')

