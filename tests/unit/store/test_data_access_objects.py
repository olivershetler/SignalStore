import pytest
from datetime import datetime, timezone, timedelta
from signalstore.store.data_access_objects import *

class TestDomainModelDAO:

    # Get tests (test all expected behaviors of get())
    # -----------------------------------------------
    # Category 1: Get a single item that exists; check that it has the right values; check that it has the right keys
    # Test 1.1: Get a single property model that exists; check that it has the right values; check that it has the right keys
    # Test 1.2: Get a single metamodel model that exists; check that it has the right values; check that it has the right keys
    # Test 1.3: Get a single data model that exists; check that it has the right values; check that it has the right keys
    # Category 2: Getting a single item that does not exist; check that it returns None
    # Test 2.1: Getting a single item that does not exist; check that it returns None
    # Category 3: Getting a single item with a bad schema_name argument (error)
    # Test 3.1: Getting a single item with a bad schema_name argument (error)

    @pytest.mark.parametrize('schema_name', ['dimension_of_measure', 'record_metamodel', 'spike_waveforms'])
    def test_get_item_that_exists(self, populated_domain_model_dao, schema_name):
        item = populated_domain_model_dao.get(schema_name=schema_name)
        assert item.get('schema_name') == schema_name, f'Expected schema_name to be "{schema_name}", got "{item.get("schema_name")}"'
        tos = item.get('time_of_save')
        assert isinstance(tos, datetime), f'Expected time_of_save to be a datetime, got {type(tos)}'

    def test_get_item_that_does_not_exist(self, populated_domain_model_dao):
        assert populated_domain_model_dao.get(schema_name='not_a_schema') is None

    def test_get_item_with_bad_schema_name_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.get(schema_name=1)

    # Exists tests (test all expected behaviors of exists())
    # ------------------------------------------------------
    # Category 1: Check if a single item exists that does exist
    # Test 1.1: Check if a single property model exists that does exist
    # Test 1.2: Check if a single metamodel model exists that does exist
    # Test 1.3: Check if a single data model exists that does exist
    # Category 2: Check if a single item exists that does not exist
    # Test 2.1: Check if a single model exists that does not exist

    @pytest.mark.parametrize('schema_name', ['dimension_of_measure', 'record_metamodel', 'spike_waveforms'])
    def test_exists__model_that_exists(self, populated_domain_model_dao, schema_name):
        assert populated_domain_model_dao.exists(schema_name=schema_name)

    def test_exists_model_that_does_not_exist(self, populated_domain_model_dao):
        assert not populated_domain_model_dao.exists(schema_name='not_a_schema')

    # Find (query) tests (test all expected behaviors of find())
    # ----------------------------------------------------
    # Category 1: Find items with no arguments
    # Test 1.1: Find all items with no arguments; check that it returns the right items
    # Category 2: Find items that exist with valid filter argument only
    # Test 2.1: Find all the models of a certain schema_type; check that it returns the right models
    # Test 2.2: Find all models with a certain metamodel_ref value; check that it returns the right models
    # Test 2.3: Find all the models with a property none of them have; check that it returns an empty list
    # Test 2.4: Find all the models with a property they have but a value none of them have; check that it returns an empty list
    # Category 3: Find items with a valid filter argument and a valid projection argument
    # Test 3.1: Find just the schema_name of all the models of a certain schema_type; check that it returns the right models
    # Test 3.2: Find just the schema_name and schema_description of all the models of a certain schema_type; check that it returns the right models
    # Category 4: Find items with bad arguments (error)
    # Test 4.1: Find with a bad filter argument (error)
    # Test 4.2: Find with a bad projection argument (error)
    # Test 4.3: Find with a bad filter and a bad projection argument (error)

    def test_find_all_models(self, populated_domain_model_dao, data_models, metamodels, property_models):
        all_models = data_models + metamodels + property_models
        response = populated_domain_model_dao.find()
        assert len(response) == len(all_models), f'Expected len(response) to be {len(all_models)}, got {len(response)}'
        for record in response:
            tos = record.get('time_of_save')
            assert isinstance(tos, datetime), f'Expected time_of_save to be a datetime, got {type(tos)}'

    @pytest.mark.parametrize('schema_type', ['property_model', 'metamodel', 'data_model'])
    def test_find_models_of_certain_schema_type(self, populated_domain_model_dao, schema_type):
        response = populated_domain_model_dao.find({'schema_type': schema_type})
        for record in response:
            assert record['schema_type'] == schema_type, f'Expected schema_type to be "{schema_type}", got "{record["schema_type"]}"'
            tos = record.get('time_of_save')
            assert isinstance(tos, datetime), f'Expected time_of_save to be a datetime, got {type(tos)}'

    @pytest.mark.parametrize('metamodel_ref', ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_find_models_with_certain_metamodel_ref(self, populated_domain_model_dao, metamodel_ref):
        response = populated_domain_model_dao.find({'metamodel_ref': metamodel_ref})
        for record in response:
            assert record['metamodel_ref'] == metamodel_ref, f'Expected metamodel_ref to be "{metamodel_ref}", got "{record["metamodel_ref"]}"'
            tos = record.get('time_of_save')
            assert isinstance(tos, datetime), f'Expected time_of_save to be a datetime, got {type(tos)}'

    def test_find_models_with_property_none_of_them_have(self, populated_domain_model_dao):
        response = populated_domain_model_dao.find({'not_a_property': 'not_a_value'})
        assert response == []

    def test_find_models_with_property_they_have_but_value_none_of_them_have(self, populated_domain_model_dao):
        response = populated_domain_model_dao.find({'schema_type': 'not_a_value'})
        assert response == []

    @pytest.mark.parametrize('projection', [{'schema_name': 1, 'schema_type': 1}, {'schema_name': 1, 'schema_type': 1, 'schema_title': 1}])
    @pytest.mark.parametrize('schema_type', ['property_model', 'metamodel', 'data_model'])
    def test_find_with_schema_type_and_projection(self, populated_domain_model_dao, schema_type, projection):
        response = populated_domain_model_dao.find(filter={'schema_type': schema_type}, projection=projection)
        for record in response:
            assert record['schema_type'] == schema_type, f'Expected schema_type to be "{schema_type}", got "{record["schema_type"]}"'
            assert set(record.keys()) == set(projection.keys()), f'Expected record to have keys {set(projection.keys())}, got {set(record.keys())}'

    def test_find_with_bad_filter_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.find(filter=1)

    def test_find_with_bad_projection_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.find(projection=1)

    def test_find_with_bad_filter_and_bad_projection_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.find(filter=1, projection=1)

    # Add tests (test all expected behaviors of add())
    # ------------------------------------------------
    # Category 1: Add a single item that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Test 1.1: Add a single model that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Add a single item that does exist (error)
    # Test 2.1: Add a single  model that does exist (error)
    # Category 3: Add a single item with a bad item argument (error)
    # Test 3.1: Add a single item with a bad document argument (error)
    # Test 3.2: Add a single item with a bad timestamp argument (error)

    def test_add_not_existing_model(self, populated_domain_model_dao):
        new_model = {
            'schema_name': 'new_model',
            'schema_type': 'data_model',
            'schema_description': 'New Model',
            'schema_title': 'New Model',
            'json_schema': {}
        }
        populated_domain_model_dao.add(document=new_model, timestamp=datetime.now(timezone.utc))
        assert populated_domain_model_dao.exists(schema_name='new_model')
        # the model should have the same keys, plus a time_of_addition key
        # and a time_of_removal key. The time_of_removal key should be None
        # and the time_of_addition key should be an integer.
        model_with_timestamps = populated_domain_model_dao.get(schema_name='new_model')
        assert model_with_timestamps.keys() == {'schema_name', 'schema_type', 'schema_description', 'schema_title', 'json_schema', 'time_of_save', 'time_of_removal', 'version_timestamp'}, f"Expected model to have keys 'schema_name', 'schema_type', 'schema_description', 'schema_title', 'json_schema', 'time_of_addition', 'time_of_removal', got {model_with_timestamps.keys()}"
        assert isinstance(model_with_timestamps['time_of_save'], datetime)
        assert model_with_timestamps['time_of_removal'] is None
        assert model_with_timestamps['schema_name'] == 'new_model'

    def test_add_existing_model(self, populated_domain_model_dao):
        with pytest.raises(MongoDAODocumentAlreadyExistsError):
            populated_domain_model_dao.add(document={'schema_name': 'dimension_of_measure'}, timestamp=datetime.now(timezone.utc))

    def test_add_model_with_bad_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.add(document=1, timestamp=datetime.now(timezone.utc))

    def test_add_model_with_bad_timestamp_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.add(document={'schema_name': 'new_model'}, timestamp=1)

    # Mark for deletion tests (test all expected behaviors of mark_for_deletion())
    # ----------------------------------------------------------------------------
    # Category 1: Mark a single item for deletion that exists; check that it can nolonger be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Test 1.1: Mark a single model for deletion that exists; check that it can nolonger be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Category 2: Mark a single item for deletion that does not exist (error)
    # Test 2.1: Mark a single model for deletion that does not exist (error)
    # Category 3: Mark a single item for deletion with a bad argument (error)
    # Test 3.1: Mark a single model for deletion with a bad schema_name argument (error)
    # Test 3.2: Mark a single model for deletion with a bad timestamp argument (error)

    def test_mark_for_deletion_model_that_exists(self, populated_domain_model_dao):
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=datetime.now(timezone.utc))
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure'), f"Expected exists to return False, got {populated_domain_model_dao.exists(schema_name='dimension_of_measure')}"
        assert populated_domain_model_dao._collection.find_one({'schema_name': 'dimension_of_measure'}, {'_id':0})['time_of_removal'] is not None

    def test_mark_for_deletion_model_that_does_not_exist(self, populated_domain_model_dao):
        with pytest.raises(MongoDAODocumentNotFoundError):
            populated_domain_model_dao.mark_for_deletion(schema_name='not_a_schema', timestamp=datetime.now(timezone.utc))

    def test_mark_for_deletion_model_with_bad_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.mark_for_deletion(schema_name=1, timestamp=datetime.now(timezone.utc))

    def test_mark_for_deletion_model_with_bad_timestamp(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=1)

    # List marked for deletion tests (test all expected behaviors of list_marked_for_deletion())
    # -----------------------------------------------------------------------------------------
    # Category 1: List items marked for deletion; check that it returns the right items
    # Test 1.1: List models marked for deletion; check that it returns the right models
    # Category 2: List items marked for deletion with a time threshold; check that it returns the right items
    # Test 2.1: List models marked for deletion before a time threshold; check that the items before the time threshold are returned and the items after the time threshold are not; try a few thresholds
    # Category 3: List items marked for deletion with a bad argument (error)
    # Test 3.1: List models marked for deletion with a bad time threshold argument (error)

    @pytest.mark.parametrize('to_delete', [['dimension_of_measure', 'record_metamodel', 'spike_waveforms']])
    def test_list_marked_for_deletion_returns_all_with_no_arg(self, populated_domain_model_dao, to_delete):
        for schema_name in to_delete:
            populated_domain_model_dao.mark_for_deletion(schema_name=schema_name, timestamp=datetime.now(timezone.utc))
        response = populated_domain_model_dao.list_marked_for_deletion()
        assert len(response) == len(to_delete), f'Expected len(response) to be {len(to_delete)}, got {len(response)}'
        for record in response:
            assert record['schema_name'] in to_delete, f'Expected schema_name to be in {to_delete}, got {record["schema_name"]}'

    @pytest.mark.parametrize('to_delete', [['dimension_of_measure', 'record_metamodel', 'spike_waveforms']])
    @pytest.mark.parametrize('threshold_delta', [timedelta(seconds=1.1), timedelta(seconds=2.1), timedelta(seconds=3.1)])
    def test_list_marked_for_deletion_returns_deletions_before_time_threshold(self, populated_domain_model_dao, timestamp, to_delete, threshold_delta):
        for i, schema_name in enumerate(to_delete):
            populated_domain_model_dao.mark_for_deletion(schema_name=schema_name, timestamp=timestamp + timedelta(seconds=i+1))
        response = populated_domain_model_dao.list_marked_for_deletion(time_threshold=timestamp + threshold_delta)
        # round threshold_delta to the nearest second to get the expected number of deletions
        expected_deletions = round(threshold_delta.total_seconds())
        assert len(response) == expected_deletions, f'Expected len(response) to be {expected_deletions}, got {len(response)}'

    def test_list_marked_for_deletion_with_bad_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.list_marked_for_deletion(1)

    # Restore tests (test all expected behaviors of restore())
    # --------------------------------------------------------
    # Category 1: Resotre the nth most recent item that was marked for deletion; check that it can be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Test 1.1: Restore the most recent model that was marked for deletion; check that it can be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Test 1.2: Restore the most recent model is idempotent; check that it can be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Test 1.3: Restore the 2nd most recent model that was marked for deletion; check that it can be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Test 1.4: Restore the nth most recent model that was marked for deletion; check that it can be retreived; check that it is still in the database; check that it has the right values; check that it has the right keys
    # Category 2: Restore a single item that already exists (error)
    # Test 2.1: Restore a single model that already exists (error)
    # Category 3: Restore a single item that was not marked for deletion (error)
    # Test 3.1: Restore a single model that exists but was not marked for deletion (error)
    # Test 3.2: Restore a single model that does not exist and was not marked for deletion (error)
    # Category 4: Restore a single item with a bad schema_name argument (error)
    # Test 4.1: Restore a single model with a bad schema_name argument (error)
    # Test 4.2: Restore a single model with a bad nth_most_recent argument (error)
    # Test 4.3: Restore a single model with a bad timestamp argument (error)
    # Category 5: Restore a single item with an out of range nth_most_recent argument(error)
    # Test 5.1: Restore a single model with an nth_most_recent argument <= 0 (error)
    # Test 5.1: Restore a single model with an nth_most_recent argument > number of versions of the model (error)

    def test_restore_most_recent_model_that_was_marked_for_deletion(self, populated_domain_model_dao, timestamp):
        tod = timestamp + timedelta(seconds=1)
        tor = timestamp + timedelta(seconds=2)
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=tod)
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure'), f'Expected exists to return False, got {populated_domain_model_dao.exists(schema_name="dimension_of_measure")}'
        populated_domain_model_dao.restore(schema_name='dimension_of_measure')
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure') is not None
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure')['schema_title'] == 'Dimension of Measure', f'Expected schema_title to be "Dimension of Measure", got "{populated_domain_model_dao.get(schema_name="dimension_of_measure")["schema_title"]}"'

    def test_restore_most_recent_model_is_idempotent(self, populated_domain_model_dao, timestamp):
        tod = timestamp + timedelta(seconds=1)
        tor = timestamp + timedelta(seconds=2)
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=tod)
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.restore(schema_name='dimension_of_measure')
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure') is not None
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=tod)
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.restore(schema_name='dimension_of_measure')
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure') is not None

    def test_restore_2nd_most_recent_model_marked_for_deletion(self, populated_domain_model_dao, timestamp):
        tod1 = timestamp + timedelta(seconds=1)
        tod2 = timestamp + timedelta(seconds=1.5)
        dom_record = populated_domain_model_dao.get(schema_name='dimension_of_measure')
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=tod1)
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.add(document=dom_record, timestamp=timestamp)
        assert populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=tod2)
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.restore(schema_name='dimension_of_measure', nth_most_recent=2)
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure') is not None
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure')['schema_title'] == 'Dimension of Measure', f'Expected schema_title to be "Dimension of Measure", got "{populated_domain_model_dao.get(schema_name="dimension_of_measure")["schema_title"]}"'

    @pytest.mark.parametrize('nth_most_recent', [2, 3, 4, 5, 6])
    def test_restore_nth_most_recent_model_marked_for_deletion(self, populated_domain_model_dao, timestamp, nth_most_recent):
        for i in range(nth_most_recent-1):
            dom_doc = populated_domain_model_dao.get(schema_name='dimension_of_measure')
            dom_doc['schema_title'] = f'Dimension of Measure V{i+1}'
            populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp + timedelta(seconds=i))
            populated_domain_model_dao.add(document=dom_doc, timestamp=timestamp)
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp + timedelta(seconds=i+1))
        assert not populated_domain_model_dao.exists(schema_name='dimension_of_measure')
        populated_domain_model_dao.restore(schema_name='dimension_of_measure',  nth_most_recent=nth_most_recent)
        assert populated_domain_model_dao.get(schema_name='dimension_of_measure') is not None
        if nth_most_recent > 1:
            assert populated_domain_model_dao.get(schema_name='dimension_of_measure')['schema_title'] == f'Dimension of Measure V{nth_most_recent-1}', f'Expected schema_title to be "Dimension of Measure V{nth_most_recent-1}", got "{populated_domain_model_dao.get(schema_name="dimension_of_measure")["schema_title"]}"'
        else:
            assert populated_domain_model_dao.get(schema_name='dimension_of_measure')['schema_title'] == 'Dimension of Measure', f'Expected schema_title to be "Dimension of Measure", got "{populated_domain_model_dao.get(schema_name="dimension_of_measure")["schema_title"]}"'

    def test_restore_model_that_already_exists(self, populated_domain_model_dao, timestamp):
        dom_doc = populated_domain_model_dao.get(schema_name='dimension_of_measure')
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp)
        populated_domain_model_dao.add(document=dom_doc, timestamp=timestamp)
        with pytest.raises(MongoDAODocumentAlreadyExistsError):
            populated_domain_model_dao.restore(schema_name='dimension_of_measure')

    def test_restore_model_that_was_not_marked_for_deletion(self, populated_domain_model_dao):
        with pytest.raises(MongoDAORangeError):
            populated_domain_model_dao.restore(schema_name='dimension_of_measure')

    def test_restore_model_with_bad_schema_name_argument(self, populated_domain_model_dao, timestamp):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.restore(schema_name=1, timestamp=timestamp)

    def test_restore_model_with_bad_nth_most_recent_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.restore(schema_name='dimension_of_measure', nth_most_recent='1')

    def test_restore_model_with_bad_timestamp_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.restore(schema_name='dimension_of_measure', timestamp=1)

    @pytest.mark.parametrize('n', [-5, 0, 7])
    def test_restore_model_with_out_of_range_nth_most_recent_argument(self, populated_domain_model_dao, timestamp, n):
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp)
        with pytest.raises(MongoDAORangeError):
            populated_domain_model_dao.restore(schema_name='dimension_of_measure', nth_most_recent=n)

    # purge tests (test all expected behaviors of purge())
    # ----------------------------------------------------
    # Category 1: Purge with and without a time threshold; check that it deletes the appropriate items
    # Test 1.1: Purge with no time threshold; check that it deletes all items marked for deletion
    # Test 1.2: Purge with a time threshold; check that it deletes all items marked for deletion before the time threshold
    # Category 2: Purge with a bad argument (error)
    # Test 2.1: Purge with a bad time threshold argument (error)

    def test_purge_with_no_time_threshold(self, populated_domain_model_dao, timestamp):
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp)
        populated_domain_model_dao.mark_for_deletion(schema_name='record_metamodel', timestamp=timestamp)
        populated_domain_model_dao.mark_for_deletion(schema_name='spike_waveforms', timestamp=timestamp)
        assert len(populated_domain_model_dao.list_marked_for_deletion()) == 3
        populated_domain_model_dao.purge()
        assert populated_domain_model_dao.list_marked_for_deletion() == []

    @pytest.mark.parametrize('threshold_delta', [timedelta(seconds=1.1), timedelta(seconds=2.1), timedelta(seconds=3.1)])
    def test_purge_with_time_threshold(self, populated_domain_model_dao, timestamp, threshold_delta):
        populated_domain_model_dao.mark_for_deletion(schema_name='dimension_of_measure', timestamp=timestamp + timedelta(seconds=1))
        populated_domain_model_dao.mark_for_deletion(schema_name='record_metamodel', timestamp=timestamp + timedelta(seconds=2))
        populated_domain_model_dao.mark_for_deletion(schema_name='spike_waveforms', timestamp=timestamp + timedelta(seconds=3))
        populated_domain_model_dao.purge(time_threshold=timestamp + threshold_delta)
        # round threshold_delta to the nearest second to get the expected number of deletions
        expected_deletions = round(threshold_delta.total_seconds())
        assert len(populated_domain_model_dao.list_marked_for_deletion()) == 3 - expected_deletions

    def test_purge_with_bad_argument(self, populated_domain_model_dao):
        with pytest.raises(MongoDAOTypeError):
            populated_domain_model_dao.purge(1)


class TestFileSystemDAO:

    # Get tests (test all expected behaviors of get())
    # -----------------------------------------------
    # Category 1A: Unversioned files that exist with default data adapter set
    # Test 1a.1: Get a single unversioned NetCDF dataarray that exists; check that it has the right values; check that it has the right keys
    # Test 1a.2: Get a single Zarr dataarray that exists; check that it has the right values; check that it has the right keys
    # Test 1a.3: Get a single versioned numpy mock model object that exists; check that it has the right values; check that it has the right keys
    # Category 1B: Versioned files that exist with default data adapter set
    # Test 1b.1: Get a single versioned (numpy model) file that exists with its timestamp.
    # Test 1b.2: Get a single versioned (numpy model) file that exists with nth_most_recent.
    # Test 1b.3: Get all the versions of a (numpy model) file that exist.
    # Category 1C: Unversioned files that exist with non-default data adapter prvided
    # Test 1c.1: Get a single unversioned NetCDF dataarray that exists and non-default data-adapter; check that it has the right values; check that it has the right keys
    # Test 1c.2: Get a single Zarr dataarray that exists and non-default data-adapter; check that it has the right values; check that it has the right keys
    # Test 1c.3: Get a single versioned numpy mock model object that exists and non-default data-adapter; check that it has the right values; check that it has the right keys
    # Category 1D: Versioned files that exist with default data adapter set
    # Test 1d.1: Get a single versioned (numpy model) file that exists and non-default data-adapter with its timestamp.
    # Test 1d.2: Get a single versioned (numpy model) file that exists and non-default data-adapter with nth_most_recent.
    # Test 1d.3: Get all the versions of a (numpy model) file that exist and non-default data-adapter.
    # Category 2: Files that do not exist
    # Test 2.1: Get a single NetCDF dataarray file that does not exist; check that it returns None
    # Test 2.2: Get a single Zarr dataarray file that does not exist; check that it returns None
    # Test 2.3: Get a single versioned numpy mock model object that does not exist; check that it returns None
    # Category 2: Bad arguments
    # Test 2.1: Get a single file with a bad schema_name argument (error)
    # Test 2.2: Get a single file with a bad data_name argument (error)
    # Test 2.3: Get a single file with a bad data_version argument (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('use_data_adapter', [True, False])
    def test_get_file_that_exists(self, file_dao_options, data_adapter_options, use_data_adapter, file_type):
        file_dao = file_dao_options[file_type]
        if use_data_adapter:
            data_adapter = data_adapter_options[file_type]
        else:
            data_adapter = None
        data_object = file_dao.get(schema_ref='test', data_name='test', data_adapter=data_adapter)
        schema_ref = data_object.attrs.get('schema_ref')
        data_name = data_object.attrs.get('data_name')
        data_version = data_object.attrs.get('data_version')
        assert schema_ref == 'test', f'Expected schema_ref to be "spike_waveforms", got "{schema_ref}"'
        assert data_name == 'test', f'Expected data_name to be "spike_waveforms", got "{data_name}"'
        assert data_version == None, f'Expected data_version to be None, got "{data_version}"'

    @pytest.mark.parametrize('use_data_adapter', [True, False])
    def test_get_versioned_file_with_timestamp(self, populated_numpy_file_dao, model_numpy_adapter, use_data_adapter, timestamp):
        if use_data_adapter:
            data_adapter = model_numpy_adapter
        else:
            data_adapter = None
        ts = timestamp + timedelta(seconds=10)
        model = populated_numpy_file_dao.get(schema_ref='test', data_name='test', data_adapter=data_adapter)
        result = model.attrs["version_timestamp"]
        assert result == ts, f"Expected {ts} but got {result}."

    @pytest.mark.parametrize('n', range(11))
    def test_get_versioned_file_with_nth_most_recent(self, populated_numpy_file_dao, n):
        checkpoint = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=n)
        assert checkpoint is not None

    def test_get_all_versions_of_file_that_exists(self, populated_numpy_file_dao):
        checkpoint = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        checkpoints = []
        n = 2
        while checkpoint is not None:
            checkpoints.append(checkpoint)
            checkpoint = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=n)
            n += 1
        assert len(checkpoints) == 10
        assert len(checkpoints) == populated_numpy_file_dao.n_versions(schema_ref='test', data_name='test')

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_get_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        assert file_dao.get(schema_ref='not_a_schema', data_name='not_a_data_name', data_adapter=None) is None
        assert file_dao.get(schema_ref='test', data_name='not_a_data_name', data_adapter=None) is None
        assert file_dao.get(schema_ref='test', data_name='test', version_timestamp=datetime.now(timezone.utc)+timedelta(seconds=5)) is None

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_get_file_with_bad_schema_ref_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            data_object = file_dao.get(schema_ref=bad_arg, data_name='test', data_adapter=None)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_get_file_with_bad_data_name_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            data_object = file_dao.get(schema_ref='test', data_name=bad_arg, data_adapter=None)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, None, "string", {"set"}, {"hash": "map"}])
    def test_get_file_with_bad_nth_most_recent_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            data_object = file_dao.get(schema_ref='test', data_name='test', nth_most_recent=bad_arg, data_adapter=None)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, 1, "string", {"set"}, {"hash": "map"}])
    def test_get_file_with_bad_version_timestamp_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            data_object = file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1, version_timestamp=bad_arg, data_adapter=bad_arg)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, 1, datetime.now(timezone.utc), "string", {"set"}, {"hash": "map"}])
    def test_get_file_with_bad_data_adapter_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            data_object = file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1, data_adapter=bad_arg)

    # Exists tests (test all expected behaviors of exists())
    # ------------------------------------------------------
    # Category 1: Unversioned files that exist
    # Test 1.1: Check if a single unversioned NetCDF dataarray exists that exists
    # Test 1.2: Check if a single unversioned Zarr dataarray exists that exists
    # Test 1.3: Check if a single unversioned numpy mock model object exists that exists
    # Category 2: Versioned files that exist
    # Test 2.1: Check if a single versioned (numpy model) file that exists (with its timestamp).
    # Category 3: Files that do not exist
    # Test 3.1: Check if a single NetCDF dataarray file that does not exist
    # Test 3.2: Check if a single Zarr dataarray file that does not exist
    # Test 3.3: Check if a single versioned numpy mock model object that does not exist
    # Category 4: Bad arguments
    # Test 4.1: Check if a single file with a bad schema_name argument (error)
    # Test 4.2: Check if a single file with a bad data_name argument (error)
    # Test 4.3: Check if a single file with a bad data_version argument (error)
    # Test 4.4: Check if a single file with a bad data_adapter argument (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_exists_unversioned_file_that_exists(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        result = file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        assert result, f'Expected exists to return True, got {result}'

    def test_exists_versioned_file_with_timestamp(self, populated_numpy_file_dao):
        x = populated_numpy_file_dao.get(schema_ref='test', data_name='test')
        ts = x.attrs["version_timestamp"]
        result = populated_numpy_file_dao.exists(schema_ref='test', data_name='test', version_timestamp=ts)
        assert result, f'Expected exists to return True, got {result}'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_exists_with_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        result = file_dao.exists(schema_ref='not_a_schema', data_name='not_a_data_name', version_timestamp=0)
        assert not result, f'Expected exists to return False, got {result}'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_exists_with_bad_schema_ref_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.exists(schema_ref=bad_arg, data_name='test', version_timestamp=0)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_exists_with_bad_data_name_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.exists(schema_ref='test', data_name=bad_arg, version_timestamp=0)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, "string", {"set"}, {"hash": "map"}])
    def test_exists_with_bad_version_timestamp_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.exists(schema_ref='test', data_name='test', version_timestamp=bad_arg)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, "string", {"set"}, {"hash": "map"}])
    def test_exists_with_bad_data_adapter_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.exists(schema_ref='test', data_name='test', data_adapter=bad_arg)

    # N versions tests (test all expected behaviors of n_versions())
    # --------------------------------------------------------------
    # Category 1: Versioned files that exist
    # Test 1.1: Get version count and assert that it is correct
    # Category 2: Files that do not exist
    # Test 2.1: Get version count and assert that it is 0
    # Category 3: Bad arguments
    # Test 3.1: Get version count with a bad schema_name argument (error)
    # Test 3.2: Get version count with a bad data_name argument (error)

    def test_n_versions_versioned_file_that_exists(self, populated_numpy_file_dao):
        result = populated_numpy_file_dao.n_versions(schema_ref='test', data_name='test')
        assert result == 10, f'Expected n_versions to return 10, got {result}'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_n_versions_with_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        result = file_dao.n_versions(schema_ref='not_a_schema', data_name='not_a_data_name')
        assert result == 0, f'Expected n_versions to return 0, got {result}'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_n_versions_with_bad_schema_ref_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.n_versions(schema_ref=bad_arg, data_name='test')

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_n_versions_with_bad_data_name_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.n_versions(schema_ref='test', data_name=bad_arg)

    # Add tests (test all expected behaviors of add())
    # ------------------------------------------------
    # Category 1: Unversioned files that do not exist
    # Test 1.1: Add a single unversioned NetCDF dataarray that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Test 1.2: Add a single unversioned Zarr dataarray that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Versioned files that do not exist
    # Test 2.1: Add a single versioned numpy mock model object that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Category 3: Unversioned that already exist
    # Test 3.1: Add a single unversioned NetCDF dataarray that already exists (error)
    # Test 3.2: Add a single unversioned Zarr dataarray that already exists (error)
    # Category 4: Versioned files that already exist
    # Test 4.1: Add a single versioned numpy mock model object that already exists and has the same timestamp (error)
    # Test 4.2: Add a single versioned numpy mock model object that already exists and has a different timestamp; check that it add works; check that it has the right values; check that it has the right keys
    # Category 5: Bad arguments
    # Test 5.1: Add a single file with a bad data_adapter argument (error)
    # Category 6: Mismatched data_object, data_adapter pairs
    # Test 6.1: Add a single file with a data_object and data_adapter that do not match (several combos) (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_add_unversioned_file_that_does_not_exist(self, file_dao_options, new_object_options, file_type):
        file_dao = file_dao_options[file_type]
        data_object = new_object_options[file_type]
        file_dao.add(data_object=data_object)
        result = file_dao.get(schema_ref='new', data_name='new')
        assert result is not None
        schema_ref = result.attrs.get('schema_ref')
        data_name = result.attrs.get('data_name')
        assert schema_ref == 'new', f'Expected schema_ref to be "new", got "{schema_ref}"'
        assert data_name == 'new', f'Expected data_name to be "new", got "{data_name}"'

    def test_add_versioned_file_that_does_not_exist(self, populated_numpy_file_dao, new_object_options):
        data_object = new_object_options['numpy']
        populated_numpy_file_dao.add(data_object=data_object)
        result = populated_numpy_file_dao.get(schema_ref='new', data_name='new', nth_most_recent=1)
        assert result is not None
        schema_ref = result.attrs.get('schema_ref')
        data_name = result.attrs.get('data_name')
        assert schema_ref == 'new', f'Expected schema_ref to be "new", got "{schema_ref}"'
        assert data_name == 'new', f'Expected data_name to be "new", got "{data_name}"'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_add_unversioned_file_that_already_exists(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        data_object = file_dao.get(schema_ref='test', data_name='test')
        with pytest.raises(FileSystemDAOFileAlreadyExistsError):
            file_dao.add(data_object=data_object)

    def test_add_versioned_file_that_already_exists_with_same_timestamp(self, populated_numpy_file_dao):
        data_object = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent = 1)
        with pytest.raises(FileSystemDAOFileAlreadyExistsError):
            populated_numpy_file_dao.add(data_object=data_object)

    def test_add_versioned_file_that_already_exists_with_different_timestamp(self, populated_numpy_file_dao):
        data_object = populated_numpy_file_dao.get(schema_ref='test', data_name='test')
        new_ts = datetime.now(timezone.utc) + timedelta(seconds=11)
        data_object.attrs['version_timestamp'] = new_ts
        populated_numpy_file_dao.add(data_object=data_object)
        result = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        assert result is not None
        schema_ref = result.attrs.get('schema_ref')
        data_name = result.attrs.get('data_name')
        version_timestamp = result.attrs.get('version_timestamp')
        assert schema_ref == 'test', f'Expected schema_ref to be "test", got "{schema_ref}"'
        assert data_name == 'test', f'Expected data_name to be "test", got "{data_name}"'
        assert version_timestamp == new_ts, f'Expected version_timestamp to be "{new_ts}", got "{version_timestamp}"'

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, "string", {"set"}, {"hash": "map"}])
    def test_add_with_bad_data_adapter_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.add(data_object=bad_arg)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_add_with_mismatched_data_object_and_data_adapter(self, file_dao_options, file_type):
        if file_type in ['netcdf', 'zarr']:
            bad_type = 'numpy'
        else:
            bad_type = 'netcdf'
        file_dao = file_dao_options[file_type]
        other_file_dao = file_dao_options[bad_type]
        incompatable_data_object = other_file_dao.get(schema_ref='test', data_name='test')
        # set attrs to new
        incompatable_data_object.attrs['schema_ref'] = 'new'
        incompatable_data_object.attrs['data_name'] = 'new'
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.add(data_object=incompatable_data_object)

    # Mark for deletion tests (test all expected behaviors of mark_for_deletion())
    # -----------------------------------------------------------------------------
    # Category 1: Unversioned files that exist
    # Test 1.1: Mark a single unversioned NetCDF dataarray that exists for deletion; check that it can nolonger be accessed through get or the exists method
    # Test 1.2: Mark a single unversioned Zarr dataarray that exists for deletion; check that it can nolonger be accessed through get or the exists method
    # Category 2: Versioned files that exist
    # Test 2.1: Mark a single versioned numpy mock model object that exists for deletion; check that it can nolonger be accessed through get or the exists method; check that the other versions can still be accessed
    # Category 3: Files that do not exist
    # Test 3.1: Mark a single NetCDF dataarray file that does not exist for deletion (error)
    # Test 3.2: Mark a single Zarr dataarray file that does not exist for deletion (error)
    # Test 3.3: Mark a single versioned numpy mock model object that does not exist for deletion (error)
    # Category 4: Bad arguments
    # Test 4.1: Mark a single file with a bad schema_name argument (error)
    # Test 4.2: Mark a single file with a bad data_name argument (error)
    # Test 4.3: Mark a single file with a bad data_version argument (error)
    # Test 4.4: Mark a single file with a bad time_of_removal argument (error)
    # Test 4.5: Mark a single file with a bad data_adapter argument (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_mark_for_deletion_unversioned_file_that_exists(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc))
        assert not file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        assert file_dao.get(schema_ref='test', data_name='test', version_timestamp=0) is None

    def test_mark_for_deletion_versioned_file_that_exists(self, populated_numpy_file_dao):
        to_remove = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_remove.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        assert not populated_numpy_file_dao.exists(schema_ref='test', data_name='test', version_timestamp=ts)
        assert populated_numpy_file_dao.get(schema_ref='test', data_name='test', version_timestamp=ts) is None
        assert populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1) is not None
        assert populated_numpy_file_dao.n_versions(schema_ref='test', data_name='test') == 9

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_mark_for_deletion_with_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOFileNotFoundError):
            file_dao.mark_for_deletion(schema_ref='not_a_schema', data_name='not_a_data_name', time_of_removal=datetime.now(timezone.utc))

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_schema_ref_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.mark_for_deletion(schema_ref=bad_arg, data_name='test', time_of_removal=datetime.now(timezone.utc))

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_data_name_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.mark_for_deletion(schema_ref='test', data_name=bad_arg, time_of_removal=datetime.now(timezone.utc))

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, "string", {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_version_timestamp_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=bad_arg, time_of_removal=datetime.now(timezone.utc))

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, "string", {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_time_of_removal_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=bad_arg)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, 1, datetime.now(timezone.utc), "string", {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_data_adapter_arg_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc), data_adapter=bad_arg)

    # List marked for deletion tests (test all expected behaviors of list_marked_for_deletion())
    # -----------------------------------------------------------------------------------------
    # Category 1: Unversioned files that exist
    # Test 1.1: List all unversioned NetCDF dataarrays that exist; check that it returns the right files
    # Test 1.2: List all unversioned Zarr dataarrays that exist; check that it returns the right files
    # Category 2: Versioned files that exist
    # Test 2.1: List all versioned numpy mock model objects that exist; check that it returns the right files
    # Category 3: Files that do not exist
    # Test 3.1: List all NetCDF dataarray files that do not exist; check that it returns an empty list
    # Test 3.2: List all Zarr dataarray files that do not exist; check that it returns an empty list
    # Test 3.3: List all versioned numpy mock model objects that do not exist; check that it returns an empty list
    # Category 4: Bad arguments
    # Test 4.1: List all files with a bad time_threshold argument (error)
    # Test 4.2: List all files with a bad data_adapter argument (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_list_marked_for_deletion_unversioned_file_that_exists(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc))
        result = file_dao.list_marked_for_deletion()
        assert len(result) == 1

    def test_list_marked_for_deletion_versioned_file_that_exists(self, populated_numpy_file_dao):
        to_remove = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_remove.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        result = populated_numpy_file_dao.list_marked_for_deletion()
        assert len(result) == 1

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    def test_list_marked_for_deletion_with_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        result = file_dao.list_marked_for_deletion()
        assert len(result) == 0

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, 1, "string", {"set"}, {"hash": "map"}])
    def test_list_marked_for_deletion_with_bad_time_threshold_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.list_marked_for_deletion(time_threshold=bad_arg)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr', 'numpy'])
    @pytest.mark.parametrize('bad_arg', [1.5, 1, datetime.now(timezone.utc), "string", {"set"}, {"hash": "map"}])
    def test_list_marked_for_deletion_with_bad_data_adapter_arg(self, file_dao_options, file_type, bad_arg):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAOTypeError):
            file_dao.list_marked_for_deletion(data_adapter=bad_arg)

    # restore tests (test all expected behaviors of restore())
    # ---------------------------------------------------------
    # Category 1: Unversioned files that exist and have been removed
    # Test 1.1: Restore a single unversioned NetCDF dataarray that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Test 1.2: Restore nth_most_recent unversioned NetCDF dataarray that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Test 1.3: Restore a single unversioned Zarr dataarray that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Test 1.4: Restore nth_most_recent unversioned Zarr dataarray that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Versioned files that exist and have been removed
    # Test 2.1: Restore a single versioned numpy mock model object that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Test 2.2: Restore nth_most_recent versioned numpy mock model object that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Category 3: Unversioned files that exist and have not been removed
    # Test 3.1: Restore a single unversioned NetCDF dataarray that exists and has not been removed (error)
    # Test 3.2: Restore a single unversioned Zarr dataarray that exists and has not been removed (error)
    # Category 4: Versioned files that exist and have not been removed
    # Test 4.1: Restore a single versioned numpy mock model object that exists and has not been removed (error)
    # Category 5: Files that do not exist
    # Test 5.1: Restore a single NetCDF dataarray file that does not exist (error)
    # Test 5.2: Restore a single Zarr dataarray file that does not exist (error)
    # Test 5.3: Restore a single versioned numpy mock model object that does not exist (error)
    # Category 6: Bad arguments
    # Test 6.1: Restore a single file with a bad schema_name argument (error)
    # Test 6.2: Restore a single file with a bad data_name argument (error)
    # Test 6.3: Restore a single file with a bad data_version argument (error)
    # Test 6.5: Restore a single file with a bad nth_most_recent argument (error)
    # Test 6.4: Restore a single file with a bad time_of_removal argument (error)
    # Test 6.6: Restore a single file with a bad data_adapter argument (error)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_restore_unversioned_file_that_exists_and_has_been_removed(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc))
        file_dao.restore(schema_ref='test', data_name='test')
        assert file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        assert file_dao.get(schema_ref='test', data_name='test', version_timestamp=0) is not None

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    @pytest.mark.parametrize('n', [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_restore_nth_most_recent_unversioned_file_that_exists_and_has_been_removed(self, file_dao_options, file_type, n):
        file_dao = file_dao_options[file_type]
        first_tod = datetime.now(timezone.utc)
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=first_tod)
        # restore and mark for deletion n times
        for i in range(n):
            file_dao.restore(schema_ref='test', data_name='test')
            file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=first_tod+timedelta(seconds=i+1))
        assert not file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        # restore one more time
        file_dao.restore(schema_ref='test', data_name='test')
        assert file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        nth_most_recent = file_dao.get(schema_ref='test', data_name='test', nth_most_recent=n)
        assert nth_most_recent is not None

    def test_restore_versioned_file_that_exists_and_has_been_removed(self, populated_numpy_file_dao):
        to_remove = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_remove.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        populated_numpy_file_dao.restore(schema_ref='test', data_name='test', version_timestamp=ts)
        assert populated_numpy_file_dao.exists(schema_ref='test', data_name='test', version_timestamp=ts)
        assert populated_numpy_file_dao.get(schema_ref='test', data_name='test', version_timestamp=ts) is not None

    @pytest.mark.parametrize('n', [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_restore_nth_most_recent_versioned_file_that_exists_and_has_been_removed(self, populated_numpy_file_dao, n):
        first_tod = datetime.now(timezone.utc)
        to_remove = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_remove.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=first_tod)
        # restore and mark for deletion n times
        for i in range(n):
            to_remove.attrs['data_name'] = f'test'
            populated_numpy_file_dao.add(data_object=to_remove)
            populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name=f'test', version_timestamp=ts, time_of_removal=first_tod+timedelta(seconds=i+1))
        assert not populated_numpy_file_dao.exists(schema_ref='test', data_name='test', version_timestamp=ts)
        # restore one more time
        populated_numpy_file_dao.restore(schema_ref='test', data_name='test', version_timestamp=ts, nth_most_recent=n)
        assert populated_numpy_file_dao.exists(schema_ref='test', data_name=f'test', version_timestamp=ts)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_restore_unversioned_file_that_exists_and_has_not_been_removed(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAORangeError):
            file_dao.restore(schema_ref='test', data_name='test')

    def test_restore_versioned_file_that_exists_and_has_not_been_removed(self, populated_numpy_file_dao):
        to_remove = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_remove.attrs['version_timestamp']
        with pytest.raises(FileSystemDAORangeError):
            populated_numpy_file_dao.restore(schema_ref='test', data_name='test', version_timestamp=ts)

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_restore_unversioned_file_that_does_not_exist(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        with pytest.raises(FileSystemDAORangeError):
            file_dao.restore(schema_ref='not_a_schema', data_name='not_a_data_name')

    def test_restore_versioned_file_that_does_not_exist(self, populated_numpy_file_dao):
        with pytest.raises(FileSystemDAORangeError):
            populated_numpy_file_dao.restore(schema_ref='test', data_name='test', version_timestamp=datetime.now(timezone.utc))

    # purge tests (test all expected behaviors of purge())
    # ---------------------------------------------------------
    # Category 1: Purge all unversioned files that exist and have been removed
    # Test 1.1: Purge all unversioned NetCDF dataarrays that exist and have been removed using time_threshold=None; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 1.2: Purge all unversioned Zarr dataarrays that exist and have been removed using time_threshold=None; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 1.3: Purge all unversioned NetCDF dataarrays that exist and have been removed using time_threshold=first_tod; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 1.4: Purge all unversioned Zarr dataarrays that exist and have been removed using time_threshold=first_tod; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Category 2: Purge all versioned files that exist and have been removed
    # Test 2.1: Purge all versioned numpy mock model objects that exist and have been removed using time_threshold=None; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 2.2: Purge all versioned numpy mock model objects that exist and have been removed using time_threshold=first_tod; check that they can nolonger be accessed through get or the exists method
    # Category 3: Purge only some unversioned files that exist and have been removed; check that the resulting purge count is correct
    # Test 3.1: Purge all unversioned NetCDF dataarrays that exist and have been removed using time_threshold=first_tod+timedelta(seconds=1.2); check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 3.2: Purge all unversioned Zarr dataarrays that exist and have been removed using time_threshold=first_tod+timedelta(seconds=1.2); check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Category 4: Purge only some versioned files that exist and have been removed
    # Test 4.1: Purge all versioned numpy mock model objects that exist and have been removed using time_threshold=first_tod+timedelta(seconds=1.2); check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Category 5: Unversioned files when none have been removed
    # Test 5.1: Purge all unversioned NetCDF dataarrays that exist and have not been removed; check that the purge count is 0
    # Test 5.2: Purge all unversioned Zarr dataarrays that exist and have not been removed; check that the purge count is 0
    # Category 6: Versioned files when none have been removed
    # Test 6.1: Purge all versioned numpy mock model objects that exist and have not been removed; check that the purge count is 0

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_purge_all_unversioned_files_that_exist_and_have_been_removed(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        to_delete = file_dao.get(schema_ref='test', data_name='test')
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc))
        for i in range(9):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            file_dao.add(data_object=to_delete)
            assert file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=0)
            file_dao.mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', time_of_removal=datetime.now(timezone.utc))
        count = file_dao.purge(time_threshold=None)
        assert count == 10
        assert not file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        for i in range(9):
            assert not file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=0)
        assert len(file_dao.list_marked_for_deletion()) == 0

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    def test_purge_all_unversioned_files_that_exist_and_have_been_removed_with_time_threshold(self, file_dao_options, file_type):
        file_dao = file_dao_options[file_type]
        to_delete = file_dao.get(schema_ref='test', data_name='test')
        file_dao.mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=datetime.now(timezone.utc))
        for i in range(9):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            file_dao.add(data_object=to_delete)
            assert file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=0)
            file_dao.mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', time_of_removal=datetime.now(timezone.utc))
        count = file_dao.purge(time_threshold=datetime.now(timezone.utc))
        assert count == 10
        assert not file_dao.exists(schema_ref='test', data_name='test', version_timestamp=0)
        for i in range(9):
            assert not file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=0)
        assert len(file_dao.list_marked_for_deletion()) == 0

    def test_purge_all_versioned_files_that_exist_and_have_been_removed(self, populated_numpy_file_dao):
        to_delete = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_delete.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        for i in range(9):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            populated_numpy_file_dao.add(data_object=to_delete)
            assert populated_numpy_file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts)
            populated_numpy_file_dao.mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        count = populated_numpy_file_dao.purge(time_threshold=None)
        assert count == 10
        assert not populated_numpy_file_dao.exists(schema_ref='test', data_name='test', version_timestamp=ts)
        for i in range(9):
            assert not populated_numpy_file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts)
        assert len(populated_numpy_file_dao.list_marked_for_deletion()) == 0

    def test_purge_all_versioned_files_that_exist_and_have_been_removed_with_time_threshold(self, populated_numpy_file_dao):
        to_delete = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_delete.attrs['version_timestamp']
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        for i in range(9):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            populated_numpy_file_dao.add(data_object=to_delete)
            assert populated_numpy_file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts)
            populated_numpy_file_dao.mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts, time_of_removal=datetime.now(timezone.utc))
        count = populated_numpy_file_dao.purge(time_threshold=datetime.now(timezone.utc))
        assert count == 10

    @pytest.mark.parametrize('file_type', ['netcdf', 'zarr'])
    @pytest.mark.parametrize('n', [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_purge_only_n_unversioned_files_that_exist_and_have_been_removed(self, file_dao_options, file_type, n):
        to_delete = file_dao_options[file_type].get(schema_ref='test', data_name='test')
        first_tod = datetime.now(timezone.utc)
        file_dao_options[file_type].mark_for_deletion(schema_ref='test', data_name='test', time_of_removal=first_tod)
        for i in range(n):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            file_dao_options[file_type].add(data_object=to_delete)
            assert file_dao_options[file_type].exists(schema_ref=f'test{i}', data_name=f'test{i}')
            file_dao_options[file_type].mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', time_of_removal=first_tod + timedelta(seconds=i + 1))
        count = file_dao_options[file_type].purge(time_threshold=first_tod+timedelta(seconds=n - 1))
        assert count == n

    @pytest.mark.parametrize('n', [1, 2, 3, 4, 5, 6, 7, 8, 9])
    def test_purge_only_n_versioned_files_that_exist_and_have_been_removed(self, populated_numpy_file_dao, n):
        to_delete = populated_numpy_file_dao.get(schema_ref='test', data_name='test', nth_most_recent=1)
        ts = to_delete.attrs['version_timestamp']
        first_tod = datetime.now(timezone.utc)
        populated_numpy_file_dao.mark_for_deletion(schema_ref='test', data_name='test', version_timestamp=ts, time_of_removal=first_tod)
        for i in range(n):
            to_delete.attrs['data_name'] = f'test{i}'
            to_delete.attrs['schema_ref'] = f'test{i}'
            to_delete.attrs['version_timestamp'] = ts
            populated_numpy_file_dao.add(data_object=to_delete)
            assert populated_numpy_file_dao.exists(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts), f"test{i} does not exist"
            populated_numpy_file_dao.mark_for_deletion(schema_ref=f'test{i}', data_name=f'test{i}', version_timestamp=ts, time_of_removal=first_tod + timedelta(seconds=i + 1))
        count = populated_numpy_file_dao.purge(time_threshold=first_tod+timedelta(seconds=n - 1))
        assert count == n, f"count is {count}"


class TestInMemoryObjectDAO:

    # Get tests (test all expected behaviors of get())
    # ------------------------------------------------
    # Category 1: Objects that exist
    # Test 1.1: Get a single object that exists; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Objects that do not exist
    # Test 2.1: Get a single object that does not exist; check that it returns None
    # Category 3: Bad arguments
    # Test 3.1: Get a single object with a bad tag argument (error)

    @pytest.mark.parametrize('object_type', [list, str, bool, int, float, type(np.array([])), type(xr.DataArray())])
    def test_get_object_that_exists(self, populated_memory_dao, object_type):
        result = populated_memory_dao.get(tag=f"test_{object_type.__name__.lower()}")
        assert type(result) == object_type

    def test_get_object_that_does_not_exist(self, populated_memory_dao):
        result = populated_memory_dao.get(tag=f"not_a_tag")
        assert result is None

    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_get_with_bad_tag_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.get(tag=bad_arg)

    # Exists tests (test all expected behaviors of exists())
    # ------------------------------------------------------
    # Category 1: Objects that exist
    # Test 1.1: Check that a single object that exists returns True
    # Category 2: Objects that do not exist
    # Test 2.1: Check that a single object that does not exist returns False
    # Category 3: Bad arguments
    # Test 3.1: Check that a single object with a bad tag argument (error)

    @pytest.mark.parametrize('object_type', [list, str, bool, int, float, type(np.array([])), type(xr.DataArray())])
    def test_exists_object_that_exists(self, populated_memory_dao, object_type):
        result = populated_memory_dao.exists(tag=f"test_{object_type.__name__.lower()}")
        assert result

    def test_exists_object_that_does_not_exist(self, populated_memory_dao):
        result = populated_memory_dao.exists(tag=f"not_a_tag")
        assert not result

    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_exists_with_bad_tag_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.exists(tag=bad_arg)

    # Add tests (test all expected behaviors of add())
    # ------------------------------------------------
    # Category 1: Objects that do not exist
    # Test 1.1: Add a single object that does not exist; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Objects that exist
    # Test 2.1: Add a single object with a tag that exists (error)
    # Test 2.2: Add a single object with an id(object) that exists (error)
    # Category 3: Bad arguments
    # Test 3.1: Add a single object with a bad tag argument (error)

    @pytest.mark.parametrize('obj', [[1,2,3], 2, False, 1.5, np.array([1,2,3]), xr.DataArray([1,2,3])])
    def test_add_object_that_does_not_exist(self, populated_memory_dao, obj):
        populated_memory_dao.add(tag=f"new_{type(obj).__name__.lower()}", object=obj)
        result = populated_memory_dao.get(tag=f"new_{type(obj).__name__.lower()}")
        # if object is iterable, use all(==) if not use ==
        try:
            assert result == obj
        except Exception:
            assert all(result == obj)

    @pytest.mark.parametrize('obj', [[1,2,3], 2, False, 1.5, np.array([1,2,3]), xr.DataArray([1,2,3])])
    def test_add_object_with_tag_that_exists(self, populated_memory_dao, obj):
        with pytest.raises(InMemoryObjectDAOObjectAlreadyExistsError):
            populated_memory_dao.add(tag=f"test_{type(obj).__name__.lower()}", object=obj)

    def test_add_object_with_id_that_exists(self, populated_memory_dao, objects):
        for obj in objects:
            with pytest.raises(InMemoryObjectDAOObjectAlreadyExistsError):
                populated_memory_dao.add(tag=f"new_{type(obj).__name__.lower()}", object=obj)

    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_add_with_bad_tag_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.add(tag=bad_arg, object=100)

    # mark_for_deletion tests (test all expected behaviors of mark_for_deletion())
    # -----------------------------------------------------------------------------
    # Category 1: Objects that exist
    # Test 1.1: Mark a single object that exists for deletion; check that it can nolonger be accessed through get or the exists method; check that it is inside the deleted objects subcollection
    # Category 2: Objects that do not exist
    # Test 2.1: Mark a single object that does not exist for deletion (error)
    # Category 3: Bad arguments
    # Test 3.1: Mark a single object with a bad tag argument (error)

    @pytest.mark.parametrize('tags', ['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray'])
    def test_mark_for_deletion_object_that_exists(self, populated_memory_dao, tags):
        obj = populated_memory_dao.get(tag=tags)
        obj_id = id(obj)
        populated_memory_dao.mark_for_deletion(tag=tags, time_of_removal = datetime.now(timezone.utc))
        assert not populated_memory_dao.exists(tag=tags)
        assert populated_memory_dao.get(tag=tags) is None
        del_obj = populated_memory_dao._collection['objects'][obj_id]
        try:
            assert all(del_obj == obj)
        except Exception:
            try:
                assert del_obj == obj
            except Exception:
                assert del_obj.equals(obj)

    def test_mark_for_deletion_object_that_does_not_exist(self, populated_memory_dao):
        with pytest.raises(InMemoryObjectDAOObjectNotFoundError):
            populated_memory_dao.mark_for_deletion(tag='not_a_tag', time_of_removal = datetime.now(timezone.utc))

    @pytest.mark.parametrize('bad_arg', [1, None, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_mark_for_deletion_with_bad_tag_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.mark_for_deletion(tag=bad_arg, time_of_removal = datetime.now(timezone.utc))

    # List marked for deletion tests (test all expected behaviors of list_marked_for_deletion())
    # -----------------------------------------------------------------------------------------
    # Category 1: When there are several objects that have been marked for deletion
    # Test 1.1: List all objects that have been marked for deletion; check that it returns all removed objects; check that the objects are in the right order (by time_of_removal)
    # Test 1.2: List only objects from before the provided time_threshold that have been marked for deletion; check that it returns the right objects; check that the objects are in the right order (by time_of_removal)
    # Category 2: When no objects have been marked for deletion
    # Test 2.1: List all objects that have been marked for deletion; check that it returns an empty list
    # Category 3: Bad arguments
    # Test 3.1: List all objects with a bad time_threshold argument (error)

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    def test_list_all_objects_marked_for_deltion_with_none_time_threshold(self, populated_memory_dao, tags):
        del_ids = []
        for obj in tags:
            del_ids.append(id(populated_memory_dao.get(tag=obj)))
            populated_memory_dao.mark_for_deletion(tag=obj, time_of_removal=datetime.now(timezone.utc))
        result = populated_memory_dao.list_marked_for_deletion()
        assert len(result) == len(tags)
        for i in range(len(result)):
            # check the order of the objects
            if i < len(result) - 1:
                assert result[i]["time_of_removal"] < result[i+1]["time_of_removal"], f"The objects were not in the right order"
            assert result[i]["tag"] == tags[i], f"The tag {tags[i]} was not in the right order"
            # check that the objects are the same
            assert result[i]["id"] == del_ids[i]

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    @pytest.mark.parametrize('time_threshold_shift', list(map(lambda x: timedelta(seconds=x), range(1, 8))))
    def test_list_all_objects_marked_for_deltion_with_time_threshold(self, populated_memory_dao, tags, time_threshold_shift):
        del_ids = []
        removal_start_time = datetime.now(timezone.utc)
        for i, tag in enumerate(tags):
            del_ids.append(id(populated_memory_dao.get(tag=tag)))
            shift = timedelta(seconds=i + 0.5)
            populated_memory_dao.mark_for_deletion(tag=tag, time_of_removal=removal_start_time + shift)
        result = populated_memory_dao.list_marked_for_deletion(time_threshold=removal_start_time + time_threshold_shift)
        assert len(result) == time_threshold_shift.seconds
        for i in range(len(result)):
            # check the order of the objects
            if i < len(result) - 1:
                assert result[i]["time_of_removal"] < result[i+1]["time_of_removal"], f"The objects were not in the right order"
            # check that the objects are the same
            assert result[i]["id"] == del_ids[i]

    def test_list_marked_for_deltion_when_non_have_been_marked(self, populated_memory_dao):
        result = populated_memory_dao.list_marked_for_deletion()
        assert len(result) == 0

    @pytest.mark.parametrize('bad_arg', [1, 12.5, {"set"}, {"hash": "map"}])
    def test_list_marked_for_deletion_with_bad_time_threshold_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.list_marked_for_deletion(time_threshold=bad_arg)

    # restore tests (test all expected behaviors of restore())
    # ---------------------------------------------------------
    # Category 1: Objects that exist and have been removed
    # Test 1.1: Restore a single object that exists and has been removed; check that it exists; check that it has the right values; check that it has the right keys
    # Category 2: Objects tha have not been removed
    # Test 2.1: Restore a single object that exists and has not been removed (error)
    # Test 2.2: Restore a single object that does not exist (error)
    # Category 3: Restore an object that has already been permanently deleted
    # Test 3.1: Restore a single object that has already been permanently deleted (error)
    # Category 4: Bad arguments
    # Test 4.1: Restore a single object with a bad tag argument (error)

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    def test_restore_object_that_exists_and_has_been_removed(self, populated_memory_dao, tags):
        for tag in tags:
            populated_memory_dao.mark_for_deletion(tag=tag, time_of_removal=datetime.now(timezone.utc))
            populated_memory_dao.restore(tag=tag)
            assert populated_memory_dao.exists(tag=tag)

    def test_restore_object_that_exists_and_has_not_been_removed(self, populated_memory_dao):
        with pytest.raises(InMemoryObjectDAOObjectAlreadyExistsError):
            populated_memory_dao.restore(tag='test_list')

    def test_restore_object_that_has_already_been_permanently_deleted(self, populated_memory_dao):
        populated_memory_dao.mark_for_deletion(tag='test_list', time_of_removal=datetime.now(timezone.utc))
        del populated_memory_dao._collection['removed']['test_list']
        with pytest.raises(InMemoryObjectDAOObjectNotFoundError):
            populated_memory_dao.restore(tag='test_list')

    @pytest.mark.parametrize('bad_arg', [1, datetime.now(timezone.utc), {"set"}, {"hash": "map"}])
    def test_restore_with_bad_tag_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.restore(tag=bad_arg)

    # purge tests (test all expected behaviors of purge())
    # ---------------------------------------------------------
    # Category 1: Purge all objects that exist and have been removed
    # Test 1.1: Purge all objects that exist and have been removed using time_threshold=None; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Test 1.2: Purge all objects that exist and have been removed using time_threshold=first_tod; check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Category 2: Purge only some objects that exist and have been removed; check that the resulting purge count is correct
    # Test 2.1: Purge all objects that exist and have been removed using time_threshold=first_tod+timedelta(seconds=1.2); check that they can nolonger be accessed through get or the exists method; check that the resulting purge count is correct
    # Category 3: Objects when none have been removed
    # Test 3.1: Purge all objects that exist and have not been removed; check that the purge count is 0
    # Category 4: Bad arguments
    # Test 4.1: Purge all objects with a bad time_threshold argument (error)

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    def test_purge_all_objects_that_exist_and_have_been_removed(self, populated_memory_dao, tags):
        for tag in tags:
            populated_memory_dao.mark_for_deletion(tag=tag, time_of_removal=datetime.now(timezone.utc))
        count = populated_memory_dao.purge(time_threshold=None)
        assert count == len(tags)
        for tag in tags:
            assert not populated_memory_dao.exists(tag=tag)

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    def test_purge_all_objects_that_exist_and_have_been_removed_with_time_threshold(self, populated_memory_dao, tags):
        for tag in tags:
            populated_memory_dao.mark_for_deletion(tag=tag, time_of_removal=datetime.now(timezone.utc))
        count = populated_memory_dao.purge(time_threshold=datetime.now(timezone.utc))
        assert count == len(tags)
        for tag in tags:
            assert not populated_memory_dao.exists(tag=tag)

    @pytest.mark.parametrize('tags', [['test_list', 'test_str', 'test_bool', 'test_int', 'test_float', 'test_ndarray', 'test_dataarray']])
    @pytest.mark.parametrize('time_threshold_shift', list(map(lambda x: timedelta(seconds=x), range(1, 8))))
    def test_purge_only_some_objects_that_exist_and_have_been_removed(self, populated_memory_dao, tags, time_threshold_shift):
        removal_start_time = datetime.now(timezone.utc)
        for i, tag in enumerate(tags):
            shift = timedelta(seconds=i + 0.5)
            populated_memory_dao.mark_for_deletion(tag=tag, time_of_removal=removal_start_time + shift)
        count = populated_memory_dao.purge(time_threshold=removal_start_time + time_threshold_shift)
        assert count == time_threshold_shift.seconds
        for tag in tags:
            assert not populated_memory_dao.exists(tag=tag)

    def test_purge_all_objects_that_exist_and_have_not_been_removed(self, populated_memory_dao):
        count = populated_memory_dao.purge(time_threshold=None)
        assert count == 0

    @pytest.mark.parametrize('bad_arg', [1, 12.5, {"set"}, {"hash": "map"}])
    def test_purge_with_bad_time_threshold_arg(self, populated_memory_dao, bad_arg):
        with pytest.raises(InMemoryObjectDAOTypeError):
            populated_memory_dao.purge(time_threshold=bad_arg)