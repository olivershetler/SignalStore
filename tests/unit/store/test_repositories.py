import pytest
from datetime import datetime, timedelta
from signalstore.store.repositories import *

class TestDomainModelRepository:
    # get tests (test all expected behaviors of get())
    # ------------------------------------------------
    # Category 1: get a domain model that exists
    # Test 1.1: get a metamodel that exists
    # Test 1.2: get a property model that exists
    # Test 1.3: get a data model that exists
    # Category 2: get a domain model that does not exist
    # Test 2.1: get a model that does not exist (error)
    # Category 3: get a domain model that exists but is invalid
    # Test 3.1: iterate over all invalid models from conftest and check for ValidationError
    # Category 4: bad arguments
    # Test 4.1: get a domain model with a bad schema_name argument (error)

    @pytest.mark.parametrize("schema_name", ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_get_metamodel_that_exists(self, populated_domain_repo, schema_name):
        metamodel = populated_domain_repo.get(schema_name)
        assert metamodel is not None
        assert metamodel.get('schema_name') == schema_name
        assert metamodel.get('schema_type') == 'metamodel'

    @pytest.mark.parametrize("schema_name", ['unit_of_measure', 'dimension_of_measure', 'time_of_save', 'time_of_removal'])
    def test_get_property_model_that_exists(self, populated_domain_repo, schema_name):
        property_model = populated_domain_repo.get(schema_name)
        assert property_model is not None
        assert property_model.get('schema_name') == schema_name
        assert property_model.get('schema_type') == 'property_model'

    @pytest.mark.parametrize("schema_name", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_get_data_model_that_exists(self, populated_domain_repo, schema_name):
        data_model = populated_domain_repo.get(schema_name)
        assert data_model is not None
        assert data_model.get('schema_name') == schema_name
        assert data_model.get('schema_type') == 'data_model'

    def test_get_metamodel_that_does_not_exist(self, populated_domain_repo):
        assert populated_domain_repo.get('does_not_exist') is None

    @pytest.mark.parametrize("invalid_name_idx", range(17))
    def test_get_invalid_model_that_exists(self, populated_domain_repo, invalid_model_schema_names, invalid_name_idx):
        schema_name = invalid_model_schema_names[invalid_name_idx]
        with pytest.raises(ValidationError):
            populated_domain_repo.get(schema_name)
            assert False, f"Should have raised a ValidationError for schema_name: {schema_name}"

    @pytest.mark.parametrize("bad_schema_name", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c"), {"hash": "map"}])
    def test_get_model_with_bad_schema_name(self, populated_domain_repo, bad_schema_name):
        with pytest.raises(DomainRepositoryTypeError):
            populated_domain_repo.get(bad_schema_name)
            assert False, f"Should have raised a TypeError for schema_name: {bad_schema_name}"

    # find tests (test all expected behaviors of find())
    # --------------------------------------------------
    # Category 1: find models that exist
    # Test 1.1: find all valid models
    # Test 1.2: find all valid metamodels
    # Test 1.3: find all property models
    # Test 1.4: find all data models
    # Test 1.5: find all data models with a specific metamodel_ref
    # Category 2: find with query that does not match any models
    # Test 2.1: find model with schema_name that does not exist; should return empty list
    # Test 2.2: find model with schema_type that does not exist; should return empty list
    # Test 2.3: find model with metamodel_ref that does not exist; should return empty list
    # Category 3: find with query that matches models but is invalid
    # Test 3.1: iterate over all invalid schemas from conftest and check for ValidationError
    # Category 4: bad arguments
    # Test 4.1: find with bad filter argument (error)
    # Test 4.2: find with bad projection argument (error)
    # Test 4.3: find with bad sort argument (error)

    def test_find_all_models(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({})
        n = len(models)
        all = populated_valid_only_domain_repo._dao._collection.find({})
        all = len(list(all))
        assert n == all

    def test_find_all_metamodels(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'schema_type': 'metamodel'})
        n = len(models)
        all = populated_valid_only_domain_repo._dao._collection.find({'schema_type': 'metamodel'})
        all = len(list(all))
        assert n == all

    def test_find_all_property_models(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'schema_type': 'property_model'})
        n = len(models)
        all = populated_valid_only_domain_repo._dao._collection.find({'schema_type': 'property_model'})
        all = len(list(all))
        assert n == all

    def test_find_all_data_models(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'schema_type': 'data_model'})
        n = len(models)
        all = populated_valid_only_domain_repo._dao._collection.find({'schema_type': 'data_model'})
        all = len(list(all))
        assert n == all

    @pytest.mark.parametrize("metamodel_ref", ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_find_data_models_with_specific_metamodel_ref(self, populated_valid_only_domain_repo, metamodel_ref):
        models = populated_valid_only_domain_repo.find({'schema_type': 'data_model', 'metamodel_ref': metamodel_ref})
        assert len(models) > 0
        for model in models:
            assert model.get('metamodel_ref') == metamodel_ref

    def test_find_model_with_schema_name_that_does_not_exist(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'schema_name': 'does_not_exist'})
        assert len(models) == 0

    def test_find_model_with_schema_type_that_does_not_exist(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'schema_type': 'does_not_exist'})
        assert len(models) == 0

    def test_find_model_with_metamodel_ref_that_does_not_exist(self, populated_valid_only_domain_repo):
        models = populated_valid_only_domain_repo.find({'metamodel_ref': 'does_not_exist'})
        assert len(models) == 0

    @pytest.mark.parametrize("invalid_name_idx", range(17))
    def test_find_invalid_model_that_exists(self, populated_domain_repo, invalid_model_schema_names, invalid_name_idx):
        schema_name = invalid_model_schema_names[invalid_name_idx]
        with pytest.raises(ValidationError):
            populated_domain_repo.find({'schema_name': schema_name})
            assert False, f"Should have raised a ValidationError for schema_name: {schema_name}"

    @pytest.mark.parametrize("bad_filter", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_find_with_bad_filter(self, populated_valid_only_domain_repo, bad_filter):
        with pytest.raises(DomainRepositoryTypeError):
            populated_valid_only_domain_repo.find(bad_filter)
            assert False, f"Should have raised a TypeError for filter: {bad_filter}"

    @pytest.mark.parametrize("bad_projection", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_find_with_bad_projection(self, populated_valid_only_domain_repo, bad_projection):
        with pytest.raises(DomainRepositoryTypeError):
            populated_valid_only_domain_repo.find({}, bad_projection)
            assert False, f"Should have raised a TypeError for projection: {bad_projection}"

    # exists tests (test all expected behaviors of exists())
    # ------------------------------------------------------
    # Category 1: exists with model that exists
    # Test 1.1: exists with model that exists returns true
    # Category 2: exists with model that does not exist
    # Test 2.1: exists with model that does not exist returns false
    # Category 3: exists with model that exists but is invalid
    # Test 3.1: iterate over all invalid schemas from conftest; should return True for all
    # Category 4: bad arguments
    # Test 4.1: exists with bad schema_name argument (error)

    @pytest.mark.parametrize("schema_name", ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_exists_with_model_that_exists(self, populated_domain_repo, schema_name):
        assert populated_domain_repo.exists(schema_name)

    def test_exists_with_model_that_does_not_exist(self, populated_domain_repo):
        assert not populated_domain_repo.exists('does_not_exist')

    @pytest.mark.parametrize("invalid_name_idx", range(17))
    def test_exists_with_invalid_model_that_exists(self, populated_domain_repo, invalid_model_schema_names, invalid_name_idx):
        schema_name = invalid_model_schema_names[invalid_name_idx]
        assert populated_domain_repo.exists(schema_name), f"Should have returned true for schema_name: {schema_name}"

    @pytest.mark.parametrize("bad_schema_name", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c"), {"hash": "map"}])
    def test_exists_with_bad_schema_name(self, populated_domain_repo, bad_schema_name):
        with pytest.raises(DomainRepositoryTypeError):
            populated_domain_repo.exists(bad_schema_name)
            assert False, f"Should have raised a TypeError for schema_name: {bad_schema_name}"

    # add tests (test all expected behaviors of add())
    # ------------------------------------------------
    # Category 1: add a model that does not exist
    # Test 1.1: add a metamodel that does not exist
    # Test 1.2: add a property model that does not exist
    # Test 1.3: add a data model that does not exist
    # Category 2: add a model that exists
    # Test 2.1: add a metamodel that exists (error)
    # Test 2.2: add a property model that exists (error)
    # Test 2.3: add a data model that exists (error)
    # Category 3: add a model that does not exist but is invalid
    # Test 3.1: iterate over all invalid schemas from conftest and check for ValidationError
    # Category 4: bad arguments
    # Test 4.1: add a model with a bad model argument (error)

    def test_add_metamodel_that_does_not_exist(self, populated_domain_repo, metamodels):
        new_metamodel = metamodels[0].copy()
        new_name = 'new_metamodel'
        new_metamodel['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        populated_domain_repo.add(new_metamodel)
        assert populated_domain_repo.exists(new_name)

    def test_add_property_model_that_does_not_exist(self, populated_domain_repo, property_models):
        new_property_model = property_models[0].copy()
        new_name = 'new_property_model'
        new_property_model['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        populated_domain_repo.add(new_property_model)
        assert populated_domain_repo.exists(new_name)

    def test_add_data_model_that_does_not_exist(self, populated_domain_repo, data_models):
        new_data_model = data_models[0].copy()
        new_name = 'new_data_model'
        new_data_model['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        populated_domain_repo.add(new_data_model)
        assert populated_domain_repo.exists(new_name)

    def test_add_metamodel_that_exists(self, populated_domain_repo, metamodels):
        existing_metamodel = metamodels[0].copy()
        assert populated_domain_repo.exists(existing_metamodel['schema_name'])
        with pytest.raises(DomainRepositoryModelAlreadyExistsError):
            populated_domain_repo.add(existing_metamodel)
            assert False, f"Should have raised a DomainRepositoryExistsError for schema_name: {existing_metamodel['schema_name']}"

    def test_add_property_model_that_exists(self, populated_domain_repo, property_models):
        existing_property_model = property_models[0].copy()
        assert populated_domain_repo.exists(existing_property_model['schema_name'])
        with pytest.raises(DomainRepositoryModelAlreadyExistsError):
            populated_domain_repo.add(existing_property_model)
            assert False, f"Should have raised a DomainRepositoryExistsError for schema_name: {existing_property_model['schema_name']}"

    def test_add_data_model_that_exists(self, populated_domain_repo, data_models):
        existing_data_model = data_models[0].copy()
        assert populated_domain_repo.exists(existing_data_model['schema_name'])
        with pytest.raises(DomainRepositoryModelAlreadyExistsError):
            populated_domain_repo.add(existing_data_model)
            assert False, f"Should have raised a DomainRepositoryExistsError for schema_name: {existing_data_model['schema_name']}"

    def test_add_invalid_metamodel_that_does_not_exist(self, populated_domain_repo, invalid_property_models):
        invalid_model = invalid_property_models[0].copy()
        new_name = 'invalid_model'
        invalid_model['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        with pytest.raises(ValidationError):
            populated_domain_repo.add(invalid_model)
            assert False, f"Should have raised a ValidationError for schema_name: {new_name}"

    def test_add_invalid_property_model_that_does_not_exist(self, populated_domain_repo, invalid_data_models):
        invalid_model = invalid_data_models[0].copy()
        new_name = 'invalid_model'
        invalid_model['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        with pytest.raises(ValidationError):
            populated_domain_repo.add(invalid_model)
            assert False, f"Should have raised a ValidationError for schema_name: {new_name}"

    def test_add_invalid_data_model_that_does_not_exist(self, populated_domain_repo, invalid_data_models):
        invalid_model = invalid_data_models[0].copy()
        new_name = 'invalid_model'
        invalid_model['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        with pytest.raises(ValidationError):
            populated_domain_repo.add(invalid_model)
            assert False, f"Should have raised a ValidationError for schema_name: {new_name}"

    @pytest.mark.parametrize("bad_model", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_add_model_with_bad_model(self, populated_domain_repo, bad_model):
        with pytest.raises(DomainRepositoryTypeError):
            populated_domain_repo.add(bad_model)
            assert False, f"Should have raised a TypeError for model: {bad_model}"

    # remove tests (test all expected behaviors of remove())
    # ------------------------------------------------------
    # Category 1: remove a model that exists
    # Test 1.1: remove a metamodel that exists; check they nolonger exist; check get returns None
    # Test 1.2: remove a property model that exists; check they nolonger exist; check get returns None
    # Test 1.3: remove a data model that exists; check they nolonger exist; check get returns None
    # Category 2: remove a model that does not exist
    # Test 2.1: remove a model that does not exist (error)
    # Category 3: remove a model that exists but is invalid
    # Test 3.1: iterate over all invalid schemas from conftest; check that they nologner exist; check get returns None
    # Category 4: bad arguments
    # Test 4.1: remove a model with a bad schema_name argument (error)

    @pytest.mark.parametrize("schema_name", ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_remove_metamodel_that_exists(self, populated_domain_repo, schema_name):
        assert populated_domain_repo.exists(schema_name)
        populated_domain_repo.remove(schema_name)
        assert not populated_domain_repo.exists(schema_name)
        assert populated_domain_repo.get(schema_name) is None

    @pytest.mark.parametrize("schema_name", ['unit_of_measure', 'dimension_of_measure', 'time_of_save', 'time_of_removal'])
    def test_remove_property_model_that_exists(self, populated_domain_repo, schema_name):
        assert populated_domain_repo.exists(schema_name)
        populated_domain_repo.remove(schema_name)
        assert not populated_domain_repo.exists(schema_name)
        assert populated_domain_repo.get(schema_name) is None

    @pytest.mark.parametrize("schema_name", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_remove_data_model_that_exists(self, populated_domain_repo, schema_name):
        assert populated_domain_repo.exists(schema_name)
        populated_domain_repo.remove(schema_name)
        assert not populated_domain_repo.exists(schema_name)
        assert populated_domain_repo.get(schema_name) is None

    def test_remove_model_that_does_not_exist(self, populated_domain_repo):
        with pytest.raises(DomainRepositoryModelNotFoundError):
            populated_domain_repo.remove('does_not_exist')
            assert False, f"Should have raised a DomainRepositoryDoesNotExistError for schema_name: does_not_exist"

    @pytest.mark.parametrize("invalid_name_idx", range(17))
    def test_remove_invalid_model_that_exists(self, populated_domain_repo, invalid_model_schema_names, invalid_name_idx):
        schema_name = invalid_model_schema_names[invalid_name_idx]
        assert populated_domain_repo.exists(schema_name)
        populated_domain_repo.remove(schema_name)
        assert not populated_domain_repo.exists(schema_name)
        assert populated_domain_repo.get(schema_name) is None

    @pytest.mark.parametrize("bad_schema_name", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c"), {"hash": "map"}])
    def test_remove_model_with_bad_schema_name(self, populated_domain_repo, bad_schema_name):
        with pytest.raises(DomainRepositoryTypeError):
            populated_domain_repo.remove(bad_schema_name)
            assert False, f"Should have raised a TypeError for schema_name: {bad_schema_name}"

    # undo tests (test all expected behaviors of undo())
    # --------------------------------------------------
    # Category 1: undo add
    # Test 1.1: undo adding a new model
    # Test 1.2: undo all the adding that was done on initialization of the populated_domain_repo fixture
    # Category 2: undo remove
    # Test 2.1: undo removing a model
    # Category 3: undo when there is nothing to undo
    # Test 3.1: undo when there is nothing to undo returns None

    def test_undo_add(self, populated_domain_repo, metamodels):
        new_metamodel = metamodels[0].copy()
        new_name = 'new_metamodel'
        new_metamodel['schema_name'] = new_name
        assert not populated_domain_repo.exists(new_name)
        n_ops = len(populated_domain_repo._operation_history)
        populated_domain_repo.add(new_metamodel)
        assert len(populated_domain_repo._operation_history) == n_ops + 1
        assert populated_domain_repo.exists(new_name)
        result = populated_domain_repo.undo()
        assert result.schema_name == new_name
        assert not populated_domain_repo.exists(new_name)
        assert len(populated_domain_repo._operation_history) == n_ops

    def test_undo_all_adds(self, empty_domain_repo, metamodels, property_models, data_models):
        for model_type in [metamodels, property_models, data_models]:
            for model in model_type:
                empty_domain_repo.add(model)
                assert empty_domain_repo.exists(model['schema_name'])
        assert len(empty_domain_repo._operation_history) == len(metamodels) + len(property_models) + len(data_models)
        results = empty_domain_repo.undo_all()
        assert len(results) == len(metamodels) + len(property_models) + len(data_models)
        assert len(empty_domain_repo._operation_history) == 0
        for model_type in [metamodels, property_models, data_models]:
            for model in model_type:
                assert not empty_domain_repo.exists(model['schema_name'])

    def test_undo_remove(self, populated_domain_repo):
        schema_name = 'record_metamodel'
        assert populated_domain_repo.exists(schema_name)
        n_ops = len(populated_domain_repo._operation_history)
        populated_domain_repo.remove(schema_name)
        assert len(populated_domain_repo._operation_history) == n_ops + 1
        assert not populated_domain_repo.exists(schema_name)
        result = populated_domain_repo.undo()
        assert result.schema_name == schema_name
        assert populated_domain_repo.exists(schema_name)
        assert len(populated_domain_repo._operation_history) == n_ops

    def test_undo_all_remove(self, populated_valid_only_domain_repo, metamodels, property_models, data_models):
        for model_type in [metamodels, property_models, data_models]:
            for model in model_type:
                populated_valid_only_domain_repo.remove(model['schema_name'])
                assert not populated_valid_only_domain_repo.exists(model['schema_name'])
        assert len(populated_valid_only_domain_repo._operation_history) == len(metamodels) + len(property_models) + len(data_models)
        results = populated_valid_only_domain_repo.undo_all()
        assert len(results) == len(metamodels) + len(property_models) + len(data_models)
        assert len(populated_valid_only_domain_repo._operation_history) == 0

    def test_undo_when_nothing_to_undo(self, populated_domain_repo):
        n_ops = len(populated_domain_repo._operation_history)
        populated_domain_repo.undo()
        assert len(populated_domain_repo._operation_history) == n_ops

    # clear operation history tests (test all expected behaviors of clear_operation_history())
    # no tests because this is just a one-liner that sets the operation history to an empty list

    # list_marked_for_deletion tests (test all expected behaviors of list_marked_for_deletion())
    # No tests because this passes through to the DAO and is tested there

    # purge tests (test all expected behaviors of purge())
    # no tests because this passes through to the DAO and is tested there

    # validate tests (test all expected behaviors of validate())
    # ----------------------------------------------------------
    # Category 1: validate a model that is valid
    # Test 1.1: validate a metamodel that is valid
    # Test 1.2: validate a property model that is valid
    # Test 1.3: validate a data model that is valid
    # Category 2: validate a model that is invalid (all the ways it can be invalid)
    # validation requirements
    #   - schema_name must be a string
    #   - schema_name must not be empty
    #   - schema_name must not contain spaces
    #   - schema_name must not contain upper case characters
    #   - schema_name must contain alphanumeric characters and underscores only
    #   - schema_name must not contain double underscores
    #   - schema_name must not contain leading or trailing underscores
    #   - schema_name must not contain leading numbers
    #   - schema_name must not contain the substring 'time_of_removal' because it is used in filenames in the file dao
    #   - schema_type must be one of 'metamodel', 'property_model', 'data_model'
    #   - metamodel_ref must be a string
    #   - metamodel_ref must be a valid schema_name
    #   - metamodel_ref must be an existing valid metamodel
    #   - schema_description must be a string
    #   - schema_description must not be empty
    #   - schema_description must not contain leading or trailing spaces
    #   - json_schema must be a dict
    #   - json_schema must not be empty
    #   - json_schema must contain a 'type' key
    #   - if the 'model_type' is 'metamodel' or 'data_model' then json_schema['type'] must be the string 'object'
    #   - json_schema must be a generally valid json schema
    #   - if schema_type is 'data_model' then it must contain a 'metamodel_ref' key
    #   - schema_title cannot be empty
    #   - schema_title can only contain alphanumeric characters and spaces
    #   - schema_title should have capital letters at the start of each word
    # Test 2.1: validate a model that is invalid because schema_name is not a string
    # Test 2.2: validate a model that is invalid because schema_name is empty
    # Test 2.3: validate a model that is invalid because schema_name contains spaces
    # Test 2.4: validate a model that is invalid because schema_name contains uppercase characters
    # Test 2.5: validate a model that is invalid because schema_name contains other invalid characters
    # Test 2.6: validate a model that is invalid because schema_name contains double underscores
    # Test 2.7: validate a model that is invalid because schema_name contains leading or trailing underscores
    # Test 2.8: validate a model that is invalid because schema_name contains leading numbers
    # Test 2.9: validate a model that is invalid because schema_name contains the substring 'time_of_removal'
    # Test 2.10: validate a model that is invalid because schema_type is not one of 'metamodel', 'property_model', 'data_model'
    # Test 2.11: validate a model that is invalid because metamodel_ref is not a string
    # Test 2.12: validate a model that is invalid because metamodel_ref is not a valid schema_name
    # Test 2.13: validate a model that is invalid because metamodel_ref is not an existing valid metamodel
    # Test 2.14: validate a model that is invalid because schema_description is not a string
    # Test 2.15: validate a model that is invalid because schema_description is empty
    # Test 2.16: validate a model that is invalid because schema_description contains leading or trailing spaces
    # Test 2.17: validate a model that is invalid because json_schema is not a dict
    # Test 2.18: validate a model that is invalid because json_schema is empty
    # Test 2.19: validate a model that is invalid because json_schema does not contain a 'type' key
    # Test 2.20: validate a data model that is invalid because json_schema['type'] is not the string 'object'
    # Test 2.21: validate a metamodel that is invalid because json_schema['type'] is not the string 'object'
    # Test 2.22: validate a model that is invalid because json_schema is not a generally valid json schema
    # Test 2.23: validate a model that is invalid because schema_type is 'data_model' but it does not contain a 'metamodel_ref' key
    # Category 3: bad arguments
    # Test 3.1: validate a model with a bad model argument (error)

    @pytest.mark.parametrize("schema_name", ['record_metamodel', 'xarray_dataarray_metamodel'])
    def test_validate_metamodel_that_is_valid(self, populated_domain_repo, schema_name):
        metamodel = populated_domain_repo.get(schema_name)
        assert metamodel is not None
        populated_domain_repo._validate(metamodel)

    @pytest.mark.parametrize("schema_name", ['unit_of_measure', 'dimension_of_measure', 'time_of_save', 'time_of_removal'])
    def test_validate_property_model_that_is_valid(self, populated_domain_repo, schema_name):
        property_model = populated_domain_repo.get(schema_name)
        assert property_model is not None
        populated_domain_repo._validate(property_model)

    @pytest.mark.parametrize("schema_name", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_validate_data_model_that_is_valid(self, populated_domain_repo, schema_name):
        data_model = populated_domain_repo.get(schema_name)
        assert data_model is not None
        populated_domain_repo._validate(data_model)


    @pytest.mark.parametrize("bad_type_value", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_schema_name_is_not_a_string(self, populated_domain_repo, models, bad_type_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(model)
        # change schema_name to be an int
        model['schema_name'] = bad_type_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(model)
            assert False, f"Should have raised a ValidationError for schema_name: 1"

    @pytest.mark.parametrize("bad_value", ['', ' ', '  ', '5tarts_with_number', 'double___underscore', 'contains-dash', 'contains space', 'has_non_@alpha_num*ric_chars', 'hasCapitalLetters', '_leading_underscore', 'trailing_underscore_', '_starting_underscore'])
    def test_validate_model_that_is_invalid_because_schema_name_is_invalid(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_name to be an int
        data_model['schema_name'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_name: {bad_value}"

    @pytest.mark.parametrize("bad_value", ['not_a_valid_schema_type', 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_schema_type_is_invalid(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_name to be an int
        data_model['schema_type'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_type: {bad_value}"

    @pytest.mark.parametrize("bad_value", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_metamodel_ref_is_not_a_string(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_name to be an int
        data_model['metamodel_ref'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for metamodel_ref: {bad_value}"

    @pytest.mark.parametrize("bad_value", ['', ' ', '  ', '5tarts_with_number', 'double___underscore', 'contains-dash', 'contains space', 'has_non_@alpha_num*ric_chars', 'hasCapitalLetters', '_leading_underscore', 'trailing_underscore_', '_starting_underscore'])
    def test_validate_model_that_is_invalid_because_metamodel_ref_is_invalid(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_name to be an int
        data_model['metamodel_ref'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for metamodel_ref: {bad_value}"

    @pytest.mark.parametrize("not_existing_ref", ["does_not_exist", "not_a_valid_schema_name"])
    def test_validate_model_that_is_invalid_because_metamodel_ref_does_not_exist(self, populated_domain_repo, models, not_existing_ref):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_name to be an int
        data_model['metamodel_ref'] = not_existing_ref
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for metamodel_ref: {not_existing_ref}"

    @pytest.mark.parametrize("bad_value", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_schema_description_is_not_a_string(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_description to be an int
        data_model['schema_description'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_description: {bad_value}"

    @pytest.mark.parametrize("bad_value", ['', ' ', '  '])
    def test_validate_model_that_is_invalid_because_schema_description_is_empty(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_description to be an int
        data_model['schema_description'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_description: {bad_value}"

    @pytest.mark.parametrize("bad_value", ['  starts_with_space', 'ends_with_space  ', '  has_space  '])
    def test_validate_model_that_is_invalid_because_schema_description_contains_leading_or_trailing_spaces(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = models[idx].copy()
        data_model['schema_description'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_description: {bad_value}"

    @pytest.mark.parametrize("bad_value", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_json_schema_is_not_a_dict(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change json_schema to be an int
        data_model['json_schema'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for json_schema: {bad_value}"

    def test_validate_model_that_is_invalid_because_json_schema_does_not_have_type_key(self, populated_domain_repo, models):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        data_model = models[idx].copy()
        data_model['json_schema'] = data_model['json_schema'].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # remove the type key from json_schema
        del data_model['json_schema']['type']
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for json_schema: {data_model['json_schema']}"

    @pytest.mark.parametrize("non_object_json_type_names", ['string', 'number', 'integer', 'boolean', 'array', 'null'])
    def test_validate_data_model_that_is_invalid_because_json_schema_type_is_not_object(self, populated_domain_repo, data_models, non_object_json_type_names):
        # get the number of data models
        n = len(data_models)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        data_model = data_models[idx].copy()
        data_model['json_schema'] = data_model['json_schema'].copy()
        # validate before changing the model
        populated_domain_repo._validate(data_model)
        # change json_schema to be an int
        data_model['json_schema']['type'] = non_object_json_type_names
        with pytest.raises(DomainRepositoryValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for json_schema: {data_model['json_schema']}"

    @pytest.mark.parametrize("non_object_json_type_names", ['string', 'number', 'integer', 'boolean', 'array', 'null'])
    def test_validate_metamodel_that_is_invalid_because_json_schema_type_is_not_object(self, populated_domain_repo, metamodels, non_object_json_type_names):
        # get the number of data models
        n = len(metamodels)
        # generate a random number between 0 and n-1
        idx = np.random.randint(0, n-1)
        # get a valid data model
        metamodel = metamodels[idx].copy()
        metamodel['json_schema'] = metamodel['json_schema'].copy()
        # validate before changing the model
        populated_domain_repo._validate(metamodel)
        # change json_schema to be an int
        metamodel['json_schema']['type'] = non_object_json_type_names
        with pytest.raises(DomainRepositoryValidationError):
            populated_domain_repo._validate(metamodel)
            assert False, f"Should have raised a ValidationError for json_schema: {metamodel['json_schema']}"


    @pytest.mark.parametrize("bad_value", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_validate_model_that_is_invalid_because_json_schema_is_not_a_valid_json_schema(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        model = models[idx].copy()
        model['json_schema'] = model['json_schema'].copy()
        # validate model before changing it
        populated_domain_repo._validate(model)
        # change json_schema to be an int
        model['json_schema'] = bad_value
        with pytest.raises(DomainRepositoryValidationError):
            populated_domain_repo._validate(model)
            assert False, f"Should have raised a ValidationError for json_schema: {bad_value}"

    def test_validate_data_model_that_is_invalid_because_json_schema_does_not_contain_metamodel_ref(self, populated_domain_repo, data_models):
        # get the number of data models
        n = len(data_models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        data_model = data_models[idx].copy()
        # validate before removing the key
        populated_domain_repo._validate(data_model)
        # remove the metamodel_ref key from json_schema
        del data_model['metamodel_ref']
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for json_schema: {data_model['json_schema']}"

    @pytest.mark.parametrize("bad_value", ['', ' ', '  ', "Has Invalid Ch@racters", "has-dashes", "has_underscores", "has\nnew\nlines"])
    def test_validate_model_that_is_invalid_because_schema_title_is_invalid(self, populated_domain_repo, models, bad_value):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        data_model = models[idx].copy()
        # validate model before changing it
        populated_domain_repo._validate(data_model)
        # change schema_title to be an int
        data_model['schema_title'] = bad_value
        with pytest.raises(ValidationError):
            populated_domain_repo._validate(data_model)
            assert False, f"Should have raised a ValidationError for schema_title: {bad_value}"

    @pytest.mark.parametrize("valid_title", ['Valid Title', 'valid lowercase title', 'Valid Title With Numbers 123'])
    def test_valid_title(self, populated_domain_repo, models, valid_title):
        # get the number of data models
        n = len(models)
        # generate a random number between 0 and n-1
        idx = 0
        # get a valid data model
        model = models[idx].copy()
        # change schema_title to be an int
        model['schema_title'] = valid_title
        populated_domain_repo._validate(model)


class TestDataRepository:

    # get tests (test all expected behaviors of get())
    # ------------------------------------------------
    # Category 1: get a data object that exists
    # Test 1.1: get an unversioned record data object (has_file=False) that exists
    # Test 1.1: get an unversioned data array object that exists (has_file=True)
    # Test 1.2: get a versioned data object that exists with a specific version (has_file=True)
    # Category 2: get a data object that exists but is invalid (error)
    # Test2.1: get an unversioned record data object (has_file=False) that exists but is invalid (error)
    # Category 3: get a data object that does not exis
    # Test 3.1: get a data object that does not exist; check that it returns None
    # Category 4: bad arguments
    # Test 4.1: get a data object with a bad schema_ref argument (error)
    # Test 4.2: get a data object with a bad data_name argument (error)
    # Test 4.3: get a data object with a bad data_version argument (error)

    @pytest.mark.parametrize("schema_ref", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_get_unversioned_data_object_that_exists(self, populated_data_repo, schema_ref):
        data_object = populated_data_repo.get(
            schema_ref=schema_ref,
            data_name="test",
            version_timestamp=0
            )
        assert data_object is not None, f"Should have returned a data object for schema_ref: {schema_ref} and data_name: test"
        if isinstance(data_object, dict):
            assert data_object['schema_ref'] == schema_ref
            assert data_object['data_name'] == "test"
            assert data_object.get('version_timestamp') == 0
        else:
            assert data_object.attrs['schema_ref'] == schema_ref
            assert data_object.attrs['data_name'] == "test"
            assert data_object.attrs.get('version_timestamp') == 0

    @pytest.mark.parametrize("time_delta", [s for s in range(1, 11)])
    def test_get_versioned_data_object_that_exists(self, populated_data_repo, timestamp, time_delta, model_numpy_adapter):
        vts = timestamp + timedelta(seconds=time_delta)
        data_object = populated_data_repo.get(schema_ref='numpy_test', data_name="numpy_test", version_timestamp=vts, data_adapter=model_numpy_adapter)
        assert data_object.attrs['schema_ref'] == 'numpy_test'
        assert data_object.attrs['data_name'] == 'numpy_test'
        assert data_object.attrs['version_timestamp'] == vts

    @pytest.mark.parametrize("kwargs", [{'schema_ref': 'session', 'data_name': 'invalid_session_date'}, {'schema_ref': 'session', 'data_name': 'invalid_session_has_file'}]) # ,{'schema_ref': 'does_not_exist', 'data_name': 'non_existing_schema_ref'}
    def test_get_unversioned_record_that_exists_but_is_invalid(self, populated_data_repo_with_invalid_records, kwargs):
        repo = populated_data_repo_with_invalid_records
        with pytest.raises(DataRepositoryValidationError):
            record = repo.get(**kwargs)
            raise Exception(f"Should have raised a DataRepositoryValidationError for kwargs: {kwargs} for record: \n\n{record}")

    def test_get_data_object_that_does_not_exist(self, populated_data_repo):
        data_object = populated_data_repo.get(schema_ref='does_not_exist', data_name='does_not_exist', version_timestamp=0)
        assert data_object is None, f"Should have returned None for schema_ref: does_not_exist and data_name: does_not_exist"

    @pytest.mark.parametrize("bad_schema_ref", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_get_data_object_with_bad_schema_ref(self, populated_data_repo, bad_schema_ref):
        with pytest.raises(DataRepositoryTypeError):
            populated_data_repo.get(schema_ref=bad_schema_ref, data_name='does_not_exist', version_timestamp=0)
            assert False, f"Should have raised a TypeError for schema_ref: {bad_schema_ref}"

    @pytest.mark.parametrize("bad_data_name", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_get_data_object_with_bad_data_name(self, populated_data_repo, bad_data_name):
        with pytest.raises(DataRepositoryTypeError):
            populated_data_repo.get(schema_ref='does_not_exist', data_name=bad_data_name, version_timestamp=0)
            assert False, f"Should have raised a TypeError for data_name: {bad_data_name}"

    @pytest.mark.parametrize("bad_version_timestamp", [1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_get_data_object_with_bad_version_timestamp(self, populated_data_repo, bad_version_timestamp):
        with pytest.raises(Exception):
            populated_data_repo.get(schema_ref='does_not_exist', data_name='does_not_exist', version_timestamp=bad_version_timestamp)
            assert False, f"Should have raised an Exception for version_timestamp: {bad_version_timestamp}"

    # exists tests (test all expected behaviors of exists())
    # ------------------------------------------------------
    # Category 1: exists a data object that exists
    # Test 1.1: exists an unversioned record data object (has_file=False) that exists
    # Test 1.1: exists an unversioned data array object that exists (has_file=True)
    # Test 1.2: exists a versioned data object that exists with a specific version (has_file=True)
    # Category 2: exists a data object that does not exist
    # Test 2.1: exists a data object that does not exist; check that it returns False
    # Category 3: bad arguments
    # Test 3.1: exists a data object with a bad schema_ref argument (error)
    # Test 3.2: exists a data object with a bad data_name argument (error)
    # Test 3.3: exists a data object with a bad data_version argument (error)

    @pytest.mark.parametrize("schema_ref", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_exists_unversioned_data_object_that_exists(self, populated_data_repo, schema_ref):
        assert populated_data_repo.exists(schema_ref=schema_ref, data_name="test", version_timestamp=0)

    @pytest.mark.parametrize("time_delta", [timedelta(seconds=s) for s in range(1, 11)])
    def test_exists_versioned_data_object_that_exists(self, populated_data_repo, model_numpy_adapter, timestamp, time_delta):
        vts = timestamp + time_delta
        assert populated_data_repo.exists(schema_ref='numpy_test', data_name="numpy_test", version_timestamp=vts)

    def test_exists_data_object_that_does_not_exist(self, populated_data_repo):
        assert not populated_data_repo.exists(schema_ref='does_not_exist', data_name='does_not_exist', version_timestamp=0)

    @pytest.mark.parametrize("bad_schema_ref", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_exists_data_object_with_bad_schema_ref(self, populated_data_repo, bad_schema_ref):
        with pytest.raises(DataRepositoryTypeError):
            populated_data_repo.exists(schema_ref=bad_schema_ref, data_name='does_not_exist', version_timestamp=0)
            assert False, f"Should have raised a TypeError for schema_ref: {bad_schema_ref}"

    @pytest.mark.parametrize("bad_data_name", [None, 1, 1.0, [1,2,3], {"x",1,2}, ("a", "b", "c")])
    def test_exists_data_object_with_bad_data_name(self, populated_data_repo, bad_data_name):
        with pytest.raises(DataRepositoryTypeError):
            populated_data_repo.exists(schema_ref='does_not_exist', data_name=bad_data_name, version_timestamp=0)
            assert False, f"Should have raised a TypeError for data_name: {bad_data_name}"

    # find tests (test all expected behaviors of find())
    # --------------------------------------------------
    # Category 1: find a data object that exists
    # Test 1.1: find an unversioned record data objects (has_file=False) that exist
    # Test 1.1: find an unversioned data array objects that exists (has_file=True)
    # Test 1.2: find all the numpy versioned data objects that exist
    # Test 1.3: find all the numpy versioned data objects that exist and have a version_timestamp that is greater than or equal to a specific timestamp
    # Category 2: find a data object that does not exist
    # Test 2.1: find a data object that does not exist; check that it returns an empty list
    # Category 3: bad arguments
    # Test 3.1: find a data object with a bad filter argument (error)
    # Test 3.2: find a data object with a bad projection argument (error)
    # Test 3.3: find a data object with a bad sort argument (error)

    @pytest.mark.parametrize("schema_ref", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_find_unversioned_data_object_that_exists(self, populated_data_repo, schema_ref):
        query_filter = {'schema_ref': schema_ref, 'data_name': 'test', 'version_timestamp': 0}
        data_objects = populated_data_repo.find(filter=query_filter)
        assert len(data_objects) > 0
        for data_object in data_objects:
            if isinstance(data_object, dict):
                assert data_object['schema_ref'] == schema_ref
                assert data_object['data_name'] == "test"
                assert data_object.get('version_timestamp') == 0
            else:
                assert data_object.attrs['schema_ref'] == schema_ref
                assert data_object.attrs['data_name'] == "test"
                assert data_object.attrs.get('version_timestamp') == 0

    def test_find_versioned_data_object_that_exists(self, populated_data_repo):
        query_filter = {'schema_ref': 'numpy_test', 'data_name': 'numpy_test'}
        data_objects = populated_data_repo.find(filter=query_filter)
        assert len(data_objects) > 0
        for data_object in data_objects:
            assert data_object['schema_ref'] == 'numpy_test'
            assert data_object['data_name'] == "numpy_test"

    @pytest.mark.parametrize("time_delta", [timedelta(seconds=s) for s in range(1, 11)])
    def test_find_versioned_data_object_that_exists_with_timestamp_filter(self, populated_data_repo, timestamp, time_delta):
        vts = timestamp + time_delta
        query_filter = {'schema_ref': 'numpy_test', 'data_name': 'numpy_test', 'version_timestamp': {'$gte': vts}}
        data_objects = populated_data_repo.find(filter=query_filter)
        assert len(data_objects) > 0
        for data_object in data_objects:
            assert data_object['schema_ref'] == 'numpy_test'
            assert data_object['data_name'] == "numpy_test"
            #TODO: fix the offset comparison issue that prevents the last assertion from passing
            try:
                assert data_object['version_timestamp'] >= timestamp, f"version_timestamp: {data_object['version_timestamp']} is not greater than or equal to timestamp: {timestamp}."
            except Exception as e:
                message = f"version_timestamp: {data_object['version_timestamp']} is not greater than or equal to timestamp: {timestamp}; error: {e}"
                raise Exception(message)


    def test_find_data_object_that_does_not_exist(self, populated_data_repo):
        query_filter = {'schema_ref': 'does_not_exist', 'data_name': 'does_not_exist'}
        data_objects = populated_data_repo.find(filter=query_filter)
        assert len(data_objects) == 0

    # add tests (test all expected behaviors of add())
    # ------------------------------------------------
    # Category 1: add a data object that is valid
    # Test 1.1: add a valid unversioned record data object (has_file=False)
    # Test 1.2: add a valid unversioned data array object (has_file=True)
    # Test 1.3: add a valid versioned data object
    # Category 2: add a data object that is invalid (error)
    # Test 2.1: add an invalid unversioned record data object (has_file=False)
    # Test 2.2: add an invalid unversioned data array object (has_file=True)
    # Test 2.3: add an invalid versioned data object
    # Category 3: bad arguments
    # Test 3.1: add a data object with a bad data_object argument (error)
    # Test 3.2: add a data object with a bad schema_ref argument (error)
    # Test 3.3: add a data object with a bad data_name argument (error)
    # Test 3.4: add a data object with a bad data_version argument (error)
    # Test 3.5: add a data object with a bad data_adapter argument (error)

    @pytest.mark.parametrize("schema_ref", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_add_unversioned_data_object_that_is_valid(self, populated_data_repo, schema_ref):
        data_object = populated_data_repo.get(schema_ref=schema_ref, data_name="test", version_timestamp=0)
        if isinstance(data_object, dict):
            data_object['data_name'] = 'test_add'
        elif isinstance(data_object, xr.DataArray):
            data_object.attrs['data_name'] = 'test_add'
        populated_data_repo.add(data_object)

    @pytest.mark.parametrize("schema_ref", ['animal', 'session', 'spike_times', 'spike_waveforms'])
    def test_add_versioned_data_object_that_is_valid(self, populated_data_repo, schema_ref, timestamp):
        data_object = populated_data_repo.get(schema_ref=schema_ref, data_name="test", version_timestamp=0)
        if isinstance(data_object, dict):
            if 'version_timestamp' in data_object:
                del data_object['version_timestamp']
        else:
            if 'version_timestamp' in data_object.attrs:
                del data_object.attrs['version_timestamp']
        populated_data_repo.add(data_object, versioning_on=True)
        sleep(0.001)