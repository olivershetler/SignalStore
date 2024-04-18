

class TestUnitOfWork:

    def test_initialize(self, unit_of_work):
        with unit_of_work as uow:
            pass