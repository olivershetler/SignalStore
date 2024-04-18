import pytest

def test_fast_ci_cd():
    assert 5 == 5

@pytest.mark.slow()
def test_slow_ci_cd():
    assert 10 == 10