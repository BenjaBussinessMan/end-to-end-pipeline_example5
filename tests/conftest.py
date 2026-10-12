import pytest

from lakehouse.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests")
    yield s
    s.stop()
