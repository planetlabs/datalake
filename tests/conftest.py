import pytest

from datalake_api import app as datalake_api

@pytest.fixture
def client():
    datalake_api.app.config['TESTING'] = True
    return datalake_api.app.test_client()
