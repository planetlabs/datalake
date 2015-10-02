import simplejson as json
import base64


def test_health(client):
    uri = '/health/'
    res = client.get(uri)
    assert res.status_code == 200
