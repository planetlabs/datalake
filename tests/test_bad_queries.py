import simplejson as json


def get_bad_request(client, params):
    uri = '/v0/archive/files/'
    q = '&'.join(['{}={}'.format(k, v) for k, v in params.iteritems()])
    if q:
        uri += '?' + q
    res = client.get(uri)
    assert res.status_code == 400
    response = json.loads(res.get_data())
    assert 'code' in response
    assert 'message' in response
    return response


def test_no_parameters(client):
    res = get_bad_request(client, {})
    assert res['code'] == 'NoArgs'


def test_no_what_parameter(client):
    res = get_bad_request(client, {'start': 123})
    assert res['code'] == 'NoWhat'


def test_no_work_id_or_interval(client):
    res = get_bad_request(client, {'what': 'syslog'})
    assert res['code'] == 'NoWorkInterval'


def test_work_id_and_start(client):
    params = {
        'what': 'syslog',
        'work_id': 'work123',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_work_id_and_end(client):
    params = {
        'what': 'syslog',
        'work_id': 'work123',
        'end': 345
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_start_without_end(client):
    params = {
        'what': 'syslog',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_end_without_start(client):
    params = {
        'what': 'syslog',
        'end': 345
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidWorkInterval'


def test_invalid_start(client):
    params = {
        'what': 'syslog',
        'start': 'notaninteger',
        'end': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidTime'


def test_invalid_end(client):
    params = {
        'what': 'syslog',
        'end': 'notaninteger',
        'start': 123
    }
    res = get_bad_request(client, params)
    assert res['code'] == 'InvalidTime'
