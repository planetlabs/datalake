def test_swagger_spec(client):
    res = client.get('/spec/')
    assert res.status_code == 200


def assert_redirect(res, location):
    assert res.status_code == 302
    assert 'Location' in res.headers
    assert res.headers['Location'].endswith(location)


def test_swagger_doc_redirect(client):
    res = client.get('/')
    assert_redirect(res, '/docs/')
    res = client.get('/docs/')
    assert_redirect(res, '/static/index.html')
