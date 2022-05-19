from armada_client.client import ArmadaClient, AuthData, AuthMethod

def test_anon_auth():
    anon = AuthData(AuthMethod.Anonymous)
    assert anon.method == AuthMethod.Anonymous

def test_basic_auth():
    basic = AuthData(AuthMethod.Basic, 'test', 'password')
    assert basic.username == 'test'
    assert basic.password == 'password'