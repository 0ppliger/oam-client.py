import asset_model
from oam_client import Client

def test_client():
    client = Client("world")

    assert client.name == "world"
    assert type(client.asset) is asset_model.FQDN
