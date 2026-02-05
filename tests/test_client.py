import pytest
from asset_model import FQDN, IPAddress, BasicDNSRelation, SourceProperty, RRHeader
from oam_client import BrokerClient


@pytest.fixture
def emit():
    return BrokerClient(
        "https://localhost:443",
        keylog_filename="sslkeylog.txt",
        verify=False)


def test_create_entity(emit):
    emit.create_entity(FQDN(name="create0.org"))


def test_delete_entity(emit):
    r = emit.create_entity(FQDN(name="delete.com"))
    emit.delete_entity(r.id)


def test_update_entity(emit):
    r = emit.create_entity(FQDN(name="update.org"))
    emit.update_entity(r.id, FQDN(name="updated.update.org"))


def test_create_edge(emit):
    r1 = emit.create_entity(FQDN(name="wikipedia.org"))
    r2 = emit.create_entity(IPAddress(address="10.0.0.1", type="IPv4"))

    emit.create_edge(
        BasicDNSRelation("dns_record", RRHeader(16)),
        r1.id,
        r2.id)


def test_delete_edge(emit):
    r1 = emit.create_entity(FQDN(name="test01.org"))
    r2 = emit.create_entity(IPAddress(address="10.1.1.1", type="IPv4"))

    e = emit.create_edge(
        BasicDNSRelation("dns_record", RRHeader(16)),
        r1.id,
        r2.id)

    emit.delete_edge(e.id)


def test_update_edge(emit):
    r1 = emit.create_entity(FQDN(name="test02.org"))
    r2 = emit.create_entity(IPAddress(address="10.1.1.2", type="IPv4"))

    e = emit.create_edge(
        BasicDNSRelation("dns_record", RRHeader(16)),
        r1.id,
        r2.id)

    emit.update_edge(
        e.id,
        BasicDNSRelation("dns_record", RRHeader(1)),
        r1.id,
        r2.id)


def test_create_entity_tag(emit):
    r = emit.create_entity(FQDN(name="wikipedia.org"))
    emit.create_entity_tag(
        SourceProperty("create", 100),
        r.id)


def test_delete_entity_tag(emit):
    r = emit.create_entity(FQDN(name="test03.org"))
    t = emit.create_entity_tag(
        SourceProperty("delete", 100),
        r.id)

    emit.delete_entity_tag(t.id)


def test_update_entity_tag(emit):
    r = emit.create_entity(FQDN(name="test04.org"))
    t = emit.create_entity_tag(
        SourceProperty("test", 100),
        r.id)

    emit.update_entity_tag(t.id, SourceProperty("update", 100), r.id)


def test_create_edge_tag(emit):
    r1 = emit.create_entity(asset=FQDN(name="tesla.com"))
    r2 = emit.create_entity(asset=IPAddress(address="192.168.1.1", type="IPv4"))

    e = emit.create_edge(
        relation=BasicDNSRelation("dns_record", RRHeader(16)),
        from_entity=r1.id,
        to_entity=r2.id)

    emit.create_edge_tag(
        property=SourceProperty("create", 100),
        edge=e.id)


def test_delete_edge_tag(emit):
    r1 = emit.create_entity(asset=FQDN(name="delete_edge_tag.com"))
    r2 = emit.create_entity(asset=IPAddress(address="21.10.22.36", type="IPv4"))

    e = emit.create_edge(
        relation=BasicDNSRelation("dns_record", RRHeader(16)),
        from_entity=r1.id,
        to_entity=r2.id,
    )

    t = emit.create_edge_tag(property=SourceProperty("delete", 100), edge=e.id)

    emit.delete_edge_tag(t.id)


def test_update_edge_tag(emit):
    r1 = emit.create_entity(asset=FQDN(name="update_edge_tag.com"))
    r2 = emit.create_entity(asset=IPAddress(address="21.10.22.37", type="IPv4"))

    e = emit.create_edge(
        relation=BasicDNSRelation("dns_record", RRHeader(16)),
        from_entity=r1.id,
        to_entity=r2.id)

    t = emit.create_edge_tag(
        property=SourceProperty("test", 100),
        edge=e.id)

    emit.update_edge_tag(t.id, SourceProperty("update", 100), edge=e.id)
