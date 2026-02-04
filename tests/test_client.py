import pytest
from asset_model import FQDN, IPAddress, BasicDNSRelation, SourceProperty
from oam_client import EmitterClient
from urllib.error import HTTPError

@pytest.fixture
def emit():
    return EmitterClient(
        "https://localhost:443",
        keylog_filename="sslkeylog.txt",
        verify=False)

@pytest.mark.asyncio
async def test_create_entity(emit):
    await emit.createEntity(FQDN(name="create0.org"))

@pytest.mark.asyncio
async def test_delete_entity(emit):
    r = await emit.createEntity(FQDN(name="delete.com"))
    await emit.deleteEntity(r.subject)

@pytest.mark.asyncio
async def test_update_entity(emit):
    r = await emit.createEntity(FQDN(name="update.org"))
    await emit.updateEntity(r.subject, FQDN(name="updated.update.org"))

@pytest.mark.asyncio
async def test_create_edge(emit):
    r1 = await emit.createEntity(FQDN(name="wikipedia.org"))
    r2 = await emit.createEntity(IPAddress(address="10.0.0.1", type="IPv4"))

    await emit.createEdge(
        BasicDNSRelation("dns_record", 16, "TXT"),
        r1.subject,
        r2.subject)

@pytest.mark.asyncio
async def test_delete_edge(emit):
    r1 = await emit.createEntity(FQDN(name="test01.org"))
    r2 = await emit.createEntity(IPAddress(address="10.1.1.1", type="IPv4"))

    e = await emit.createEdge(
        BasicDNSRelation("dns_record", 16, "TXT"),
        r1.subject,
        r2.subject)

    await emit.deleteEdge(e.subject)

@pytest.mark.asyncio
async def test_update_edge(emit):
    r1 = await emit.createEntity(FQDN(name="test02.org"))
    r2 = await emit.createEntity(IPAddress(address="10.1.1.2", type="IPv4"))

    e = await emit.createEdge(
        BasicDNSRelation("dns_record", 16, "TXT"),
        r1.subject,
        r2.subject)

    await emit.updateEdge(
        e.subject,
        BasicDNSRelation("dns_record", 1, "A"),
        r1.subject,
        r2.subject)

@pytest.mark.asyncio
async def test_create_entity_tag(emit):
    r = await emit.createEntity(FQDN(name="wikipedia.org"))
    await emit.createEntityTag(
        SourceProperty("create", 100),
        r.subject)

@pytest.mark.asyncio
async def test_delete_entity_tag(emit):
    r = await emit.createEntity(FQDN(name="test03.org"))
    t = await emit.createEntityTag(
        SourceProperty("delete", 100),
        r.subject)

    await emit.deleteEntityTag(t.subject)

@pytest.mark.asyncio
async def test_update_entity_tag(emit):
    r = await emit.createEntity(FQDN(name="test04.org"))
    t = await emit.createEntityTag(
        SourceProperty("test", 100),
        r.subject)

    await emit.updateEntityTag(t.subject, SourceProperty("update", 100), r.subject)

@pytest.mark.asyncio
async def test_create_edge_tag(emit):
    r1 = await emit.createEntity(asset=FQDN(name="tesla.com"))
    r2 = await emit.createEntity(asset=IPAddress(address="192.168.1.1", type="IPv4"))

    e = await emit.createEdge(
        relation=BasicDNSRelation("dns_record", 16, "TXT"),
        from_entity=r1.subject,
        to_entity=r2.subject)

    await emit.createEdgeTag(
        property=SourceProperty("create", 100),
        edge=e.subject)

@pytest.mark.asyncio
async def test_delete_edge_tag(emit):
    r1 = await emit.createEntity(asset=FQDN(name="delete_edge_tag.com"))
    r2 = await emit.createEntity(asset=IPAddress(address="21.10.22.36", type="IPv4"))

    e = await emit.createEdge(
        relation=BasicDNSRelation("dns_record", 16, "TXT"),
        from_entity=r1.subject,
        to_entity=r2.subject,
    )

    t = await emit.createEdgeTag(property=SourceProperty("delete", 100), edge=e.subject)

    await emit.deleteEdgeTag(t.subject)


@pytest.mark.asyncio
async def test_update_edge_tag(emit):
    r1 = await emit.createEntity(asset=FQDN(name="update_edge_tag.com"))
    r2 = await emit.createEntity(asset=IPAddress(address="21.10.22.37", type="IPv4"))

    e = await emit.createEdge(
        relation=BasicDNSRelation("dns_record", 16, "TXT"),
        from_entity=r1.subject,
        to_entity=r2.subject)

    t = await emit.createEdgeTag(
        property=SourceProperty("test", 100),
        edge=e.subject)

    await emit.updateEdgeTag(t.subject, SourceProperty("update", 100), edge=e.subject)
