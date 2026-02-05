import httpx
from httpx_sse import connect_sse, ServerSentEvent
from asset_model import Asset, Relation, Property
from .messages import (
    Entity,
    Edge,
    EdgeTag,
    EntityTag,
)
from typing import Callable
from .base import BrokerClientBase


class BrokerClient(BrokerClientBase):
    def __send(
            self,
            method: str,
            path: str,
            payload: str
    ) -> str:
        with httpx.Client(
                http2=True,
                verify=self.ssl_context
        ) as client:
            response = client.request(
                method=method.upper(),
                url=self.url + path,
                headers={
                    "Content-Type": "application/json; charset=utf-8"
                },
                content=payload.encode("utf-8"),
            )

            return response.text

    def __listen(
            self,
            method: str,
            path: str,
            callback: Callable[[ServerSentEvent], None]
    ):
        while True:
            try:
                with httpx.Client(
                        http2=True,
                        verify=self.ssl_context,
                        timeout=httpx.Timeout(None, connect=10.0)
                ) as client:
                    with connect_sse(
                            client=client,
                            method=method.upper(),
                            url=self.url + path
                    ) as event_source:
                        for sse in event_source.iter_sse():
                            callback(sse)

            except (httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.HTTPStatusError):
                continue
            except Exception as e:
                raise e

    def listen_events(self):
        def print_event(sse: ServerSentEvent):
            print(
                sse.event,
                sse.data,
                sse.id,
                sse.retry)
        self.__listen("GET", "/listen", print_event)

    def create_entity(
            self,
            asset: Asset
    ) -> Entity:
        entity = Entity(asset.asset_type, asset)
        return Entity.from_json(
            self.__send("post", "/emit/entity", entity.to_json()))

    def update_entity(
            self,
            id: str,
            asset: Asset
    ) -> Entity:
        entity = Entity(asset.asset_type, asset)
        return Entity.from_json(
            self.__send("put", f"/emit/entity/{id}", entity.to_json()))

    def delete_entity(
            self,
            id: str
    ) -> Entity:
        return Entity.from_json(
            self.__send("delete", f"/emit/entity/{id}", ""))

    def create_edge(
            self,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> Edge:
        edge = Edge(
            relation.relation_type, relation,
            from_entity, to_entity)
        return Edge.from_json(
            self.__send("post", "/emit/edge", edge.to_json()))

    def update_edge(
            self,
            id: str,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> Edge:
        edge = Edge(
            relation.relation_type, relation,
            from_entity, to_entity)
        return Edge.from_json(
            self.__send("put", f"/emit/edge/{id}", edge.to_json()))

    def delete_edge(
            self,
            id: str
    ) -> Edge:
        return Edge.from_json(
            self.__send("delete", f"/emit/edge/{id}", ""))

    def create_entity_tag(
            self,
            property: Property,
            entity: str,
    ) -> EntityTag:
        entity_tag = EntityTag(
            property.property_type, property, entity)
        return EntityTag.from_json(
            self.__send(
                "post", "/emit/entity_tag", entity_tag.to_json()))

    def update_entity_tag(
            self,
            id: str,
            property: Property,
            entity: str,
    ) -> EntityTag:
        entity_tag = EntityTag(
            property.property_type, property, entity)
        return EntityTag.from_json(
            self.__send(
                "put", f"/emit/entity_tag/{id}", entity_tag.to_json()))

    def delete_entity_tag(
            self,
            id: str
    ) -> EntityTag:
        return EntityTag.from_json(
            self.__send("delete", f"/emit/entity_tag/{id}", ""))

    def create_edge_tag(
            self,
            property: Property,
            edge: str
    ) -> EdgeTag:
        edge_tag = EdgeTag(
            property.property_type, property, edge)
        return EdgeTag.from_json(
            self.__send("post", "/emit/edge_tag", edge_tag.to_json()))

    def update_edge_tag(
            self,
            id: str,
            property: Property,
            edge: str
    ) -> EdgeTag:
        edge_tag = EdgeTag(
            property.property_type, property, edge)
        return EdgeTag.from_json(
            self.__send(
                "put", f"/emit/edge_tag/{id}", edge_tag.to_json()))

    def delete_edge_tag(
            self,
            id: str
    ) -> EdgeTag:
        return EdgeTag.from_json(
            self.__send("delete", f"/emit/entity_tag/{id}", ""))
