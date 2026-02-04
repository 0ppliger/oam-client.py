import httpx
import asyncio
from httpx_sse import aconnect_sse, ServerSentEvent
from asset_model import Asset, Relation, Property
from .messages import (
    ServerResponse,
    EntityRequest,
    EdgeRequest,
    EdgeTagRequest,
    EntityTagRequest,
)
from typing import Callable
from .base import BrokerClientBase


class AsyncBrokerClient(BrokerClientBase):
    async def __send(
            self,
            method: str,
            path: str,
            payload: str
    ) -> ServerResponse:
        async with httpx.AsyncClient(
                http2=True,
                verify=self.ssl_context
        ) as client:
            response = await client.request(
                method=method.upper(),
                url=self.url + path,
                headers={
                    "Content-Type": "application/json; charset=utf-8"
                },
                content=payload.encode("utf-8"),
            )

            payload = response.json()

            return ServerResponse(
                payload["subject"],
                payload["action"]
            )

    async def __listen(
            self,
            method: str,
            path: str,
            callback: Callable[[ServerSentEvent], None]
    ):
        while True:
            try:
                async with httpx.AsyncClient(
                        http2=True,
                        verify=self.ssl_context,
                        timeout=httpx.Timeout(None, connect=10.0)
                ) as client:
                    async with aconnect_sse(
                            client=client,
                            method=method.upper(),
                            url=self.url + path
                    ) as event_source:
                        async for sse in event_source.aiter_sse():
                            callback(sse)

            except (httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.HTTPStatusError):
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                raise e

    async def listen_events(self):
        def print_event(sse: ServerSentEvent):
            print(
                sse.event,
                sse.data,
                sse.id,
                sse.retry)
        await self.__listen("GET", "/listen", print_event)

    async def create_entity(
            self,
            asset: Asset
    ) -> ServerResponse:
        entity = EntityRequest(asset.asset_type, asset)
        return await self.__send("post", "/emit/entity", entity.to_json())

    async def update_entity(
            self,
            id: str,
            asset: Asset
    ) -> ServerResponse:
        entity = EntityRequest(asset.asset_type, asset)
        return await self.__send("put", f"/emit/entity/{id}", entity.to_json())

    async def delete_entity(
            self,
            id: str
    ) -> ServerResponse:
        return await self.__send("delete", f"/emit/entity/{id}", "")

    async def create_edge(
            self,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> ServerResponse:
        edge = EdgeRequest(
            relation.relation_type, relation,
            from_entity, to_entity)
        return await self.__send("post", "/emit/edge", edge.to_json())

    async def update_edge(
            self,
            id: str,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> ServerResponse:
        edge = EdgeRequest(
            relation.relation_type, relation,
            from_entity, to_entity)
        return await self.__send("put", f"/emit/edge/{id}", edge.to_json())

    async def delete_edge(
            self,
            id: str
    ) -> ServerResponse:
        return await self.__send("delete", f"/emit/edge/{id}", "")

    async def create_entity_tag(
            self,
            property: Property,
            entity: str,
    ) -> ServerResponse:
        entity_tag = EntityTagRequest(
            property.property_type, property, entity)
        return await self.__send(
            "post", "/emit/entity_tag", entity_tag.to_json())

    async def update_entity_tag(
            self,
            id: str,
            property: Property,
            entity: str,
    ) -> ServerResponse:
        entity_tag = EntityTagRequest(
            property.property_type, property, entity)
        return await self.__send(
            "put", f"/emit/entity_tag/{id}", entity_tag.to_json())

    async def delete_entity_tag(
            self,
            id: str
    ) -> ServerResponse:
        return await self.__send("delete", f"/emit/entity_tag/{id}", "")

    async def create_edge_tag(
            self,
            property: Property,
            edge: str
    ) -> ServerResponse:
        edge_tag = EdgeTagRequest(
            property.property_type, property, edge)
        return await self.__send("post", "/emit/edge_tag", edge_tag.to_json())

    async def update_edge_tag(
            self,
            id: str,
            property: Property,
            edge: str
    ) -> ServerResponse:
        edge_tag = EdgeTagRequest(
            property.property_type, property, edge)
        return await self.__send(
            "put", f"/emit/edge_tag/{id}", edge_tag.to_json())

    async def delete_edge_tag(
            self,
            id: str
    ) -> ServerResponse:
        return await self.__send("delete", f"/emit/entity_tag/{id}", "")
