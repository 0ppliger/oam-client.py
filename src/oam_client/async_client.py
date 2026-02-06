import httpx
import asyncio
from typing import Callable, Awaitable
from httpx_sse import aconnect_sse
from asset_model import Asset, Relation, Property
from .messages import (
    Event,
    Entity,
    Edge,
    EdgeTag,
    EntityTag,
)
from .base import BrokerClientBase

AsyncHandlerFunction = Callable[[Event], Awaitable[None]]


class AsyncBrokerClient(BrokerClientBase):
    async def __send(
            self,
            method: str,
            path: str,
            payload: str
    ) -> str:
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

            return response.text

    async def __listen(
            self,
            method: str,
            path: str,
            handler: AsyncHandlerFunction
    ):
        tasks = []
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
                            tasks.append(
                                asyncio.create_task(
                                    handler(Event.from_sse(sse))))

            except (httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.HTTPStatusError):
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                await asyncio.gather(*tasks)
                raise e

        await asyncio.gather(*tasks)

    async def listen_events(
            self,
            handler: AsyncHandlerFunction
    ):
        await self.__listen("GET", "/listen", handler)

    async def create_entity(
            self,
            asset: Asset
    ) -> Entity:
        entity = Entity(asset.asset_type, asset)
        return Entity.from_json(
            await self.__send("post", "/emit/entity", entity.to_json()))

    async def update_entity(
            self,
            id: str,
            asset: Asset
    ) -> Entity:
        entity = Entity(asset.asset_type, asset)
        return Entity.from_json(
            await self.__send("put", f"/emit/entity/{id}", entity.to_json()))

    async def delete_entity(
            self,
            id: str
    ) -> Entity:
        return Entity.from_json(
            await self.__send("delete", f"/emit/entity/{id}", ""))

    async def create_edge(
            self,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> Edge:
        edge = Edge(
            relation.relation_type, relation,
            from_entity, to_entity)
        return Edge.from_json(
            await self.__send("post", "/emit/edge", edge.to_json()))

    async def update_edge(
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
            await self.__send("put", f"/emit/edge/{id}", edge.to_json()))

    async def delete_edge(
            self,
            id: str
    ) -> Edge:
        return Edge.from_json(
            await self.__send("delete", f"/emit/edge/{id}", ""))

    async def create_entity_tag(
            self,
            property: Property,
            entity: str,
    ) -> EntityTag:
        entity_tag = EntityTag(
            property.property_type, property, entity)
        return EntityTag.from_json(
            await self.__send(
                "post", "/emit/entity_tag", entity_tag.to_json()))

    async def update_entity_tag(
            self,
            id: str,
            property: Property,
            entity: str,
    ) -> EntityTag:
        entity_tag = EntityTag(
            property.property_type, property, entity)
        return EntityTag.from_json(
            await self.__send(
                "put", f"/emit/entity_tag/{id}", entity_tag.to_json()))

    async def delete_entity_tag(
            self,
            id: str
    ) -> EntityTag:
        return EntityTag.from_json(
            await self.__send("delete", f"/emit/entity_tag/{id}", ""))

    async def create_edge_tag(
            self,
            property: Property,
            edge: str
    ) -> EdgeTag:
        edge_tag = EdgeTag(
            property.property_type, property, edge)
        return EdgeTag.from_json(
            await self.__send("post", "/emit/edge_tag", edge_tag.to_json()))

    async def update_edge_tag(
            self,
            id: str,
            property: Property,
            edge: str
    ) -> EdgeTag:
        edge_tag = EdgeTag(
            property.property_type, property, edge)
        return EdgeTag.from_json(
            await self.__send(
                "put", f"/emit/edge_tag/{id}", edge_tag.to_json()))

    async def delete_edge_tag(
            self,
            id: str
    ) -> EdgeTag:
        return EdgeTag.from_json(
            await self.__send("delete", f"/emit/entity_tag/{id}", ""))
