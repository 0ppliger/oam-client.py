from asset_model import Asset, Relation, Property
import json
from urllib import request
from .messages import (
    ServerResponse,
    EntityRequest,
    EdgeRequest,
    EdgeTagRequest,
    EntityTagRequest
)
from typing import Optional

class EmitterClient:
    url: str
    
    def __init__(self, url: str):
        self.url = url

    def __send(self, method: str, path: str, payload: str) -> ServerResponse:
        r = request.Request(
            url=self.url + path,
            method=method.upper(),
            data=payload.encode("utf-8"),
            headers={
                "Content-Type": "application/json; charset=utf-8"
            }
        )
        response = request.urlopen(r)
        payload = response.read().decode(response.headers.get_content_charset())
        payload_data = json.loads(payload)
        return ServerResponse(
            payload_data["subject"],
            payload_data["action"]
        )
       
    def createEntity(
            self,
            asset: Asset
    ) -> ServerResponse:
        entity = EntityRequest(asset.asset_type, asset)
        return self.__send("post", "/emit/entity", entity.to_json())

    def updateEntity(
            self,
            id: str,
            asset: Asset
    ) -> ServerResponse:
        entity = EntityRequest(asset.asset_type, asset)
        return self.__send("put", f"/emit/entity/{id}", entity.to_json())

    def deleteEntity(
            self,
            id: str
    ) -> ServerResponse:
        return self.__send("delete", f"/emit/entity/{id}", "")
    
    def createEdge(
            self,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> ServerResponse:
        edge = EdgeRequest(
            relation.relation_type, relation,
            from_entity, to_entity)
        return self.__send("post", "/emit/edge", edge.to_json())

    def updateEdge(
            self,
            id: str,
            relation: Relation,
            from_entity: str,
            to_entity: str,
    ) -> ServerResponse:
        edge = EdgeRequest(
            relation.relation_type, relation,
            from_entity, to_entity)
        return self.__send("put", f"/emit/edge/{id}", edge.to_json())

    def deleteEdge(
            self,
            id: str
    ) -> ServerResponse:
        return self.__send("delete", f"/emit/edge/{id}", "")
    
    def createEntityTag(
            self,
            property: Property,
            entity: str,
    ) -> ServerResponse:
        entity_tag = EntityTagRequest(
            property.property_type, property, entity)
        return self.__send("post", "/emit/entity_tag", entity_tag.to_json())

    def updateEntityTag(
            self,
            id: str,
            property: Property,
            entity: str,
    ) -> ServerResponse:
        entity_tag = EntityTagRequest(
            property.property_type, property, entity)
        return self.__send("put", f"/emit/entity_tag/{id}", entity_tag.to_json())

    def deleteEntityTag(
            self,
            id: str
    ) -> ServerResponse:
        return self.__send("delete", f"/emit/entity_tag/{id}", "")
    
    def createEdgeTag(
            self,
            property: Property,
            edge: str
    ) -> ServerResponse:        
        edge_tag = EdgeTagRequest(
            property.property_type, property, edge)
        return self.__send("post", "/emit/edge_tag", edge_tag.to_json())

    def updateEdgeTag(
            self,
            id: str,
            property: Property,
            edge: str
    ) -> ServerResponse:        
        edge_tag = EdgeTagRequest(
            property.property_type, property, edge)
        return self.__send("put", f"/emit/edge_tag/{id}", edge_tag.to_json())

    def deleteEdgeTag(
            self,
            id: str
    ) -> ServerResponse:
        return self.__send("delete", f"/emit/entity_tag/{id}", "")
