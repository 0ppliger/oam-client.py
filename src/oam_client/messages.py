import json
import os
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from asset_model import (
    OAMObject,
    Asset,
    AssetType,
    Relation,
    RelationType,
    Property,
    PropertyType,
    get_asset_by_type
)

LOGLEVEL = os.getenv("LOGLEVEL", "WARNING").upper()

logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.WARNING))

logger = logging.getLogger(__name__)


class ServerAction(str, Enum):
    Deleted = "deleted"
    Upserted = "upserted"
    Updated = "updated"


@dataclass
class Entity:
    type: AssetType
    asset: Asset
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "type": self.type.value,
            "asset": self.asset.to_dict(),
        })

    @staticmethod
    def from_json(json_data: str) -> "Entity":
        logger.debug(json_data)
        data = json.loads(json_data)
        asset_type = AssetType(data["type"])
        return Entity(
            id=data["id"],
            created_at=data["created_at"],
            last_seen=data["last_seen"],
            type=asset_type,
            asset=OAMObject.from_dict(
                get_asset_by_type(asset_type), data["asset"])
        )


@dataclass
class Edge:
    type: RelationType
    relation: Relation
    from_entity: str
    to_entity: str
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "type": self.type.value,
            "relation": self.relation.to_dict(),
            "from_entity": self.from_entity,
            "to_entity": self.to_entity
        })

    @staticmethod
    def from_json(json_data: str) -> "Edge":
        logger.debug(json_data)
        data = json.loads(json_data)
        rel_type = RelationType(data["type"])
        return Edge(
            id=data["id"],
            created_at=data["created_at"],
            last_seen=data["last_seen"],
            type=rel_type,
            relation=OAMObject.from_dict(
                get_asset_by_type(rel_type), data["relation"]),
            from_entity=data["from_entity"],
            to_entity=data["to_entity"],
        )


@dataclass
class EntityTag:
    type: PropertyType
    property: Property
    entity: str
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "type": self.type.value,
            "property": self.property.to_dict(),
            "entity": self.entity,
        })

    @staticmethod
    def from_json(json_data: str) -> "EntityTag":
        logger.debug(json_data)
        data = json.loads(json_data)
        prop_type = PropertyType(data["type"])
        return EntityTag(
            id=data["id"],
            created_at=data["created_at"],
            last_seen=data["last_seen"],
            type=prop_type,
            property=OAMObject.from_dict(
                get_asset_by_type(prop_type), data["property"]),
            entity=data["entity"]
        )


@dataclass
class EdgeTag:
    type: PropertyType
    property: Property
    edge: str
    id: Optional[str] = None
    created_at: Optional[datetime] = None
    last_seen: Optional[datetime] = None

    def to_json(self) -> str:
        return json.dumps({
            "id": self.id,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
            "type": self.type.value,
            "property": self.property.to_dict(),
            "edge": self.edge,
        })

    @staticmethod
    def from_json(json_data: str) -> "EdgeTag":
        logger.debug(json_data)
        data = json.loads(json_data)
        prop_type = PropertyType(data["type"])
        return EdgeTag(
            id=data["id"],
            created_at=data["created_at"],
            last_seen=data["last_seen"],
            type=prop_type,
            property=OAMObject.from_dict(
                get_asset_by_type(prop_type), data["property"]),
            edge=data["edge"]
        )


@dataclass
class EntityCreatedEvent:
    id: str
    type: AssetType
    asset: Asset
