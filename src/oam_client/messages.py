import json
from dataclasses import dataclass
from enum import Enum
from asset_model import (
    Asset,
    AssetType,
    Relation,
    RelationType,
    Property,
    PropertyType
)


class ServerAction(str, Enum):
    Deleted = "deleted"
    Upserted = "upserted"


@dataclass
class ServerResponse:
    subject: str
    action: ServerAction


@dataclass
class EntityRequest:
    type: AssetType
    asset: Asset

    def to_json(self):
        return json.dumps({
            "type": self.type.value,
            "asset": self.asset.to_dict(),
        })


@dataclass
class EdgeRequest:
    type: RelationType
    relation: Relation
    from_entity: str
    to_entity: str

    def to_json(self):
        return json.dumps({
            "type": self.type.value,
            "relation": self.relation.to_dict(),
            "from": self.from_entity,
            "to": self.to_entity
        })


@dataclass
class EntityTagRequest:
    type: PropertyType
    property: Property
    entity: str

    def to_json(self):
        return json.dumps({
            "type": self.type.value,
            "property": self.property.to_dict(),
            "entity": self.entity,
        })


@dataclass
class EdgeTagRequest:
    type: PropertyType
    property: Property
    edge: str

    def to_json(self):
        return json.dumps({
            "type": self.type.value,
            "property": self.property.to_dict(),
            "edge": self.edge,
        })


@dataclass
class EntityCreatedEvent:
    id: str
    type: AssetType
    asset: Asset
