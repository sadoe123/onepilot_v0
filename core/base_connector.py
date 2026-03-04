from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class Field:
    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    description: str = ""

@dataclass
class Relation:
    source_entity: str
    target_entity: str
    source_field: str
    target_field: str
    relation_type: str = "many_to_one"

@dataclass
class Entity:
    name: str
    fields: List[Field] = field(default_factory=list)
    relations: List[Relation] = field(default_factory=list)
    source_type: str = ""
    description: str = ""

@dataclass
class ConnectorMetadata:
    connector_id: str
    connector_type: str
    entities: List[Entity] = field(default_factory=list)


class BaseConnector(ABC):
    """Interface commune a tous les connecteurs OnePilot."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def test_connection(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_metadata(self) -> ConnectorMetadata:
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        pass

    def disconnect(self):
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected