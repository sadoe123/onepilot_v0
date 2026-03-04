import importlib
import logging
from typing import Dict, Type, Optional, List, Any
from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class PluginManager:

    def __init__(self):
        self._registry: Dict[str, Type[BaseConnector]] = {}
        self._active_connectors: Dict[str, BaseConnector] = {}
        self._disabled: set = set()

    def register(self, name: str, connector_class: Type[BaseConnector]):
        if not issubclass(connector_class, BaseConnector):
            raise TypeError(f"{connector_class} doit heriter de BaseConnector")
        self._registry[name] = connector_class
        logger.info(f"[PluginManager] Connecteur '{name}' enregistre.")

    def unregister(self, name: str):
        if name in self._registry:
            del self._registry[name]

    def register_from_module(self, module_path: str, class_name: str, plugin_name: str):
        try:
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            self.register(plugin_name, cls)
        except (ImportError, AttributeError) as e:
            logger.error(f"[PluginManager] Impossible de charger {module_path}.{class_name}: {e}")
            raise

    def enable(self, name: str):
        self._disabled.discard(name)

    def disable(self, name: str):
        self._disabled.add(name)

    def is_enabled(self, name: str) -> bool:
        return name in self._registry and name not in self._disabled

    def get(self, name: str) -> Type[BaseConnector]:
        if name not in self._registry:
            raise KeyError(f"Connecteur '{name}' non enregistre. Disponibles: {self.list_registered()}")
        if name in self._disabled:
            raise RuntimeError(f"Connecteur '{name}' est desactive.")
        return self._registry[name]

    def create(self, name: str, config: Dict[str, Any], instance_id: Optional[str] = None) -> BaseConnector:
        cls = self.get(name)
        instance = cls(config)
        key = instance_id or f"{name}_{id(instance)}"
        self._active_connectors[key] = instance
        logger.info(f"[PluginManager] Instance '{key}' creee.")
        return instance

    def get_instance(self, instance_id: str) -> Optional[BaseConnector]:
        return self._active_connectors.get(instance_id)

    def remove_instance(self, instance_id: str):
        if instance_id in self._active_connectors:
            self._active_connectors[instance_id].disconnect()
            del self._active_connectors[instance_id]

    def list_registered(self) -> List[str]:
        return list(self._registry.keys())

    def list_active(self) -> List[str]:
        return list(self._active_connectors.keys())

    def status(self) -> Dict:
        return {
            "registered": self.list_registered(),
            "disabled": list(self._disabled),
            "active_instances": self.list_active(),
        }


plugin_manager = PluginManager()