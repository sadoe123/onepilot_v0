import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.base_connector import BaseConnector, ConnectorMetadata, Entity, Field, Relation
from core.plugin_manager  import PluginManager
from core.auth_manager    import AuthManager
from connectors.rest_connector import json_to_entity, infer_type


class MockConnector(BaseConnector):
    def connect(self):                  self._connected = True; return True
    def test_connection(self):          return {"success": True, "message": "OK", "latency_ms": 0}
    def get_metadata(self):             return ConnectorMetadata("mock", "mock", [])
    def execute_query(self, q, p=None): return [{"id": 1}]


class TestPluginManager:
    def setup_method(self): self.pm = PluginManager()

    def test_register_and_get(self):
        self.pm.register("mock", MockConnector)
        assert self.pm.get("mock") == MockConnector

    def test_invalid_class(self):
        with pytest.raises(TypeError): self.pm.register("bad", object)

    def test_unknown_key(self):
        with pytest.raises(KeyError): self.pm.get("nope")

    def test_disable_enable(self):
        self.pm.register("mock", MockConnector)
        self.pm.disable("mock")
        assert not self.pm.is_enabled("mock")
        self.pm.enable("mock")
        assert self.pm.is_enabled("mock")

    def test_create_instance(self):
        self.pm.register("mock", MockConnector)
        inst = self.pm.create("mock", {}, "test_id")
        assert isinstance(inst, MockConnector)
        assert "test_id" in self.pm.list_active()

    def test_remove_instance(self):
        self.pm.register("mock", MockConnector)
        self.pm.create("mock", {}, "to_remove")
        self.pm.remove_instance("to_remove")
        assert "to_remove" not in self.pm.list_active()


class TestAuthManager:
    def setup_method(self): self.auth = AuthManager()

    def test_no_auth(self):
        assert self.auth.get_headers({"type": "none"}) == {}

    def test_basic(self):
        h = self.auth.get_headers({"type": "basic", "username": "u", "password": "p"})
        assert h["Authorization"].startswith("Basic ")

    def test_bearer(self):
        h = self.auth.get_headers({"type": "bearer", "token": "tok"})
        assert h["Authorization"] == "Bearer tok"

    def test_api_key(self):
        h = self.auth.get_headers({"type": "api_key", "header": "X-Key", "value": "abc"})
        assert h["X-Key"] == "abc"


class TestRESTHelpers:
    def test_infer_types(self):
        assert infer_type(42)         == "integer"
        assert infer_type(3.14)       == "float"
        assert infer_type(True)       == "boolean"
        assert infer_type("hello")    == "string"
        assert infer_type("2024-01-15") == "date"

    def test_entity_from_dict(self):
        e = json_to_entity("c", {"id": 1, "name": "A", "active": True})
        assert e and len(e.fields) == 3

    def test_entity_from_list(self):
        e = json_to_entity("u", [{"id": 1}, {"id": 2}])
        assert e and len(e.fields) == 1

    def test_empty_returns_none(self):
        assert json_to_entity("x", []) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])