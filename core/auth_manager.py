import base64
import time
import logging
from typing import Dict, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)


class AuthType(str, Enum):
    NONE    = "none"
    BASIC   = "basic"
    BEARER  = "bearer"
    OAUTH2  = "oauth2"
    API_KEY = "api_key"


class AuthManager:

    def __init__(self):
        self._token_cache: Dict[str, Dict] = {}

    def get_headers(self, auth_config: Dict[str, Any]) -> Dict[str, str]:
        auth_type = AuthType(auth_config.get("type", "none"))

        if auth_type == AuthType.NONE:
            return {}
        elif auth_type == AuthType.BASIC:
            return self._basic_auth_header(auth_config["username"], auth_config["password"])
        elif auth_type == AuthType.BEARER:
            return {"Authorization": f"Bearer {auth_config['token']}"}
        elif auth_type == AuthType.API_KEY:
            key = auth_config.get("header", "X-API-Key")
            return {key: auth_config["value"]}
        elif auth_type == AuthType.OAUTH2:
            token = self._get_oauth2_token(auth_config)
            return {"Authorization": f"Bearer {token}"}
        return {}

    def _basic_auth_header(self, username: str, password: str) -> Dict[str, str]:
        credentials = f"{username}:{password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    def _get_oauth2_token(self, auth_config: Dict) -> str:
        cache_key = auth_config.get("client_id", "")
        if cache_key in self._token_cache:
            cached = self._token_cache[cache_key]
            if time.time() < cached["expires_at"] - 60:
                return cached["access_token"]
        import requests
        response = requests.post(
            auth_config["token_url"],
            data={
                "grant_type":    "client_credentials",
                "client_id":     auth_config["client_id"],
                "client_secret": auth_config["client_secret"],
                **( {"scope": auth_config["scope"]} if "scope" in auth_config else {} )
            },
            timeout=30
        )
        response.raise_for_status()
        token_data = response.json()
        self._token_cache[cache_key] = {
            "access_token": token_data["access_token"],
            "expires_at":   time.time() + token_data.get("expires_in", 3600),
        }
        return token_data["access_token"]

    def clear_cache(self, client_id: Optional[str] = None):
        if client_id:
            self._token_cache.pop(client_id, None)
        else:
            self._token_cache.clear()


auth_manager = AuthManager()