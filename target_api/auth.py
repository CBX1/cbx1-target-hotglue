import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

import backoff
import requests

from target_api.constants import ACCESS_KEY, CODE_KEY


class Cbx1Authenticator:
    """API Authenticator for JWT flows."""

    def __init__(self, target, state) -> None:
        self._config: Dict[str, Any] = target._config
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = os.getenv("BASE_URL", default="https://qa-api.cbx1.app/") + "api/g/v1/auth/token/generate",
        self._target = target
        self.state = state
        self.config_file = target.config_file

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {"Authorization": f"Bearer {self._config.get(ACCESS_KEY)}"}
        return result

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
            "authenticationType": "ACCESS_KEY",
            "code": self._config.get(CODE_KEY),
        }

    def is_token_valid(self) -> bool:
        access_token = self._config.get(ACCESS_KEY)
        now = round(datetime.utcnow().timestamp())
        expires_in = self._config.get("expires_in")
        if expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False
        if not expires_in:
            return False
        return not ((expires_in - now) < 120)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        try:
            token_response = requests.get(
                self._auth_endpoint, params=self.oauth_request_body
            )
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.text})
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.text()}'. {ex}"
            )

        token_json = token_response.json().get("data", {})

        self.access_token = token_json.get("sessionToken")
        self._config[ACCESS_KEY] = token_json["sessionToken"]
        self._config["refresh_token"] = token_json["refreshToken"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = now + token_json["maxAge"]

        with open(self._target.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)
