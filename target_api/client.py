from __future__ import annotations

import json
import os
from sys import getsizeof

from pydantic import BaseModel
from target_api.auth import Cbx1Authenticator
from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueBaseSink
from target_hotglue.common import HGJSONEncoder
import requests
import urllib3
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import backoff

from target_api.constants import ORG_ID_KEY

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ApiSink(HotglueBaseSink):
    auth_state = {}

    @property
    def name(self):
        return self.stream_name

    @property
    def authenticator(self):
        return Cbx1Authenticator(self._target, self.auth_state)

    @property
    def base_url(self) -> str:
        return os.getenv("BASE_URL")

    @property
    def endpoint(self) -> str:
        return f"api/t/v1/targets/{self.name}/upsert"

    @property
    def bulk_endpoint(self) -> str:
        return f"api/t/v1/targets/{self.name}/bulk"

    def _get_lookup_field(self) -> str:
        """Return the lookup field based on stream name."""
        stream_lower = self.stream_name.lower()
        if "account" in stream_lower:
            return "domain"
        elif "contact" in stream_lower:
            return "email"
        return "id"

    @property
    def unified_schema(self) -> BaseModel:
        return None

    @property
    def custom_headers(self) -> dict:
        custom_headers = {
            "User-Agent": self._config.get("user_agent", "target-api <hello@hotglue.xyz>")
        }
        config_custom_headers = self._config.get("custom_headers") or list()
        for ch in config_custom_headers:
            if not isinstance(ch, dict):
                continue
            name = ch.get("name")
            value = ch.get("value")
            if not isinstance(name, str) or not isinstance(value, str):
                continue
            custom_headers[name] = value

        custom_headers["Authorization"] = f"Bearer {self.authenticator.access_token}"
        custom_headers["x-organisation-id"] = self.config.get(ORG_ID_KEY)
        return custom_headers

    @property
    def is_full(self) -> bool:
        is_full_in_length = super().is_full
        is_full_in_bytes = False

        if self._config.get("max_size_in_bytes") and self._pending_batch:
            max_size_in_bytes = int(self._config.get("max_size_in_bytes"))
            batch_size_in_bytes = getsizeof(json.dumps(self._pending_batch["records"], cls=HGJSONEncoder))
            is_full_in_bytes = batch_size_in_bytes >= max_size_in_bytes * 0.9

        return is_full_in_length or is_full_in_bytes

    def response_error_message(self, response: requests.Response) -> str:
        try:
            response_text = f" with response body: '{response.text}'"
        except:
            response_text = None

        request_url = response.request.url
        if self._config.get("api_key"):
            request_url = request_url.replace(self._config.get("api_key"), "__MASKED__")
        
        return f"Status code: {response.status_code} with {response.reason} for path: {request_url} {response_text}"
    
    def curlify_on_error(self, response):
        command = "curl -X {method} -H {headers} -d '{data}' '{uri}'"
        method = response.request.method
        uri = response.request.url
        data = response.request.body

        if self._config.get("api_key_url"):
            uri = uri.replace(self._config.get("api_key"), "__MASKED__")

        headers = []
        api_key_headers = ["authorization", "x-organisation-id"]

        for k, v in response.request.headers.items():
            # Mask the Authorization header
            if k.lower() in api_key_headers:
                v = "__MASKED__"
            headers.append('"{0}: {1}"'.format(k, v))

        headers = " -H ".join(headers)
        return command.format(method=method, headers=headers, data=data, uri=uri)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.warning(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise RetriableAPIError(error)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.warning(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise FatalAPIError(error)


    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=2,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)

        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data is not None
            else None
        )

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            verify=verify,
            timeout=30
        )
        self.validate_response(response)
        return response
