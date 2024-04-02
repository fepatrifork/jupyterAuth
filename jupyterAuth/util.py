import os
import time
from requests.auth import AuthBase
import requests
import logging
import sys
from typing import Optional

def verify_connection(host_url: str, auth: Optional[AuthBase] = None):
    """Checks the connection to OpenSearch"""
    url = f"{host_url}/_plugins/_security/api/account"
    try:
        resp = requests.get(url, verify=False, timeout=5, auth=auth)
    except requests.exceptions.RequestException as err:
        logging.error(f"Connection to OpenSearch could not be established: {err}")
        return False

    if not resp.ok:
        logging.error(f"Error connecting to OpenSearch: {resp.text}")

    return resp.ok

class BearerAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token['access_token']}"
        return r

    def isExpired(self) -> bool:
        if self.token["expires_at"]:
            # 10 seconds buffer
            return time.time() > float(self.token["expires_at"]) - 10
        return False