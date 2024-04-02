import os
import time
from requests.auth import AuthBase
import requests
import logging
import sys
from typing import Optional


def get_files(directory, extension):
    if not os.path.isdir(directory):
        return []
    return [file for file in os.listdir(directory) if file.endswith(extension)]    


def collect(collect_mode: bool, **kwargs):
    """Collection helper function"""
    if collect_mode:
        return collector.collect(**kwargs)
    return True

def initialize_service_loggers():
    """Initialize loggers for when running as a server"""
    log_format = "%(asctime)s.%(msecs)03dZ [%(threadName)s] %(levelname)s %(module)s - %(message)s"
    log_date = "%Y-%m-%dT%H:%M:%S"

    formatter = logging.Formatter(log_format, datefmt=log_date)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # console
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stdout.setFormatter(formatter)

    root_logger.addHandler(handler_stdout)

def initialize_cli_loggers():
    """Initialize loggers for when running as a CLI"""
    log_format = "%(levelname)s - %(message)s"

    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # console
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stdout.setFormatter(formatter)

    root_logger.addHandler(handler_stdout)

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
