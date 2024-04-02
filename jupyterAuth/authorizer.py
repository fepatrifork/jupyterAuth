import os
import time
import logging
import prometheus_client
import requests
import urllib3
from requests.auth import AuthBase, HTTPBasicAuth
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import jupyterAuth.util
from jupyterAuth.util import BearerAuth

ALLOWED_SCHEMES = ("http", "https")

class Authorizer:

    def __init__(self, host, prom_port, http_url_service, scope, disable_https):
        self.host= host
        self.diff_mode = None
        self.collect_mode = None
        self.service_mode = None
        self.no_name_validation = None
        self.prometheus_port = str(prom_port)
        self.http_url_service = http_url_service
        self.os_scope = scope
        if disable_https:
            urllib3.disable_warnings()
            os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'      

    def getOauth2(self, user, password):
        os_tokenendpoint = os.environ.get("OS_TOKENENDPOINT", self.http_url_service)
        os_scope = os.environ.get("OS_SCOPE", self.os_scope)
        os_user = os.environ.get("OS_USER", user)
        os_pass = os.environ.get("OS_PASS", password)

        if os_tokenendpoint:
            # Create an OAuth2 client with Client Credentials Grant
            backendClient = BackendApplicationClient(client_id=os_user)
            oauth = OAuth2Session(client=backendClient)

            # Fetch the token (if not cached or expired) and apply it to the session
            token = oauth.fetch_token(
                token_url=os_tokenendpoint,
                auth=HTTPBasicAuth(os_user, os_pass),
                scope=os_scope,
                verify= False,
            )
            auth = BearerAuth(token)
            print ("Connection to OpenSearch established")
            print (f"Using OAuth2 flow as {os_user}")

        elif os_user and os_pass:
            auth = HTTPBasicAuth(os_user, os_pass)
            print ("Connection to OpenSearch established")
            print (f"Using HTTP basic authentication as {os_user}")
        else:
            auth = None
            print ("Connection to OpenSearch established")
            logging.info("Using anonymous authentication")

        scheme = os.environ.get("SCHEME", "http")
        if scheme not in ALLOWED_SCHEMES:
            print (f"Scheme {scheme} not understood. Allowed schemes: {ALLOWED_SCHEMES}")

        opensearch_endpoint = f"{scheme}://{self.host}"

        if not util.verify_connection(opensearch_endpoint, auth):
            print (f"Authentication not valid")

        return auth

    def getJWToken(self, user, password):
        os_tokenendpoint = os.environ.get("OS_TOKENENDPOINT", self.http_url_service)
        os_scope = os.environ.get("OS_SCOPE", self.os_scope)
        os_user = os.environ.get("OS_USER", user)
        os_pass = os.environ.get("OS_PASS", password)

        if os_tokenendpoint:
            # Create an OAuth2 client with Client Credentials Grant
            backendClient = BackendApplicationClient(client_id=os_user)
            oauth = OAuth2Session(client=backendClient)

            # Fetch the token (if not cached or expired) and apply it to the session
            token = oauth.fetch_token(
                token_url=os_tokenendpoint,
                auth=HTTPBasicAuth(os_user, os_pass),
                scope=os_scope,
                verify= False,
            )
            auth = BearerAuth(token)
            print ("Connection to OpenSearch established")
            print (f"Using OAuth2 flow as {os_user}")

        scheme = os.environ.get("SCHEME", "http")
        if scheme not in ALLOWED_SCHEMES:
            print (f"Scheme {scheme} not understood. Allowed schemes: {ALLOWED_SCHEMES}")

        opensearch_endpoint = f"{scheme}://{self.host}"

        if not util.verify_connection(opensearch_endpoint, auth):
            print (f"Authentication not valid")
            
        return token