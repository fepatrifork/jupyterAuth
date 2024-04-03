import unittest
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException
from jupyterAuth.util import verify_OS_connection, BearerAuth

class TestVerifyOSConnection(unittest.TestCase):

    @patch('jupyterAuth.authorizer.requests.get')
    def test_verify_os_connection_success(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.ok = True
        mock_requests_get.return_value = mock_response

        host_url = 'http://example.com'
        auth = BearerAuth({'access_token': 'fake_token', 'expires_at': 1700000000})

        result = verify_OS_connection(host_url, auth)

        self.assertTrue(result)
        mock_requests_get.assert_called_once_with(
            f"{host_url}/_plugins/_security/api/account",
            verify=False,
            timeout=5,
            auth=auth
        )

    @patch('jupyterAuth.authorizer.requests.get')
    def test_verify_os_connection_failure(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.text = 'Unauthorized'
        mock_requests_get.return_value = mock_response

        host_url = 'http://example.com'
        auth = BearerAuth({'access_token': 'fake_token', 'expires_at': 1700000000})

        result = verify_OS_connection(host_url, auth)

        self.assertFalse(result)
        mock_requests_get.assert_called_once_with(
            f"{host_url}/_plugins/_security/api/account",
            verify=False,
            timeout=5,
            auth=auth
        )

    @patch('jupyterAuth.authorizer.requests.get')
    def test_verify_os_connection_request_exception(self, mock_requests_get):
        mock_requests_get.side_effect = RequestException('Connection error')

        host_url = 'http://example.com'
        auth = BearerAuth({'access_token': 'fake_token', 'expires_at': 1700000000})

        result = verify_OS_connection(host_url, auth)

        self.assertFalse(result)
        mock_requests_get.assert_called_once_with(
            f"{host_url}/_plugins/_security/api/account",
            verify=False,
            timeout=5,
            auth=auth
        )

if __name__ == '__main__':
    unittest.main()
