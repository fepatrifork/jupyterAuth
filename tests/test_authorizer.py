import unittest
from unittest.mock import patch, MagicMock
from jupyterAuth.authorizer import Authorizer

class TestAuthorizer(unittest.TestCase):

    @patch('jupyterAuth.authorizer.urllib3.disable_warnings')
    @patch('jupyterAuth.authorizer.os.environ')
    def test_init_disable_https(self, mock_environ, mock_disable_warnings):
        mock_environ.get.return_value = None
        authorizer = Authorizer('localhost', 8080, 'http://example.com', 'scope', True)
        mock_disable_warnings.assert_called_once()
        mock_environ.__setitem__.assert_called_once_with('OAUTHLIB_INSECURE_TRANSPORT', '1')

    @patch('jupyterAuth.authorizer.os.environ')
    def test_init_enable_https(self, mock_environ):
        mock_environ.get.return_value = None
        authorizer = Authorizer('localhost', 8080, 'http://example.com', 'scope', False)
        mock_environ.__setitem__.assert_called_once_with('OAUTHLIB_INSECURE_TRANSPORT', '0')

    @patch('jupyterAuth.authorizer.os.environ')
    @patch('jupyterAuth.authorizer.OAuth2Session')
    @patch('jupyterAuth.authorizer.BearerAuth')
    def test_get_oauth2(self, mock_bearer_auth, mock_oauth_session, mock_environ):
        mock_environ.get.side_effect = [None, None, None, None, 'http']
        mock_oauth_session.return_value.fetch_token.return_value = 'fake_token'
        authorizer = Authorizer('localhost', 8080, 'http://example.com', 'scope', True)
        with patch('jupyterAuth.authorizer.util.verify_OS_connection', return_value=True):
            auth = authorizer.getOauth2('user', 'password')
            self.assertEqual(auth, mock_bearer_auth.return_value)
        mock_bearer_auth.assert_called_once_with('fake_token')

    @patch('jupyterAuth.authorizer.os.environ')
    @patch('jupyterAuth.authorizer.OAuth2Session')
    @patch('jupyterAuth.authorizer.BearerAuth')
    def test_get_jwtoken(self, mock_bearer_auth, mock_oauth_session, mock_environ):
        mock_environ.get.side_effect = [None, None, None, None, 'http']
        mock_oauth_session.return_value.fetch_token.return_value = 'fake_token'
        authorizer = Authorizer('localhost', 8080, 'http://example.com', 'scope', True)
        with patch('jupyterAuth.authorizer.util.verify_OS_connection', return_value=True):
            token = authorizer.getJWToken('user', 'password')
            self.assertEqual(token, 'fake_token')

if __name__ == '__main__':
    unittest.main()
