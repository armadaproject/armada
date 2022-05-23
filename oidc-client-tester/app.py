#!/usr/bin/env python

##########################################################################
# Copyright 2016 Curity AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

import sys
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from flask import redirect, request, render_template, session, abort, Flask
from jwkest import BadSignature

import tools
from client import Client
from config import Config
from tools import decode_token, generate_random_string, print_json
from validator import JwtValidator

_app = Flask(__name__)


class UserSession:
    def __init__(self):
        pass

    access_token = None
    refresh_token = None
    id_token = None
    access_token_json = None
    id_token_json = None
    name = None
    api_response = None
    front_end_id_token = None
    front_end_id_token_json = None
    front_end_access_token = None


@_app.route('/')
def index():
    """
    :return: the index page with the tokens, if set.
    """
    user = None
    if 'session_id' in session:
        user = _session_store.get(session['session_id'])

    if 'base_url' not in _config or not _config['base_url']:
        _config['base_url'] = request.base_url

    if 'redirect_uri' not in _config:
        _config['redirect_uri'] = _config['base_url'].rstrip('/') + '/callback'

    if isinstance(user, (bytes, str)):
        # User is a string! Probably a bunch of HTML from a previous error. Just bail and hope for the best.
        return user

    if user:
        if user.front_end_id_token:
            user.front_end_id_token_json = decode_token(user.front_end_id_token)

        if user.front_end_access_token:
            user.front_end_access_token_json = decode_token(user.front_end_access_token)

        if user.id_token:
            user.id_token_json = decode_token(user.id_token)

        if user.access_token:
            user.access_token_json = decode_token(user.access_token)

        return render_template('index.html',
                               server_name=_config['issuer'],
                               session=user, flow=session.get("flow", "code"))
    else:
        client_data = _client.get_client_data()
        dynamically_registered = bool(client_data and 'client_id' in client_data)
        using_static_registration = "client_id" in _config and "client_secret" in _config
        registered = dynamically_registered or using_static_registration
        client_id = client_data['client_id'] if dynamically_registered else _config.get("client_id", "")

        return render_template('welcome.html',
                               registered=registered,
                               client_id=client_id,
                               server_name=_config['issuer'],
                               client_data=client_data,
                               flow="code",
                               using_dynamic_registration=dynamically_registered,
                               authorization_endpoint=_config["authorization_endpoint"])


@_app.route('/start-login')
def start_code_flow():
    """
    :return: redirects to the authorization server with the appropriate parameters set.
    """
    provided_scopes = request.args.get("scope")
    default_scopes = _client.config['scope']
    scopes = provided_scopes if provided_scopes else default_scopes

    login_url = _client.get_authn_req_url(session, request.args.get("acr", None),
                                          request.args.get("forceAuthN", False),
                                          scopes, request.args.get("forceConsent", False),
                                          request.args.get("allowConsentOptionDeselection", False),
                                          request.args.get("responseType", "code"),
                                          request.args.get("ui_locales"),
                                          request.args.get("max_age"),
                                          request.args.get("claims"),
                                          _config.get("send_parameters_via", "query_string"))
    return redirect(login_url)


@_app.route('/logout')
def logout():
    """
    Logout clears the session, along with the tokens
    :return: redirects to /
    """
    if 'session_id' in session:
        del _session_store[session['session_id']]
    session.clear()
    
    print("Logging out at ", _config['end_session_endpoint'])
    logout_request = _config['end_session_endpoint'] + '?client_id=' + _config['client_id'] + '&post_logout_redirect_uri=' + _config['base_url']
    return redirect(logout_request)

@_app.route('/refresh')
def refresh():
    """
    Refreshes the access token using the refresh token
    :return: redirects to /
    """
    user = _session_store.get(session['session_id'])
    try:
        token_data = _client.refresh(user.refresh_token)
    except Exception as e:
        create_error('Could not refresh Access Token', e)
        return
    user.access_token = token_data['access_token']
    user.refresh_token = token_data['refresh_token']
    return redirect_with_baseurl('/')


@_app.route('/revoke')
def revoke():
    """
    Revokes the access and refresh token and clears the sessions
    :return: redirects to /
    """
    if 'session_id' in session:
        user = _session_store.get(session['session_id'])
        if not user:
            redirect_with_baseurl('/')

        token = None
        token_type_hint = "access_token"
        error_message = "Could not revoke access token"

        if user.refresh_token and "refresh_token" in request.args:
            error_message = 'Could not revoke refresh token'
            token_type_hint = "refresh_token"
            token = user.refresh_token
            user.refresh_token = None
            user.access_token = None
        elif user.access_token and "back_end_access_token" in request.args:
            token = user.access_token
            user.access_token = None
        elif user.front_end_access_token and "front_end_access_token" in request.args:
            token = user.front_end_access_token
            user.front_end_access_token = None
        elif "id_token" in request.args:
            token_type_hint = "id"
            error_message = "Could not revoke ID token"

            if "back_end" in request.args and user.id_token:
                token = user.id_token
                user.id_token = None
            elif "front_end" in request.args and user.front_end_id_token:
                token = user.front_end_id_token
                user.front_end_id_token = None
            else:
                abort(400)
        else:
            abort(400)

        try:
            _client.revoke(token, token_type_hint)
        except URLError as e:
            return create_error(error_message, e)

    return redirect_with_baseurl('/')


@_app.route('/register')
def register():
    """
    Register the client to get unique client credentials
    :return: redirects to /
    """

    try:
        _client.register()
    except Exception as e:
        return create_error('Could not register client dynamically: %s' % e, e)

    return redirect_with_baseurl('/')


@_app.route('/clean-registration')
def clean_registration():
    """
    Remove the registration file to be able to re register
    :return: redirects to /
    """
    _client.clean_registration(_config)

    return redirect_with_baseurl('/')


@_app.route('/call-api')
def call_api():
    """
    Call an api using the Access Token
    :return: the index template with the data from the api in the parameter 'data'
    """

    if 'session_id' in session:
        user = _session_store.get(session['session_id'])
        if not user:
            return redirect_with_baseurl('/')
        if 'api_endpoint' in _config:
            user.api_response = None
            if "front-end" in request.args and user.front_end_access_token:
                access_token = user.front_end_access_token
            elif user.access_token:
                access_token = user.access_token
            else:
                user.api_response = None
                print('No access token in session')

                return redirect_with_baseurl("/")

            try:
                req = Request(_config['api_endpoint'])
                req.add_header('User-Agent', 'CurityExample/1.0')
                req.add_header("Authorization", "Bearer %s" % access_token)
                req.add_header("Accept", 'application/json')
                
                if 'subscription_key' in _config:
                    req.add_header('Ocp-Apim-Subscription-Key', _config['subscription_key'])
                    req.add_header('Ocp-Apim-Trace', 'true')

                response = urlopen(req, context=tools.get_ssl_context(_config))
                user.api_response = {'code': response.code, 'data': response.read()}
            except HTTPError as e:
                user.api_response = {'code': e.code, 'data': e.read()}
            except Exception as e:
                message = e.message if len(e.message) > 0 else "unknown error"
                user.api_response = {"code": "unknown error", "data": message}
        else:
            user.api_response = None
            print('No API endpoint configured')

    return redirect_with_baseurl('/')


@_app.route("/callback-js", methods=['POST'])
def ajax_callback():
    if 'state' not in session or session['state'] != request.form['state']:
        abort(400) # TODO: Provide nicer error to AJAX client

    if "code_verifier" not in session:
        abort(400)

    if 'code' not in request.form:
        abort(400)

    user = callback(request.form)

    user.front_end_id_token = request.form.get("id_token", "")
    user.front_end_access_token = request.form.get("access_token", "")

    if "session_id" in session:
        _session_store[session['session_id']] = user
    else:
        # New session. Unexpected since code flow would normally be done first. Make a new session instead
        session['session_id'] = generate_random_string()
        _session_store[session['session_id']] = user

    return "ok"


@_app.route('/callback')
def oauth_callback():
    """
    Called when the resource owner is returning from the authorization server
    :return:redirect to / with user info stored in the session.
    """
    if session.get("flow", None) != "code":
        # This is the callback for a hybrid or implicit flow
        return render_template('index.html')

    if 'state' not in session or session['state'].decode() != request.args['state']:
        return create_error('Missing or invalid state')

    if "code_verifier" not in session:
        return create_error("No code_verifier in session")

    if 'code' not in request.args:
        return create_error('No code in response')

    user = callback(request.args)

    session['session_id'] = generate_random_string()
    _session_store[session['session_id']] = user

    return redirect_with_baseurl('/')


def callback(params):
    session.pop('state', None)

    try:
        token_data = _client.get_token(params['code'], session["code_verifier"].decode())
    except Exception as e:
        return create_error('Could not fetch token(s)', e)

    # Store in basic server session, since flask session use cookie for storage
    user = UserSession()

    if 'access_token' in token_data:
        user.access_token = token_data['access_token']

    if _jwt_validator and 'id_token' in token_data:
        # validate JWS; signature, aud and iss.
        # Token type, access token, ref-token and JWT
        if 'issuer' not in _config:
            return create_error('Could not validate token: no issuer configured')

        if 'audience' in _config:
            audience = _config['audience']
        elif 'template_client' in _config:
            audience = _config['template_client']
        else:
            audience = _config['client_id']

        try:
            _jwt_validator.validate(token_data['id_token'], _config['issuer'], audience)
        except BadSignature as bs:
            return create_error('Could not validate token: %s' % bs.message)
        except Exception as ve:
            return create_error("Unexpected exception: %s" % ve.message)

        user.id_token = token_data['id_token']

    if 'refresh_token' in token_data:
        user.refresh_token = token_data['refresh_token']

    return user


def create_error(message, exception=None):
    """
    Print the error and output it to the page
    :param exception:
    :param message:
    :return: redirects to index.html with the error message
    """
    print('Caught error!')
    print(message, exception)
    if _app:
        user = UserSession()
        if 'session_id' in session:
            user = _session_store.get(session['session_id'])
        return render_template('index.html',
                               flow="code",
                               server_name=_config['issuer'],
                               session=user,
                               error=message)


def load_config():
    """
    Load config from the file given by argument, or settings.json
    :return:
    """
    if len(sys.argv) > 1:
        print("Using an alternative config file: %s" % sys.argv[1])
        filename = sys.argv[1]
    else:
        filename = 'settings.json'
    config = Config(filename)

    return config.load_config()


def redirect_with_baseurl(path):
    return redirect(_config['base_url'] + path)


if __name__ == '__main__':
    # load the config
    _config = load_config()

    _client = Client(_config)

    # load the jwk set.
    if 'jwks_uri' in _config:
        _jwt_validator = JwtValidator(_config)
    else:
        print('Found no url to JWK set, will not be able to validate JWT signature.')
        _jwt_validator = None

    # create a session store
    _session_store = {}

    # initiate the app
    _app.secret_key = generate_random_string()

    # some default values
    if 'port' in _config:
        port = int(_config['port'])
    else:
        port = 5443

    _disable_https = 'disable_https' in _config and _config['disable_https']

    if 'base_url' not in _config:
        _config['base_url'] = 'https://localhost:%i' % port

    debug = _config['debug'] = 'debug' in _config and _config['debug']

    if debug:
        print('Running conf:')
        print_json(_config)

    if _disable_https:
        _app.run('0.0.0.0', debug=debug, port=port)
    else:
        _app.run('0.0.0.0', debug=debug, port=port, ssl_context='adhoc')
