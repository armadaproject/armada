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
import hashlib
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import URLError
from urllib.request import Request

from jwkest.jwk import KEYS
from jwkest.jws import JWS

import tools

REGISTERED_CLIENT_FILENAME = 'registered_client.json'


def make_request_object(request_args, jwk):
    keys = KEYS()
    jws = JWS(request_args)

    if jwk:
        keys.load_jwks(json.dumps(dict(keys=[jwk])))

    return jws.sign_compact(keys)


class Client:
    def __init__(self, config):
        self.config = config

        print('Getting ssl context for oauth server')
        self.ctx = tools.get_ssl_context(self.config)
        self.__init_config()
        self.client_data = None

    def __init_config(self):

        if 'issuer' in self.config:
            meta_data_url = self.config['issuer'] + '/.well-known/openid-configuration'
            print('Fetching config from: %s' % meta_data_url)
            meta_data = urlopen(meta_data_url, context=self.ctx)
            if meta_data:
                self.config.update(json.load(meta_data))
            else:
                print('Unexpected response on discovery document: %s' % meta_data)
        else:
            print('Found no issuer in config, can not perform discovery. All endpoint config needs to be set manually')

        # Mandatory settings
        if 'authorization_endpoint' not in self.config:
            raise Exception('authorization_endpoint not set.')
        if 'token_endpoint' not in self.config:
            raise Exception('token_endpoint not set.')

        self.read_credentials_from_file()
        if 'client_id' not in self.config:
            print('Client is not registered.')

        if 'scope' not in self.config:
            self.config['scope'] = 'openid'

    def read_credentials_from_file(self):
        if not os.path.isfile(REGISTERED_CLIENT_FILENAME):
            print('Client is not dynamically registered')
            return

        try:
            registered_client = json.loads(open(REGISTERED_CLIENT_FILENAME).read())
        except Exception as e:
            print('Could not read credentials from file', e)
            return
        self.config['client_id'] = registered_client['client_id']
        self.config['client_secret'] = registered_client['client_secret']
        self.config['redirect_uri'] = registered_client['redirect_uris'][0]
        self.client_data = registered_client

    def register(self):
        """
        Register a client at the AS
        :raises: raises error when http call fails
        """
        if 'registration_endpoint' not in self.config:
            print('Authorization server does not support Dynamic Client Registration. Please configure client ' \
                  'credentials manually ')
            return

        if 'client_id' in self.config:
            raise Exception('Client is already registered')

        dcr_access_token = None

        if 'dcr_client_id' in self.config and 'dcr_client_secret' in self.config:
            # DCR endpoint requires an access token, so perform CC flow and get one
            dcr_access_token = self.get_registration_token()

        if 'template_client' in self.config:
            print('Registering client using template_client: %s' % self.config['template_client'])
            data = {
                'software_id': self.config['template_client']
            }
        else:
            data = {
                'client_name': 'OpenID Connect Demo',
                'grant_types': ['implicit', 'authorization_code', 'refresh_token'],
                'redirect_uris': [self.config['redirect_uri']]
            }

            if self.config['debug']:
                print('Registering client with data:\n %s' % json.dumps(data))

        register_response = self.__urlopen(self.config['registration_endpoint'], data=json.dumps(data),
                                           context=self.ctx, token=dcr_access_token)
        self.client_data = json.loads(register_response.read())

        with open(REGISTERED_CLIENT_FILENAME, 'w') as outfile:
            outfile.write(json.dumps(self.client_data))

        if self.config['debug']:
            tools.print_json(self.client_data)

        self.read_credentials_from_file()

    def clean_registration(self, config):
        """
        Removes the registration file and reloads config
        :return:
        """
        os.remove(REGISTERED_CLIENT_FILENAME)
        config.pop('client_id', None)
        config.pop('client_secret', None)
        self.client_data = None
        self.config = config

    def revoke(self, token, token_type_hint="access_token"):
        """
        Revoke the token
        :param token: the token to revoke
        :param token_type_hint: a hint to the OAuth server about the kind of token being revoked
        :raises: raises error when http call fails
        """
        if 'revocation_endpoint' not in self.config:
            print('No revocation endpoint set')
            return

        data = {
            'token': token,
            "token_type_hint": token_type_hint,
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret']
        }

        self.__urlopen(self.config['revocation_endpoint'], urlencode(data), context=self.ctx)

    def refresh(self, refresh_token):
        """
        Refresh the access token with the refresh_token
        :param refresh_token:
        :return: the new access token
        """
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret']
        }
        token_response = self.__urlopen(self.config['token_endpoint'], urlencode(data), context=self.ctx)
        return json.loads(token_response.read())

    def get_authn_req_url(self, session, acr, forceAuthN, scope, forceConsent, allowConsentOptionDeselection,
                          response_type, ui_locales, max_age, claims, send_parameters_via):
        """
        :param session: the session, will be used to keep the OAuth state
        :param acr: The acr to request
        :param force_authn: Force the resource owner to authenticate even though a session exist
        :return redirect url for the OAuth code flow
        """
        state = tools.generate_random_string()
        session['state'] = state
        session['code_verifier'] = code_verifier = tools.generate_random_string(100)
        session["flow"] = response_type

        code_challenge = tools.base64_urlencode(hashlib.sha256(code_verifier).digest())

        request_args = {'scope': scope,
                        'response_type': response_type,
                        'client_id': self.config['client_id'],
                        'state': state,
                        'code_challenge': code_challenge,
                        'code_challenge_method': "S256",
                        'redirect_uri': self.config.get('redirect_uri')}

        if 'authn_parameters' in self.config:
            request_args.update(self.config['authn_parameters'])

        if acr: request_args["acr_values"] = acr

        if ui_locales: request_args["ui_locales"] = ui_locales

        if max_age: request_args["max_age"] = max_age

        if forceAuthN: request_args["prompt"] = "login"

        if claims: request_args["claims"] = claims

        if forceConsent:
            if allowConsentOptionDeselection:
                request_args["prompt"] = request_args.get("prompt", "") + " consent consent_allow_deselection"
            else:
                request_args["prompt"] = request_args.get("prompt", "") + " consent"

        if response_type.find("id_token"):
            request_args["nonce"] = session["nonce"] = tools.generate_random_string()

        delimiter = "?" if self.config['authorization_endpoint'].find("?") < 0 else "&"

        if send_parameters_via == "request_object":
            request_object_claims = request_args
            request_object_claims.update(dict(
                issuer=self.config["client_id"],
                aud=self.config["issuer"],
                exp=int(time.time()) + 180,  # Expires in 3 minutes
                purpose="request"  # FIXME: Required for Curity's implementation of request object (for some reason)
            ))
            request_args = dict(
                request=make_request_object(request_object_claims, self.config.get("request_object_key", None)),
                client_id=request_args["client_id"],
                code_challenge=request_args["code_challenge"],  # FIXME: Curity can't currently handle PCKE if not
                code_challenge_method=request_args["code_challenge_method"],  # provided on query string
                scope=request_args["scope"],
                response_type=request_args["response_type"],
                redirect_uri=request_args["redirect_uri"]  # FIXME: Curity requires this even if in request obj
            )
        elif send_parameters_via == "request_uri":
            request_args = None  # TODO: Implement request URI support

        login_url = "%s%s%s" % (self.config['authorization_endpoint'], delimiter, urlencode(request_args))

        print("Redirect to %s" % login_url)

        return login_url

    def get_token(self, code, code_verifier):
        """
        :param code: The authorization code to use when getting tokens
        :param code_verifier: The original code verifier sent with the authorization request
        :return the json response containing the tokens
        """
        data = {'client_id': self.config['client_id'], "client_secret": self.config['client_secret'],
                'code': code,
                "code_verifier": code_verifier,
                'redirect_uri': self.config['redirect_uri'],
                'grant_type': 'authorization_code'}

        # Exchange code for tokens
        try:
            token_response = self.__urlopen(self.config['token_endpoint'], urlencode(data), context=self.ctx)
        except URLError as te:
            print("Could not exchange code for tokens")
            raise te
        return json.loads(token_response.read())

    def get_client_data(self):
        if not self.client_data:
            self.read_credentials_from_file()

        return self.client_data

    def get_registration_token(self):

        if 'dcr_client_id' not in self.config:
            raise Exception('Can not run client registration. Missing client id.')

        if 'dcr_client_secret' not in self.config:
            raise Exception('Can not run client registration. Missing client secret.')

        data = {
            'client_id': self.config['dcr_client_id'],
            'client_secret': self.config['dcr_client_secret'],
            'grant_type': 'client_credentials',
            'scope': 'dcr'
        }

        try:
            token_response = self.__urlopen(self.config['token_endpoint'], urlencode(data), context=self.ctx)
        except URLError as te:
            print("Could not get DCR access token")
            raise te

        json_response = json.loads(token_response.read())
        if self.config['debug']:
            print('Got DCR token response: %s ' % json_response)

        return json_response['access_token']

    def __urlopen(self, url, data=None, context=None, token=None):
        """
        Open a connection to the specified url. Sets valid requests headers.
        :param url: url to open - cannot be a request object 
        :param data: data to send, optional
        :param context: ssl context
        :param token: token to add to the authorization header
        :return the request response
        """
        headers = {
            'User-Agent': 'CurityExample/1.0',
            'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,'
                      '*/*;q=0.8 '
        }
        if token:
            headers['Authorization'] = 'Bearer %s' % token

        if data is not None:
            data = data.encode('utf-8')

        request = Request(url, data, headers)

        if self.config['debug']:
            print('Request url: ' + url)
            print('Request headers:\n' + json.dumps(headers))
            print('Request data:\n' + json.dumps(data.decode() if data is not None else None))

        return urlopen(request, context=context)

    def __authn_req_args(self, state, scope, code_challenge, code_challenge_method="plain"):
        """
        :param state: state to send to authorization server
        :return a map of arguments to be sent to the authz endpoint
        """
        if 'client_id' not in self.config:
            raise Exception('Client is not registered')

        args = {'scope': scope,
                'response_type': 'code',
                'client_id': self.config['client_id'],
                'state': state,
                'code_challenge': code_challenge,
                'code_challenge_method': code_challenge_method,
                'redirect_uri': self.config['redirect_uri']}

        if 'authn_parameters' in self.config:
            args.update(self.config['authn_parameters'])
        return args
