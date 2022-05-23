##########################################################################
# Copyright 2017 Curity AB
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

import os
import json

class Config():

    _keys = ['api_endpoint',
            'authn_parameters',
            'authorization_endpoint',
            'base_url',
            'client_id',
            'client_secret',
            'dcr_client_id',
            'dcr_client_secret',
            'debug',
            'disable_https',
            'discovery_url',
            'issuer',
            'audience',
            'jwks_uri',
            'end_session_endpoint',
            'port',
            'redirect_uri',
            'revocation_endpoint',
            'scope',
            'token_endpoint',
            'verify_ssl_server']

    def __init__(self, filename):
        self.filename = filename

    def load_config(self):
        """
        Load config from file and environment
        :return:
        """
        self._load_from_file(self.filename)
        self._update_config_from_environment()
        return self.store

    def _load_from_file(self, filename):
        print('Loading settings from %s' % filename)
        self.store = json.loads(open(filename).read())

    def _update_config_from_environment(self):
        from_env = {}
        for key in self._keys:
            env = os.environ.get(key.upper(), None)
            if env:
                from_env[key] = env
        self.store.update(from_env)
