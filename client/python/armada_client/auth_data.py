from enum import auto, Enum
from typing import Optional


class AuthMethod(Enum):
    Anonymous = auto()
    Basic = auto()
    OpenId = auto()
    Kerberos = auto()


class AuthData:
    def __init__(self, method: AuthMethod = AuthMethod.Anonymous,
                 # for BasicAuth
                 username: Optional[str] = None, password: Optional[str] = None,
                 oidc_token: Optional[str] = None,
                 # TODO add options for kerberos authentication
                 ):

        self.method = method

        if self.method == AuthMethod.Anonymous:
            pass
        elif self.method == AuthMethod.Basic:
            if not (username and password):
                raise Exception("need username and password for Basic auth")
            self.username = username
            self.password = password
        elif self.method == AuthMethod.OpenId:
            if not oidc_token:
                raise Exception("need oidc_token for OpenId auth")
        elif self.method == AuthMethod.Kerberos:
            pass

