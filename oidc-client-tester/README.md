# OpenID Connect Demo

[![Quality](https://img.shields.io/badge/quality-demo-red)](https://curity.io/resources/code-examples/status/)
[![Availability](https://img.shields.io/badge/availability-source-blue)](https://curity.io/resources/code-examples/status/)

This is a demo application to explain how the OpenID Connect code flow is implemented.

## Usage

```bash
$ python app.py
```

Flask will start a web server listening on all interfaces that can be used for demo purposes. The webserver will use HTTPS with a certificate for localhost.
Browse to https://localhost:5443 to see the app.

## Dependencies

**python 3.x** (tested with python 3.9.1)

**OpenSSL 1.0** to be able to do modern TLS versions. Python together with 0.9.x has a bug that makes it impossible to select protocol in the handshake, so it cannot connect to servers that have disabled SSLv2.

Python dependencies can be installed by using PIP: `pip install -r requirements.txt`

## settings.json
Settings.json is used as a configuration file for the example app. Change the values to match your system.

Name                | Type    | Default  | Description
--------------------| ------- | -------- | :---------------
`issuer`            | string  |          | The ID of the token issuer. This is used for both OpenID Connect Discovery, and validating a ID Token. Mandatory for discovery
`client_id`         | string  |          | The ID for the client. Used to authenticate the client against the authorization server endpoint.
`client_secret`     | string  |          | The shared secret to use for authentication against the token endpoint.
`dcr_client_id`     | string  |          | The client ID of the client for to use for registration.
`dcr_client_secret` | string  |          | The client secret of the client for to use for registration.
`scope`             | string  | `openid` | The scopes to ask for.
`verify_ssl_server` | boolean | `true`   | Set to false to disable certificate checks.
`debug`             | boolean | `false`  | If set to true, Flask will be in debug mode and write stacktraces if an error occurs. Some extra logging is also printed.
`port`              | number  | `5443`   | The port that the Flask server should listen to
`disable_https`     | boolean | `false`  | Set to true to run on http
`base_url`          | string  |          | base url to be added to internal redirects. If this is not configured, the base url will be extracted from the first request to the index page
`send_parameters_via`|string  | `query_string`|How request parameters should be sent to the authorization endpoint. Valid values are `query_string`, `request_object` or `request_uri`.
`request_object_keys`|JSON object|       | The JSON Web Key (JWK) used to sign JWTs used when sending authorization request parameters by-value in a request object or by reference in a request URI. For example:<br>`{`<br>`"kty":"RSA",`<br>`"n":"0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",`<br>`"e":"AQAB",`<br>`"d":"X4cTteJY_gn4FYPsXB8rdXix5vwsg1FLN5E3EaG6RJoVH-HLLKD9M7dx5oo7GURknchnrRweUkC7hT5fJLM0WbFAKNLWY2vv7B6NqXSzUvxT0_YSfqijwp3RTzlBaCxWp4doFk5N2o8Gy_nHNKroADIkJ46pRUohsXywbReAdYaMwFs9tv8d_cPVY3i07a3t8MN6TNwm0dSawm9v47UiCl3Sk5ZiG7xojPLu4sbg1U2jx4IBTNBznbJSzFHK66jT8bgkuqsk0GjskDJk19Z4qwjwbsnn4j2WBii3RL-Us2lGVkY8fkFzme1z0HbIkfz0Y6mqnOYtqc0X4jfcKoAC8Q",`<br>`"p":"83i-7IvMGXoMXCskv73TKr8637FiO7Z27zv8oj6pbWUQyLPQBQxtPVnwD20R-60eTDmD2ujnMt5PoqMrm8RfmNhVWDtjjMmCMjOpSXicFHj7XOuVIYQyqVWlWEh6dN36GVZYk93N8Bc9vY41xy8B9RzzOGVQzXvNEvn7O0nVbfs",`<br>`"q":"3dfOR9cuYq-0S-mkFLzgItgMEfFzB2q3hWehMuG0oCuqnb3vobLyumqjVZQO1dIrdwgTnCdpYzBcOfW5r370AFXjiWft_NGEiovonizhKpo9VVS78TzFgxkIdrecRezsZ-1kYd_s1qDbxtkDEgfAITAG9LUnADun4vIcb6yelxk",`<br>`"dp":"G4sPXkc6Ya9y8oJW9_ILj4xuppu0lzi_H7VTkS8xj5SdX3coE0oimYwxIi2emTAue0UOa5dpgFGyBJ4c8tQ2VF402XRugKDTP8akYhFo5tAA77Qe_NmtuYZc3C3m3I24G2GvR5sSDxUyAN2zq8Lfn9EUms6rY3Ob8YeiKkTiBj0",`<br>`"dq":"s9lAH9fggBsoFR8Oac2R_E2gw282rT2kGOAhvIllETE1efrA6huUUvMfBcMpn8lqeW6vzznYY5SSQF7pMdC_agI3nG8Ibp1BUb0JUiraRNqUfLhcQb_d9GF4Dh7e74WbRsobRonujTYN1xCaP6TO61jvWrX-L18txXw494Q_cgk",`<br>`"qi":"GyM_p6JrXySiz1toFgKbWV-JdI3jQ4ypu9rbMWx3rQJBfmt0FoYzgUIZEVFEcOqwemRN81zoDAaa-Bk0KWNGDjJHZDdDmFhW3AN7lI-puxk_mHZGJ11rxyR8O55XLSe3SPmRfKwZI6yU24ZxvQKFYItdldUKGzO6Ia6zTKhAVRU",`<br>`"alg":"RS256",`<br>`"kid":"2011-04-29"`<br>`}`

### Mandatory parameters if discovery is not available
Name                     | Type |  Description
-------------------------|------|-------------
`jwks_uri`               | URL  |  The URL that points to the JWK set. Mandatory if the openid scope is requested.
`authorization_endpoint` |      |  The URL to the authorization endpoint.
`token_endpoint`         | URL  |  The URL to the token endpoint.
`registration_endpoint`  | URL  |  The URL to the registration endpoint.

## Docker
To run the example in a Docker container, build an image and run a container like this.:

```bash
$ docker build -t curityio/openid-python-example .
$ docker run -ti curityio/openid-python-example

```
All setting can be set using an environment variable with uppercase letters. Example:
```bash
$ docker build -t curityio/openid-python-example
$ docker run -e DEBUG=true -e ISSUER=se.curity -ti curityio/openid-python-example
```
## Docker Compose
In the root of the repository, there is a `docker-compose.yml`. Customize the settings using environment variables with uppercase letters.

```bash
$ docker-compose up
```

## Questions and Support

For questions and support, contact Curity AB:

> Curity AB
>
> info@curity.io
> https://curity.io


Copyright (C) 2016 Curity AB.
