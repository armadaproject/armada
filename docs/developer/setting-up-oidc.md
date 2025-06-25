# Setting up OIDC

You can use the [Okta Developer Program](https://developer.okta.com/signup/) to test OAuth flow.

1) Create a Okta Developer Account
    - I used my GitHub account.
2) Create a new App in the Okta UI.
    - Select **OIDC - OpenID Connect**.
    - Select **Web Application**.
3) In grant type, select **Client Credentials**.
4) Select **Allow Everyone to Access**.
5) Deselect **Federation Broker Mode**.
6) Select **OK** and generate a client secret.
7) In the Okta settings, go to the API settings for your default authenticator.
8) Select **Audience** to be your client ID.

## Config requirements

Setting up OIDC for Armada requires two separate configs (one for the Armada server, one for the client's)

Add the following to your Armada server config:

```
 auth:
    anonymousAuth: false
    openIdAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
      groupsClaim: "groups"
      clientId: "CLIENT_ID_FROM_UI"
      scopes: []
```

For client credentials, use the following config for the executor and other clients.

```
  openIdClientCredentialsAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
    clientId: "CLIENT_ID_FROM_UI"
    clientSecret: "CLIENT_SECRET"
    scopes: []
```

If you want to interact with Armada, you have to use one of our client APIs. The armadactl is not setup to work with OIDC at this time.
