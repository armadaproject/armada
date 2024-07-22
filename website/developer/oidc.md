### Setting up OIDC for developers.

Setting up OIDC can be an art.  The [Okta Developer Program](https://developer.okta.com/signup/) provides a nice to test OAuth flow.

1) Create a Okta Developer Account
    - I used my github account.
2) Create a new App in the Okta UI.
    - Select OIDC - OpenID Connect.
    - Select Web Application.
3) In grant type, make sure to select Client Credentials.  This has the advantage of requiring little interaction.
4) Select 'Allow Everyone to Access'
5) Deselect Federation Broker Mode.
6) Click okay and generate a client secret.
7) Navigate in the Okta settings to the API settings for your default authenticator.
8) Select Audience to be your client id.


Setting up OIDC for Armada requires two separate configs (one for Armada server and one for the clients)

You can add this to your armada server config.
```
 auth:
    anonymousAuth: false
    openIdAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
      groupsClaim: "groups"
      clientId: "CLIENT_ID_FROM_UI"
      scopes: []
```

For client credentials, you can use the following config for the executor and other clients.

```
  openIdClientCredentialsAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
    clientId: "CLIENT_ID_FROM_UI"
    clientSecret: "CLIENT_SECRET"
    scopes: []
```

If you want to interact with Armada, you will have to use one of our client APIs.  The armadactl is not setup to work with OIDC at this time.
