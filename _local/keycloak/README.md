# Keycloak Authentication for Armada Local Development

This directory contains the Keycloak configuration for the `auth` profile of Armada local development.

## How It Works

When Keycloak starts with the `auth` Docker Compose profile, it automatically imports the realm configuration from `import/armada-realm.json`. This provides a fully configured OIDC provider with all necessary clients, users, and groups pre-configured for immediate use.

## Auto-provisioned Resources

The imported configuration includes:

### Realm: `armada`
- Display Name: Armada
- Login with email allowed
- SSL required for external requests

### Clients

#### `armada-server`
- Type: Public client (no secret required)
- PKCE enabled (S256 challenge method)
- Redirect URIs configured for:
  - armadactl: `http://localhost:8085/*`
  - Lookout UI: `http://localhost:3000/*`, `http://localhost:3001/*`

#### `armada-executor`
- Type: Confidential client (service account)
- Client secret: `executor-secret`
- Used for service-to-service authentication (executor, scheduler)

### User: `admin`
- Username: `admin`
- Password: `admin`
- Email: `admin@example.com`
- Group membership: `admins`

### Groups
- `admins` - Administrative users
- `executors` - Service accounts for executors
- `users` - Regular users

### Token Configuration
- Access token lifespan: 5 minutes
- SSO session idle: 30 minutes
- SSO session max: 10 hours

## Access Details

- Keycloak Admin Console: http://localhost:8180 (admin/admin)
- OIDC Provider URL: http://localhost:8180/realms/armada

## Configuration Files

- `import/armada-realm.json` - Complete realm configuration
- `../server/config-auth.yaml` - Server OIDC configuration
- `../scheduler/config-auth.yaml` - Scheduler OIDC client credentials configuration
- `../executor/config-auth.yaml` - Executor OIDC client credentials configuration
- `../lookout/config-auth.yaml` - Lookout OIDC configuration (API + UI)
- `~/.armadactl.yaml` - armadactl OIDC contexts (user configuration)

## Authentication Architecture

All components use OIDC authentication with Keycloak:

- **Server**: Accepts both OIDC tokens and basic auth (fallback)
- **Scheduler**: Uses OIDC client credentials flow (service account)
- **Executor**: Uses OIDC client credentials flow (service account)
- **Lookout API**: Accepts OIDC tokens and basic auth
- **Lookout UI**: Browser-based OIDC authentication
- **armadactl**: OIDC authorization code flow with PKCE (user authentication)