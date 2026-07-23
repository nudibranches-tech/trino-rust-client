# Manual OAuth2 test stack (Trino + Keycloak)

A **local, manual** stack for exercising the client's interactive OAuth2 support
against a real Trino coordinator and a real IdP (Keycloak). It backs
`tests/oauth2.rs::oauth2_real_login`.

> **Not run in CI.** The interactive flow requires a human to complete the
> Keycloak login in a browser — there is no automated login here. The stack has
> been brought up and verified up to that human step: the coordinator starts
> healthy and returns the expected `401` + `WWW-Authenticate: Bearer
> x_redirect_server=..., x_token_server=...` challenge over TLS. Completing the
> browser login (the two gotchas below) is the part you drive yourself.

## What's in it

- **Keycloak** (`realm=trino`, confidential client `trino`/`trino-secret`, user
  `alice`/`alice`) on `http://keycloak:8080`.
- **Trino 478** coordinator with TLS on `8443` and
  `http-server.authentication.type=oauth2`, plus a `memory` catalog.
- A one-shot job that generates a self-signed keystore for the coordinator.

## Two gotchas (read before running)

1. **Trino mandates TLS for OAuth2.** The coordinator serves the client over
   `https://localhost:8443`; the client's `auth_http_insecure` can't help here
   because it's *Trino* rejecting plain http, not the client. The stack uses a
   self-signed cert, so run the test with `TRINO_OAUTH2_NO_VERIFY=1` (or import
   the generated cert via `ClientBuilder::ssl`).
2. **Keycloak issuer/hostname must be consistent.** The token `issuer` and
   `jwks-url` (used server-to-server by Trino) and the `auth-url` (opened in your
   host browser) must all resolve to the *same* Keycloak origin, or issuer
   validation fails. The stack pins everything to `http://keycloak:8080`, so add
   a hosts entry so your browser can reach it too:

   ```bash
   echo "127.0.0.1 keycloak" | sudo tee -a /etc/hosts   # one-time
   ```

## Run

```bash
docker compose -f integration_tests/test_setup/oauth/docker-compose.yml up -d

# Wait for Trino to report healthy, then:
TRINO_OAUTH2_HOST=localhost TRINO_OAUTH2_PORT=8443 TRINO_OAUTH2_NO_VERIFY=1 \
    cargo test --test oauth2 -- --ignored oauth2_real_login
```

A browser opens for the Keycloak login — sign in as `alice` / `alice`. The test
then completes the poll → bearer → query round-trip and asserts one row.

The test's Trino session user defaults to `alice` to match the authenticated
principal — Trino denies a query whose session user differs from the OAuth2
principal (`Access Denied: User alice cannot impersonate user ...`) unless
impersonation is explicitly configured. Override with `TRINO_OAUTH2_USER` for a
coordinator whose principal differs.

Tear down with:

```bash
docker compose -f integration_tests/test_setup/oauth/docker-compose.yml down -v
```
