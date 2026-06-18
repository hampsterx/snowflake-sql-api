# Security Policy

## Reporting a vulnerability

Please report security issues privately. Do **not** open a public issue for a
suspected vulnerability.

- Preferred: open a private advisory via GitHub Security Advisories on this
  repository ("Security" tab -> "Report a vulnerability").
- Alternative: email the maintainer at tim.vdh@gmail.com with details and, if
  possible, a minimal reproduction.

Please include the affected version, the impact, and steps to reproduce. You can
expect an acknowledgement within a few days. Once a fix is available it will be
released to PyPI and the advisory published.

## Scope

This library performs keypair (JWT) authentication and sends SQL over HTTPS to
Snowflake's SQL API. Of particular interest:

- handling of private keys and generated JWTs;
- parameter binding / SQL injection surface;
- TLS and request construction in the transport layer.

## Handling secrets

Never commit private keys. The repository's `.gitignore` excludes `*.pem`,
`*.p8`, `*.key`, and `*private_key*`, and a pre-commit `detect-private-key` hook
provides a second check. Provide keys at runtime via a file path, in-memory
bytes, or environment variables (see [docs/authentication.md](docs/authentication.md)).
