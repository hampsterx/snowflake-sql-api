# Authentication

`snowflake-sql-api` uses Snowflake's keypair (JWT) authentication. Each request
carries a short-lived RS256 JWT signed by your RSA private key; Snowflake
verifies it against the public key registered on your user. There is no password
and no browser SSO flow.

## 1. Generate an RSA key pair

Unencrypted private key (simplest):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Passphrase-encrypted private key (recommended for shared environments):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Keep `rsa_key.p8` secret. Never commit it: the project's `.gitignore` already
excludes `*.p8`, `*.pem`, and `*private_key*`, and a pre-commit `detect-private-key`
hook is a second line of defence.

## 2. Register the public key on your Snowflake user

Take the body of `rsa_key.pub` (everything between the `BEGIN`/`END` lines, with
the newlines removed) and set it on the user. Run this as a role with the
privilege to alter the user (e.g. `SECURITYADMIN`):

```sql
ALTER USER my_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...rest of the public key...';
```

Verify it took:

```sql
DESCRIBE USER my_user;        -- look for RSA_PUBLIC_KEY_FP (the fingerprint)
```

The client computes the same `SHA256:<base64>` fingerprint from your private key
and puts it in the JWT issuer claim, so this fingerprint must match.

## 3. Construct the client

From a key file:

```python
from snowflake_sql_api import SnowflakeClient

client = SnowflakeClient(
    account="myorg-myaccount",
    user="MY_USER",
    private_key_path="/path/to/rsa_key.p8",
)
```

From in-memory PEM bytes (e.g. fetched from a secrets manager):

```python
client = SnowflakeClient(
    account="myorg-myaccount",
    user="MY_USER",
    private_key=pem_bytes,                 # bytes
    private_key_passphrase="my-passphrase",  # only if the key is encrypted
)
```

Or entirely from the environment (see [getting-started.md](getting-started.md)
for the variable list):

```python
client = SnowflakeClient.from_env()
```

## The account-locator region gotcha

This is the single most common keypair failure. Two account forms are derived
**differently**, and the client handles both for you:

- The **JWT claim** account (issuer/subject) must drop any region/cloud suffix
  and be uppercased: `xy12345.ap-southeast-2` becomes `XY12345`. Leaving the
  region in makes the JWT invalid.
- The **API host** keeps the full account (the region routes the request):
  `xy12345.ap-southeast-2` becomes
  `xy12345.ap-southeast-2.snowflakecomputing.com`.

You pass the full account locator (whatever Snowflake shows you, region included)
and the library splits it correctly. The org-account dash form
(`myorg-myaccount`) has no dot, so it is preserved as-is.

If your account uses PrivateLink or a non-standard host, pass `host=` explicitly
to bypass host derivation (the claim account is still derived from `account`).

## Token lifetime

Tokens are signed with a one-hour lifetime (Snowflake's cap) and cached, with a
small renewal margin so an in-flight request never races expiry. You do not need
to manage tokens yourself.

## Troubleshooting auth

A `SnowflakeAuthError` (HTTP 401) almost always means one of:

- the public key is not registered, or does not match the private key;
- the account locator carried a region into the claim (not possible via this
  client unless you bypass it);
- significant clock skew between your host and Snowflake;
- an encrypted key supplied without (or with the wrong) passphrase, which raises
  a `SnowflakeConfigError` before any request.

See [troubleshooting.md](troubleshooting.md) for more.
