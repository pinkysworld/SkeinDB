# Security Policy

## Supported versions

SkeinDB is a fast-moving prototype. Security fixes are applied to the `main`
branch and included in the next tagged release. Always test against the latest
release before reporting.

## Reporting a vulnerability

Please **do not** open a public issue for security problems.

Use GitHub's private vulnerability reporting:

1. Go to the [Security advisories page](https://github.com/pinkysworld/SkeinDB/security/advisories/new).
2. Describe the issue, affected surface (SkeinQL / MySQL / PostgreSQL / SkeinAdmin /
   storage), and a minimal reproduction.
3. Include the version string from `system.version` or the SkeinAdmin **Version** button.

We aim to acknowledge reports within a few business days. Once a fix is available,
we will coordinate a disclosure timeline with you.

## Scope

In scope:
- The `skeindb` binary and its protocol surfaces (SkeinQL, MySQL, PostgreSQL, HTTP).
- The embedded SkeinAdmin console.

Out of scope:
- Issues that require a pre-compromised host or operator credentials.
- Denial of service from intentionally pathological local configuration.
