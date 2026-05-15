---
sidebar_position: 3
---

# 🔐 Security Policy

[![Security](https://img.shields.io/badge/Security-Responsible%20Disclosure-red)](https://github.com/schivei/aeternum-db/security)

AeternumDB takes security seriously. If you discover a security vulnerability, please follow our responsible disclosure process.

---

## Supported Versions

| Version | Supported |
|---|---|
| `main` branch | ✅ Active development |
| Tagged releases | ✅ Security patches backported |
| Old branches | ❌ Not supported |

---

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, use one of the following methods:

1. **GitHub Security Advisories** — [Report privately](https://github.com/schivei/aeternum-db/security/advisories/new)
2. **Email** — Contact the maintainer directly (see GitHub profile)

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce the issue
- Any proof-of-concept code (if applicable)
- Your name / handle for credit (optional)

---

## Response Timeline

| Step | Timeline |
|---|---|
| Acknowledgement of report | Within 48 hours |
| Initial assessment | Within 7 days |
| Fix + advisory published | Within 30 days (critical issues: 14 days) |

---

## Security Best Practices

When deploying AeternumDB:

- **Restrict file system access** to database files — they contain raw data pages
- **Rotate credentials** regularly
- **Keep .NET runtime updated** to receive platform security patches
- **Monitor logs** for unexpected access patterns
- **Use encryption at rest** *(roadmap: Phase 6)* for sensitive data

---

:::info
AeternumDB's production code path returns `Result<T>` types instead of throwing exceptions. This design avoids unexpected `catch`-based security bypasses.
:::
