# Security Policy

## Supported Versions

MoneyBin is pre-1.0 and under active development. Security fixes are applied to the latest commit on `main`. There are no backported patch releases at this time.

| Version | Supported |
|---------|-----------|
| `main` (latest) | Yes |
| Older commits | No |

## Reporting a Vulnerability

**Please do not file security issues as public GitHub issues.**

MoneyBin handles sensitive financial data, so responsible disclosure is critical. You have two options:

### Option 1: GitHub Private Vulnerability Reporting (Preferred)

1. Go to the [Security tab](https://github.com/bsaffel/moneybin/security) of this repository.
2. Click **"Report a vulnerability"**.
3. Fill out the form with as much detail as you can.

This creates a private advisory where we can discuss and develop a fix before public disclosure.

### Option 2: Email

Send details to [brandon@prestidigital.co](mailto:brandon@prestidigital.co) with the subject line **"MoneyBin Security Report"**.

## What to Include

- Description of the vulnerability
- Steps to reproduce (or a proof of concept)
- Affected component (database encryption, import pipeline, MCP server, CLI, etc.)
- Potential impact as you understand it

## What to Expect

- **Acknowledgment**: Within 48 hours of your report.
- **Assessment**: A severity determination within 7 days.
- **Resolution targets**:
  - Critical (data exposure, encryption bypass): patch within 7 days.
  - High (privilege escalation, injection): patch within 14 days.
  - Moderate/Low: patch within 30 days.
- **Disclosure**: We will coordinate with you on public disclosure timing. We aim to disclose within 7 days of the fix being released.

## Scope

The following are in scope for security reports:

- Database encryption bypass or key exposure
- SQL injection or command injection
- PII/financial data leakage through logs, error messages, or MCP responses
- Path traversal in file import operations
- Authentication or authorization flaws in the sync server integration
- Dependencies with known CVEs that affect MoneyBin's usage

The following are **not** security issues (file as regular issues instead):

- Incorrect financial calculations
- Missing input validation that doesn't lead to exploitation
- Feature requests for additional security controls

## Acknowledgment

Reporters who responsibly disclose valid vulnerabilities will be credited in the release notes (unless they prefer to remain anonymous).
