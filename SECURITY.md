# Security Policy

## Reporting a vulnerability

Please report security issues privately rather than opening a public issue.

- Use GitHub's [private vulnerability reporting](https://github.com/rmonteiro-pereira/Scraper-Lib/security/advisories/new) — preferred, as it keeps the report and the fix in one place.
- Or email **rmonteiropereira1@gmail.com** with `SECURITY` in the subject.

Please include what you were running (version or commit), the steps to reproduce, and what
you observed. A proof of concept helps but is not required.

Expect an acknowledgement within **7 days**. This is a personal open-source project, not a
staffed product, so please treat that as a best effort rather than a guarantee.

## Supported versions

Only the latest release on the default branch receives fixes. Older tags are not patched.

## Scope

This library downloads files from URLs you supply and writes them to disk. When assessing
impact, the areas that matter most are:

- **Path handling** — a crafted remote filename that escapes the download directory.
- **URL handling** — redirects to unintended hosts or schemes.
- **State files** — the download-state JSON is written and re-read across runs; corrupting
  it should never lead to writing outside the configured directory.
- **Dependency vulnerabilities** — reachable through this library's public API.

## Out of scope

- The security of arbitrary third-party sites you point the scraper at.
- Rate limiting or blocking by remote hosts, including the user-agent rotation behaviour,
  which exists to avoid false-positive bot blocks on public data and is not an evasion of
  authentication or authorisation.
- Denial of service caused by deliberately configuring extreme parallelism locally.

## Operational note

This project stores no credentials and requires no API keys. If you supply URLs containing
secrets (tokens in query strings, for example), those are your responsibility — they may be
written into logs, state files and reports.
