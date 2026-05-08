# Licensing

MoneyBin is licensed under [AGPL-3.0-or-later](../LICENSE). This page is the rationale.

> The README's [License](../README.md#license) section is the short pointer. This is the long answer.

## What AGPL means in practice

AGPL is a copyleft license with a specific clause about **network use**: if you run a modified version of the software as a network service, you must publish the modified source code to your users. Compared to GPL, this closes the "SaaS loophole" where companies could fork GPL software and run it as a closed-source hosted service without contributing changes back.

For users, AGPL guarantees:

- **Free use.** You can install, run, and modify MoneyBin for any purpose, including commercial use, personal use, and within your organization.
- **Free fork.** You can fork the project on GitHub, change anything, redistribute it.
- **Source-availability when run as a service.** If anyone — including us — runs a modified MoneyBin as a network service, the modified source code must be published. The hosted MoneyBin tier (M3E) runs the same AGPL code anyone can self-host.

For closed-source competitors, AGPL means:

- **They can't trivially clone MoneyBin's hosted experience without contributing back.** A VC-funded MoneyBin clone running closed-source on someone else's infrastructure violates the license.
- **They can build on MoneyBin internally** for their own non-network use without the source-availability obligation. AGPL only triggers on network distribution.

## Why we chose AGPL specifically

Three reasons, in order of weight.

### 1. AGPL is the moat for the OSS-with-hosted-tier business model

The hosted tier (M3E) is part of MoneyBin's product. AGPL ensures that running the same hosted experience requires either using our hosted tier *or* publishing your own modifications. This is the "Bitwarden play": OSS + hosted, where the hosted is convenience and the OSS is the long-term portability guarantee.

Same model used by:

- **[Bitwarden](https://bitwarden.com/)** — password manager, AGPL server with hosted + self-host parity (Vaultwarden is the unofficial reimplementation).
- **[Plausible](https://plausible.io/)** — analytics, AGPL with the explicit "you can self-host but we'll happily run it for you" framing.
- **[Element / Matrix](https://element.io/)** — chat, AGPL server (Synapse).
- **[Sentry](https://sentry.io/)** — error tracking, FSL/BSL with AGPL-equivalent intent (the license shifted in 2024, but the philosophy is the same).
- **[Ghost](https://ghost.org/)** — publishing, MIT-but-trademarked with AGPL-equivalent operational discipline.

The license is structurally aligned with the business model. It's a feature, not a tax.

### 2. AGPL signals that ownership matters

The MoneyBin audience self-selects toward "I want my data, my code, my ability to walk away" (see [Audience](audience.md) for the segment breakdown). AGPL signals this without saying it. Every user who reads the LICENSE file gets the same "this project actively chose protection over permissiveness" message.

A more permissive license (MIT, Apache, BSD) would attract more downstream commercial reuse, but our audience isn't optimizing for that. They're optimizing for "the project survives independent of the maintainer" and "the hosted tier can't quietly become a closed-source cash cow."

### 3. AGPL leaves the door open to commercial dual-licensing

If a future commercial partner needs a non-AGPL license for their integration, AGPL allows dual-licensing as long as we have a Contributor License Agreement (CLA) covering all contributions. We don't have a CLA today (and don't need one pre-launch), but the path exists.

A more restrictive non-OSS license (BSL, FSL, ELv2) would foreclose this option without buying us much defensive value the AGPL doesn't already provide.

## What AGPL doesn't do

To keep the framing honest:

- **AGPL doesn't prevent competition.** Anyone can build a competing personal-finance MCP server from scratch under any license they like. AGPL only governs derivatives of MoneyBin's code.
- **AGPL doesn't generate revenue automatically.** It's compatible with the OSS+hosted model but doesn't make hosted profitable on its own; that requires actual hosted operations and pricing discipline.
- **AGPL doesn't include patent grants.** Apache 2.0 and similar licenses include explicit patent-grant clauses; AGPL doesn't. If patent exposure becomes a real concern, that's a separate license discussion. Today it isn't a concern.
- **AGPL doesn't make MoneyBin "more secure."** Open source code has both more potential attackers (everyone can read it) and more potential defenders (everyone can audit it). Whether the net effect is more or less secure depends on how seriously the project takes security review (we take it very seriously; see [`SECURITY.md`](../SECURITY.md) and [Threat Model](guides/threat-model.md)).

## Frequently misunderstood AGPL implications

**"AGPL means I can't use MoneyBin in my for-profit company."**
Wrong. You can run MoneyBin internally — for your finance team, your accountant, whatever — with no obligation to publish source. AGPL only triggers when *you offer the modified software as a network service to others.*

**"If I fork MoneyBin to fix a bug for my own use, I have to publish my fork."**
Wrong. You only have to publish modifications if you run the modified version as a network service that other users connect to. Self-host with local modifications? No obligation.

**"AGPL is incompatible with anything proprietary."**
Wrong, with caveats. AGPL is incompatible with linking proprietary code into the same binary in some interpretations. But running MoneyBin alongside proprietary tools, or calling MoneyBin's CLI from a proprietary script, doesn't trigger AGPL on the proprietary code.

**"AGPL kills VC interest."**
Mostly true, and intentional. Bitwarden, Plausible, Element, and Sentry are all OSS+hosted projects that succeeded without traditional VC paths (Bitwarden eventually took on capital after the model was proven). The license doesn't preclude funding; it does preclude certain kinds of investor expectations around exit-via-acquisition by a closed-source acquirer.

If MoneyBin's strategy ever shifts toward a venture path, the license discussion would reopen. Today it doesn't, and AGPL is structurally aligned with the indie-OSS-with-hosted-tier path.

## Trademark

"MoneyBin" is the project's name and brand identity. Trademark policy (when one is needed) follows the same model as Bitwarden's and Plausible's: the source code is open under AGPL; the trademark protects against confusing use of the name in derivative or competing products. This isn't formalized today — when it becomes relevant (likely around M3E launch), a `TRADEMARK.md` will land alongside this page.

## Reporting a license violation

If you believe a third party is violating MoneyBin's AGPL license — running a modified version as a hosted service without publishing source — please open a confidential issue per the [security policy](../SECURITY.md) or email the maintainer directly. Public license-enforcement actions are rare; we'd rather have a conversation first.
