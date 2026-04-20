# SkeinDB — Commercial Options

Last updated: 2026-04-19.

SkeinDB is, and will stay, free and open-source software under **Apache-2.0**
(see [`LICENSE`](LICENSE)). You do not need any commercial relationship to use
SkeinDB in production, embed it in your product, or modify it for internal use.

This document exists for organizations who want **more than the open-source
default**: a predictable support channel, priority on the roadmap, or a paid
hosted/enterprise tier.

---

## TL;DR

| Option                        | Target audience                         | Status                |
|-------------------------------|-----------------------------------------|-----------------------|
| GitHub Sponsors               | Individuals, hobby users, small teams   | **Available**         |
| PayPal                        | One-off donations                       | **Available**         |
| Starter / Business / Enterprise support | Teams running SkeinDB in production | Indicative pricing published |
| Training & consulting         | Teams adopting SkeinDB / SkeinQL        | Available on request  |
| Named research-track sponsor  | Companies funding a specific track      | Available on request  |
| Hosted / managed SkeinDB      | Teams who want "just give me a DB URL"  | After 1.0             |
| Commercial add-on modules     | Enterprise SSO/SAML, advanced RBAC, etc.| After 1.0             |

Indicative monthly pricing is published on the website at
[`site/pricing.html`](site/pricing.html): **Starter €299 / month**,
**Business €1,200 / month**, **Enterprise €3,900 / month**. Final scope,
taxes, billing terms, and any 24×7 coverage are still agreed case-by-case.

---

## 1. Sponsor SkeinDB

Recurring sponsorship is by far the lowest-friction way to support the project.
Even small monthly amounts make roadmap planning much more predictable.

- **GitHub Sponsors:** <https://github.com/sponsors/pinkysworld>
- **PayPal:** <https://www.paypal.com/paypalme/mippinky>
  (or send directly to `mip@gmx.biz`)

Suggested tiers (informational — GitHub Sponsors tiers may vary):

- **$5 / month — Supporter.** Thank-you mention in release notes.
- **$25 / month — Backer.** Supporter + "Backers" listing in the README.
- **$100 / month — Sustainer.** Backer + monthly email with roadmap preview.
- **$500 / month — Corporate sponsor.** Sustainer + logo on the website
  (`Roadmap` / `Sponsors` sections) + a direct email channel for questions.

These tiers are **not** a support contract. See §2 below if you need an SLA.

---

## 2. Commercial support contracts

For organizations running SkeinDB in production we offer paid support with
defined response times. Everything in the support contract is scoped around
the open-source SkeinDB — no code is held back.

| Tier       | Indicative monthly price | Response time (business hours) | Channels              | Scope                                                               |
|------------|--------------------------|-------------------------------|-----------------------|---------------------------------------------------------------------|
| Starter    | €299 / month             | Next business day              | Email                 | Questions, config review, bug triage on the open-source tree.       |
| Business   | €1,200 / month           | Same business day              | Email + shared chat   | Starter + priority triage on reported bugs + quarterly roadmap call. |
| Enterprise | €3,900 / month           | 4 business hours               | Email + chat + video  | Business + named engineer + migration/design reviews.               |

Custom / 24×7 engagements are available separately where pager coverage,
on-site work, or private-fork maintenance are actually needed.

Not included (contact us for a separate engagement):

- On-call / 24×7 pager rotation.
- Guaranteed upstream patch turnaround (we'll prioritize, not promise a date).
- Operations of your deployment — we advise, we do not run your servers
  (that changes once hosted SkeinDB ships; see §5).

To open a commercial conversation, file a [Commercial inquiry issue][ci]
or email the repository owner. We'll reply with a short scoping questionnaire
and an indicative quote.

[ci]: https://github.com/pinkysworld/SkeinDB/issues/new?labels=commercial&title=Commercial%20inquiry

---

## 3. Training & consulting

Available as time-boxed engagements:

- **SkeinDB internals workshop** (1–2 days, remote or on-site): storage
  layout, MVCC, WAL, recovery, compaction, research tracks.
- **SkeinQL for backend engineers** (half day): native HTTP API, ETag
  validators, query coalescing patterns.
- **Architecture review** (2–5 days): evaluate whether SkeinDB is a good
  fit for your workload, review schema / access patterns, produce a written
  recommendation.
- **Custom engineering** (T&M): targeted features or parity work, scoped
  per engagement; resulting code ships to the open-source main branch
  unless the customer specifically requests a private fork.

---

## 4. Named research-track sponsorships

SkeinDB has 20+ research tracks (see [`docs/RESEARCH_BACKLOG.md`][rb]). A
sponsor can fund one track end-to-end and receive attribution in:

- The relevant design doc (e.g. `docs/WASM_UDFS.md`, `docs/ETAG_VALIDATORS.md`).
- The release notes for the version that ships the feature.
- The [Roadmap page][rp] on the project website.

Candidate tracks where sponsorship would meaningfully accelerate delivery:

- Wasm UDFs (`docs/WASM_UDFS.md`) — sandbox + fuel + aggregates.
- Hybrid row/column snapshots (`docs/COLUMN_SNAPSHOTS.md`).
- Oblivious execution (`docs/OBLIVIOUS_EXECUTION.md`).
- Tamper-evident audit WAL (`docs/AUDIT_WAL.md`) — extended verifier.

[rb]: docs/RESEARCH_BACKLOG.md
[rp]: https://github.com/pinkysworld/SkeinDB/blob/main/site/roadmap.html

---

## 5. Hosted / managed SkeinDB *(after 1.0)*

Once the on-disk format is stable and the clustering track lands, we plan to
offer a hosted SkeinDB tier (single-tenant, HA, or multi-tenant dev). The
hosted service runs the same binary from the open-source tree; there is no
feature fork. Pricing model will be volume-based (storage + traffic).

---

## 6. Commercial add-on modules *(after 1.0)*

Optional modules targeted at enterprise operators. Each is shipped as a
**separate** binary/crate under a commercial EULA so the core stays
Apache-2.0.

Planned modules (subject to demand):

- Enterprise SSO / SAML / OIDC.
- Advanced RBAC (row-level policy editor, audit trail export, field
  masking policies).
- Multi-region replication UI + topology manager.
- Compliance bundle (SOC 2 / HIPAA / GDPR reporting templates).

Nothing in the current open-source tree is moved into paid modules;
add-ons are built on top.

---

## 7. What will never happen

We think these practices erode trust in database software. The maintainers
commit to the following even if commercial offerings become significant:

- **No retroactive relicensing of the core.** The core stays Apache-2.0.
- **No paywalled bug fixes.** Correctness lives in the open-source tree.
- **No default telemetry.** SkeinDB ships with no "phone home" by default.
  Any telemetry is opt-in, documented, and inspectable in the source tree.
- **No forced upgrades.** You can run an older open-source version
  indefinitely; we won't break that.

---

## 8. How to reach us

- [Commercial inquiry issue][ci]
- Direct email: `mip@gmx.biz`
- GitHub Sponsors: <https://github.com/sponsors/pinkysworld>

When you open a commercial inquiry, including the following speeds up the
reply:

1. Short description of your workload (MySQL compat / SkeinQL / mixed).
2. Deployment target (single node / HA / multi-region).
3. Rough scale (TPS, storage, number of tables).
4. Which tier or engagement you are considering.
