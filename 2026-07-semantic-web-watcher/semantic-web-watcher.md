# Semantic web watcher

Personal tool that replaces Google Alerts: watch the web for **newly appeared
pages that semantically match a natural-language interest** (first use case:
new developments of houses around Sofia — "комплекс от къщи"), judged by an
LLM instead of keyword matching. Runs daily via cron on the host PC, presents
confirmed matches in the terminal on demand.

**Status: design settled (decisions 1–14) after the 2026-07-14 buffered-pipeline
session; repo created at `~/src/semantic-web-watcher`, no code yet. Next: M0.**

## Problem statement

Google Alerts for `комплес от къщи` (sic) produced 6 sample notifications
(`fixtures/google-alerts-sample.txt`); analysis showed **zero** matched the
actual intent. Failure mode: bag-of-words matching anywhere in the document,
no adjacency, no document-intent classification, News-index bias (yellow
press dominates; developer sites / portals / forums underrepresented). The
intent — "a page announcing a new residential development of houses for sale"
— is a semantic category keyword alerts cannot express.

## Core reframe

Don't solve "feed of all new pages on the internet." Use **many cheap, noisy,
recall-oriented candidate sources** and an **LLM as the precision filter**.
A 5%-precision candidate stream is fine when classification is near-free.

## Key decisions (with rationale)

1. **Generic, search-engine-only discovery; no hardcoded vertical sites.**
   Losses accepted: latency (days vs hours), deliberately-unindexed listing
   inventory (~half for inventory-shaped topics), pre-web signals (permit
   registers). Clawbacks that keep the product generic: multi-engine union
   (Google/Bing/Brave/DDG; Yandex relevant for Cyrillic), deep pagination +
   LLM query expansion, and **learned source escalation** — domains that
   repeatedly produce confirmed matches get dedicated `site:` queries, then
   sitemap/RSS polling. Config is learned per subscription, not shipped.

2. **Per-site parsing is a solved problem; access is the only per-site cost.**
   Crawler = 3 subproblems: URL discovery (standardized: sitemaps, RSS,
   listing-page diff), parsing (generic: readability-strip + LLM — no
   per-site selectors), access (a ladder: default fetch → browser UA →
   headless browser → commercial fetch API → true login walls = drop or
   deliberate integration). Litmus test, automatable: **if a search engine
   shows content snippets, the site serves anonymous crawlers.**
   Verified empirically on bg-mamma (`fixtures/bgmamma-topic-1102272.cp1251.html`):
   assumed reg-walled, actually public — 403 was UA-gating (browser UA → 200),
   content is windows-1251. Both obstacles generic, ~1 minute to defeat.

3. **Host fetches + extracts; the LLM judges clean text.** Never send bare
   URLs for server-side fetching (loses fetch control, breaks content-hash
   dedup/verdict caching, no token savings — option A rejected). Extraction
   via trafilatura: bg-mamma page is 257KB raw (~70–90K tokens) vs ~3–4K
   tokens stripped ⇒ ~20× cheaper and better signal. LLM calls are pure
   functions `(text, intent) → verdict`.

4. **Billing: Claude subscription route (user decision — no per-token API).**
   Facts verified 2026-07-13: Messages API is strictly pay-per-token, never
   subscription-billable. Supported subscription automation = **Claude Code
   headless**: `claude setup-token` → `CLAUDE_CODE_OAUTH_TOKEN` in cron env →
   `claude -p ... --output-format json`. Agent SDK + subscription auth is
   contradictory/gray (support article says SDK draws from plan limits;
   GitHub issue #42106 for personal OAuth use closed "not planned") — do not
   build on it. Keep `judge.py` as a backend seam (`SubscriptionBackend` via
   subprocess now; `APIBackend` with `messages.parse()` + Batches later if
   quota becomes the constraint).

5. **Batch to amortize harness overhead** (~10–20K tokens/invocation):
   - *Triage:* ONE prompt-batched call/day — all ~200 title+snippet
     candidates as JSONL in, JSONL verdicts out (truncation-tolerant).
   - *Deep-check:* ONE agentic session over a run directory —
     `intent.md` + `inbox/<contenthash>.txt` files; agent appends to
     `verdicts.jsonl` incrementally, skipping already-verdicted files ⇒
     crash-resumable. Cap ~20–40 docs/session ("lost in the middle");
     generous subprocess timeout, no --max-turns limit on the agentic run.
   - Per-doc invocation is the only wrong answer (~80% overhead).

6. **Verdict = boolean decision + metadata**, never a bare boolean:
   `{id, match, confidence, reason, published_date, location, project_name,
   page_type}`.
   `reason` is the prompt-tuning feedback channel (the thing Google Alerts
   never gave); `published_date` feeds new-to-me vs new-to-web filtering;
   location/project feed clustering and digest readability; `page_type`
   (leaf|hub) marks listing/index pages — costs one prompt line, seeds hub
   handling (decision 14) and learned source promotion (decision 1).

7. **Architecture — `main.py` (cron) + `digest` (interactive), SQLite truth:**

   ```
   main.py:  1 crawl → 2 seen-check → 3 triage (claude -p, batched)
             → 4 prepare /tmp run dir (fetch+extract survivors)
             → 5 deep-judge (claude -p agentic) → 6 ingest verdicts to DB
   digest:   print matches WHERE presented_at IS NULL (from DB, never /tmp);
             mark presented_at on explicit ack only
   ```

   The numbered steps are DATAFLOW order, not a synchronous handoff — each
   is an independent stage buffered through SQLite (decision 11).

   **Two timestamps, one writer each:** `judged_at` set by the run for EVERY
   doc as verdicts arrive (gates reprocessing — a 'no' is final at judgment
   time); `presented_at` set only by digest ack for 'yes' docs. Conflating
   them ⇒ unread digests cause nightly re-judging + crash-window duplicates.
   Run dir is a disposable projection materialized on demand from DB + blob
   store (decision 12) — rebuildable at any moment, so it needs no lifecycle
   rules of its own. [Supersedes: "disposable only after ingest / keep run
   N-1 for postmortems" — postmortems query the DB and `store/` instead.]

8. **Seen-store (set-difference model, skip-tolerant by construction):**
   key = canonicalized URL (strip utm/fbclid, fold m. hosts + pagination,
   honor rel=canonical). Seen gates *re-insertion*, not *processing*: what
   lives forever is that the URL was considered (`INSERT OR IGNORE` on
   re-discovery); a discovered-but-unjudged row keeps matching its pending
   predicate across runs until terminal (decision 11). Content identity =
   hash of *extracted text* — cross-URL dedup only (syndication, mirrors the
   canonicalizer missed), NEVER a re-judge trigger (decision 14; conditional
   GETs via ETag/Last-Modified stay); story identity = cluster by
   entity/embedding, notify per cluster. Search
   engines get no cursor — freshness window widens to cover gaps
   (skipped 3 days → "past week"); sitemap `lastmod` is a hint, hash is the
   arbiter. First run = silent seed mode. `published_at` (page's own,
   LLM-extracted) ≠ `first_seen_at` (ours) — notify on "first-seen now AND
   published recently or unknown".

9. **TDD, small units over integration** (user requirement). Every phase
   takes dependencies as arguments; fakes at the seams. Pyramid:
   ~40 pure unit tests (extraction fixtures incl. the cp1251 bg-mamma page;
   canonicalization table; verdict JSONL parsing/missing-id/malformed;
   in-memory-SQLite seen logic; digest render-is-read-only + explicit-mark) /
   ~5 plumbing tests (stub `claude` executable on PATH emitting fixture
   JSONL — tests subprocess+timeout+crash paths at zero quota; httpx
   MockTransport with a 403→200 script for the UA-escalation ladder) /
   ~3 manual LLM eval runs (`pytest -m llm`) — a labeled set seeded from the
   Google Alerts sample (scooter fire = no, museum lecture = no) that doubles
   as the **prompt-regression harness**: every production misjudgment becomes
   a labeled example; rerun before trusting any `intent.md` edit. Assert
   booleans only on clear-cut cases. One smoke test wires all fakes through
   main.py.

10. **Local pre-filter: only ever allowed to say "obviously junk".**
    Asymmetric risk — its false negatives are invisible ⇒ recall-safe
    thresholds + a **shadow audit** (each run, send 5–10 random rejects to
    Claude; any 'yes' = recall leak → loosen + add to eval set).
    Tier 1 (v1, free): seen-dedup, language detection, URL-shape rules
    (category/tag/search pages), learned domain priors. Tier 2 (v1.5):
    multilingual sentence embeddings (e.g. multilingual-e5-small, CPU) —
    similarity-to-intent floor; same vectors reused for story clustering
    (BLOB column). Tier 3 (local LLM): rejected — heavy, weak Bulgarian,
    silent recall losses. No keyword-content filtering ever (that's
    rebuilding Google Alerts).

11. **Buffered pipeline — stages communicate ONLY through SQLite**
    (2026-07-14). Each stage = work predicate → process → write only its
    own columns; a row's pipeline state is the NULL-pattern of stage-owned
    columns (state and data physically cannot desync — no status enum, no
    location-as-state). Stages never hand objects in memory; `main.py`
    stays a plain loop calling stages in order, each a no-op when nothing
    is pending — one process, one cron entry, one flock. Parallelism, if
    ever needed, goes *inside* a stage; anything queue-shaped beyond this
    (workers, brokers) = design drift. Batching is internal to a stage: a
    truncated triage response leaves unanswered rows pending → picked up
    next run, zero recovery code; crash anywhere = resume by construction.
    Discipline that keeps it legible: ALL lifecycle predicates are named
    functions in db.py (`pending_triage()`, `pending_fetch()`,
    `pending_judge()`, `unpresented_matches()`…) — no other module writes
    WHERE clauses over lifecycle columns. db.py IS the state machine;
    its unit tests are the state-machine tests.

12. **Extracted text = immutable content-addressed blob files; DB is the
    sole state authority.** `store/<contenthash>.txt`, written once by
    extract, never moved or renamed. Files-not-DB because deep-judge
    consumes files anyway (agentic `claude -p` over a run dir — DB blobs
    would round-trip). Directory-moves-as-state (pending-llm/ →
    pending-user/) rejected: a file rename and a SQLite transaction can
    never be atomic together, so a crash between them forks two truths —
    the same bug class decision 7's one-writer rule kills. Deletion is a
    GC pass over DB-terminal rows, not a state transition (a missed GC =
    stale kilobytes, never wrong state); keep judged text ≥30 days so
    misjudgments can be harvested into the eval set (decision 9).

13. **Failure taxonomy — two families, opposite treatments.**
    *Item-scoped / external* (fetch+extract of one URL): expected, routine;
    retry ≤N nights (`fetch_attempts`, `last_error` columns; the work
    predicate gains `fetch_attempts < N` so pending sets always DRAIN —
    transient outages self-heal, dead URLs exit), then terminal give-up,
    surfaced in the digest as a manual-review line (informative: the
    decision-2 litmus says indexed ⇒ fetchable, so repeated failure = the
    access ladder needs a rung). *Run-scoped / internal* (search-API auth,
    `claude` subprocess, DB errors): unexpected, systemic; halt loudly with
    nonzero exit — untouched rows stay pending and the next successful run
    resumes everything, so failure recovery is no code. One malformed
    verdict record among 200 is item-scoped (skip; row stays pending),
    never a halt. Digest prints a health header ("last successful run:
    <date>") so a silently stalled cron is visible where the user already
    looks.

14. **Identity & re-judging — leaf/hub split; no re-judging on the generic
    web** (2026-07-14; supersedes decision 8's "re-judge on change").
    v1 identity is URL-level: judged once, forever — for announcement-shaped
    intents the event is the page *appearing*, not mutating. Pages have two
    roles: *leaf* (the announcement itself; stable content) and *hub*
    (stable-URL listing page with mutating content, e.g.
    builderX.com/new_properties). A hub's change signal is never its bytes —
    it is NEW OUTBOUND LINKS, consumed as ordinary candidate URLs through
    the seen-store's set-difference: cosmetic churn ⇒ zero new links ⇒ zero
    LLM calls. (Content-hash re-judging rejected as unworkably noisy for
    the generic web — any footer tweak invalidates.) The judge marks hubs
    via the `page_type` verdict field (decision 6), which also gives source
    promotion (decision 1) a data trail. Watched-source polling with
    link-set diff lands M3/M4; extracted-text-hash re-judge is permitted
    ONLY for the small explicit watched set, and only if inline-listing
    hubs (items without per-item URLs) show up in practice.

## Stack

Python 3.12 + uv · SQLite (stdlib) · httpx + charset-normalizer · trafilatura
· Brave Search API (primary; free tier, freshness param) + ddgs behind a
`SearchSource` protocol · `claude -p` subprocess (subscription) · typer CLI ·
`interests.toml` config · cron with `flock -n`; prefer anacron (desktop may be
off — pipeline is skip-tolerant, anacron just narrows catch-up windows).
Dependency budget ~6 packages; a heavy framework appearing = design drift.

## Milestones

- **M0 — scaffold + spec-by-test:** repo layout (`cli.py db.py sources/
  fetch.py extract.py judge.py cluster.py`), schema
  (`pages(url_canon PK, first_seen_at, title, snippet, triage_verdict,
  triaged_at, last_fetched_at, etag, fetch_attempts, last_error,
  content_hash, published_at, verdict…, page_type, judged_at, cluster_id)`,
  `cursors`, `clusters` with `presented_at`), `store/` blob dir (decision
  12), lifecycle predicates as named db.py functions (decision 11),
  fixtures committed, unit suites for extract / canonicalize / db / digest
  written first.
- **M1 — pipeline vertical slice:** Brave source + query expansion, fetch
  ladder (UA escalation), trafilatura extract, triage + deep-judge via
  `claude -p` (stub-tested), ingest. Seed mode. First real run against the
  property intent.
- **M2 — operations:** digest command, cron/anacron entry, run-dir lifecycle,
  LLM eval suite seeded and passing, shadow-audit hook (even before Tier 2).
- **M3 — quality:** Tier-2 embedding pre-filter + threshold calibration,
  story clustering in digest, learned domain priors.
- **M4 — reach:** learned `site:` escalation → sitemap/RSS polling of hot
  domains, second engine (ddgs/Yandex), second interest to prove genericity.

## Open questions

- Batch-size sweet spot for the deep-judge session (start 20–40, measure).
- Whether Pro-tier weekly caps accommodate ~200 triage + ~20 deep docs/day
  alongside interactive use (Max almost certainly does) — measure in M1;
  fallback is the `APIBackend` seam.
- Digest UX: plain stdout table vs TUI — decide when it exists.

## Session log

### 2026-07-13 — brainstorming session (Claude Code)

Analyzed the Google Alerts sample (0/6 relevant; failure mode identified).
Live experiment: bg-mamma thread fetch — search-snippet litmus predicted
public readability; confirmed (403 = UA-gating, cp1251 encoding; both defeated
generically). Verified subscription-billing facts via docs (claude -p headless
supported incl. `setup-token`; Agent SDK gray — issue #42106 closed
"not planned"; Messages API never subscription-billed). Settled decisions 1–10
above. Fixtures captured. Next: M0.

### 2026-07-14 — design session #2 (Claude Code): buffered pipeline

Repo created (`~/src/semantic-web-watcher`, empty). User's driving idea:
stages independent, buffered through SQLite, never implied
sequential-synchronous — for testable contracts and future parallelism.
Settled decisions 11–14: NULL-pattern state with all lifecycle predicates
centralized in db.py; extracted text as immutable content-addressed files
with DB-driven GC (user's files-over-DB-blobs instinct adopted;
directory-moves-as-state rejected — rename + DB write can't be atomic, and
the durable "considered" record must be a DB row regardless since files
die on 'no'); failure taxonomy (item-scoped external → bounded retries then
digest-surfaced give-up so pending sets drain; run-scoped internal → loud
halt, resume-by-construction); re-judge-on-content-change dropped for the
generic web in favor of the leaf/hub split — a hub's change signal is new
outbound links through seen-store set-difference, immune to cosmetic churn
(content-hash triggers judged unworkably noisy). `page_type` added to the
verdict schema. Amended decisions 6, 7, 8 and M0 accordingly. Next: M0.
