#!/usr/bin/env python3
"""Generate themed HTML documentation from docs/*.md into docs/site/guide/.

This script mirrors the site's dark/indigo theme (see site/index.html) and adds
a left sidebar with grouped navigation, a per-page TOC, and a top search bar.
Pages are organized into sections: Tutorials, User Guide, Admin Guide,
Developer / API, Internals & Architecture, and Research.

Run: python3 scripts/build_docs_site.py
"""
from __future__ import annotations

import html
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import markdown
from markdown.extensions.toc import TocExtension

ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT / "docs"
OUT_DIR = ROOT / "docs" / "site" / "guide"
TUTORIALS_DIR = ROOT / "docs" / "tutorials"  # hand-written tutorial .md

# ---------------------------------------------------------------------------
# Navigation layout — edit this to change the sidebar grouping / ordering.
# Each entry is (slug, source_md_path_relative_to_docs, title).
# ---------------------------------------------------------------------------

@dataclass
class DocPage:
    slug: str
    src: str            # path relative to docs/
    title: str
    section: str
    order: int = 0

@dataclass
class Section:
    title: str
    icon: str           # small unicode or SVG-safe emoji
    pages: list[DocPage] = field(default_factory=list)


NAV: list[Section] = [
    Section("Tutorials", "🚀", [
        DocPage("index",                  "tutorials/index.md",                "Documentation home",           "Tutorials"),
        DocPage("quickstart",             "tutorials/quickstart.md",           "Quickstart — 5 minutes",       "Tutorials"),
        DocPage("first-query",            "tutorials/first-query.md",          "Your first query (SkeinQL)",   "Tutorials"),
        DocPage("mysql-in-5-min",         "tutorials/mysql-in-5-min.md",       "MySQL in 5 minutes",           "Tutorials"),
      DocPage("postgresql-in-5-min",    "tutorials/postgresql-in-5-min.md",  "PostgreSQL in 5 minutes",      "Tutorials"),
        DocPage("admin-tour",             "tutorials/admin-tour.md",           "Admin console tour",           "Tutorials"),
      DocPage("monitoring-and-metrics", "tutorials/monitoring-and-metrics.md","Monitoring and metrics",       "Tutorials"),
      DocPage("vector-rag",             "tutorials/vector-rag.md",           "Vector RAG retrieval",         "Tutorials"),
      DocPage("cdc-with-sse",           "tutorials/cdc-with-sse.md",         "CDC with SSE",                 "Tutorials"),
      DocPage("encryption-and-key-rotation", "tutorials/encryption-and-key-rotation.md", "Encryption and key rotation", "Tutorials"),
      DocPage("replay-bundles-and-integrity", "tutorials/replay-bundles-and-integrity.md", "Replay bundles and integrity", "Tutorials"),
        DocPage("setting-up-cluster",     "tutorials/setting-up-cluster.md",   "Setting up a 3-node cluster",  "Tutorials"),
    ]),
    Section("User Guide", "📖", [
        DocPage("getting-started",        "GETTING_STARTED.md",                "Getting Started",              "User Guide"),
        DocPage("skeinql",                "SKEINQL.md",                        "SkeinQL reference",            "User Guide"),
        DocPage("mysql-compat",           "MYSQL_COMPAT.md",                   "MySQL compatibility",          "User Guide"),
        DocPage("pg-compat",              "PG_COMPAT.md",                      "PostgreSQL compatibility",     "User Guide"),
        DocPage("compatibility",          "COMPATIBILITY.md",                  "Compatibility telemetry",      "User Guide"),
        DocPage("offline-write-queue",    "OFFLINE_WRITE_QUEUE.md",            "Offline write queue",          "User Guide"),
        DocPage("query-patch",            "QUERY_PATCH.md",                    "Query patch protocol",         "User Guide"),
        DocPage("query-coalescing",       "QUERY_COALESCING.md",               "Query coalescing",             "User Guide"),
    ]),
    Section("Admin Guide", "🛠", [
        DocPage("configuration",          "CONFIGURATION.md",                  "Configuration",                "Admin Guide"),
        DocPage("skeinadmin",             "SKEINADMIN.md",                     "SkeinAdmin (web console)",     "Admin Guide"),
        DocPage("clustering",             "CLUSTERING.md",                     "Clustering",                   "Admin Guide"),
        DocPage("observability",          "OBSERVABILITY.md",                  "Observability",                "Admin Guide"),
        DocPage("performance",            "PERFORMANCE.md",                    "Performance",                  "Admin Guide"),
        DocPage("release-packaging",      "RELEASE_PACKAGING.md",              "Release packaging",            "Admin Guide"),
        DocPage("audit-wal",              "AUDIT_WAL.md",                      "Audit WAL",                    "Admin Guide"),
        DocPage("time-travel-replay",     "TIME_TRAVEL_REPLAY.md",             "Time-travel & replay",         "Admin Guide"),
        DocPage("compaction-scheduler",   "COMPACTION_SCHEDULER.md",           "Compaction scheduler",         "Admin Guide"),
        DocPage("index-advisor",          "INDEX_ADVISOR.md",                  "Index advisor",                "Admin Guide"),
        DocPage("telemetry-and-migration","TELEMETRY_AND_MIGRATION.md",        "Telemetry & migration",        "Admin Guide"),
    ]),
    Section("Developer / API", "⚡", [
      DocPage("api-reference",          "API_REFERENCE.md",                 "API reference",                "Developer / API"),
        DocPage("skeinir",                "SKEINIR.md",                        "SkeinIR",                      "Developer / API"),
        DocPage("skeinql-research",       "SKEINQL_RESEARCH_EXTENSIONS.md",    "SkeinQL research extensions",  "Developer / API"),
        DocPage("wasm-udfs",              "WASM_UDFS.md",                      "Wasm UDFs",                    "Developer / API"),
        DocPage("wasm-operators",         "WASM_OPERATORS.md",                 "Wasm query operators",         "Developer / API"),
        DocPage("merge-functions",        "MERGE_FUNCTIONS.md",                "Merge functions (CRDT)",       "Developer / API"),
        DocPage("cdc-changefeed",         "CDC_CHANGEFEED.md",                 "CDC changefeed",               "Developer / API"),
        DocPage("etag-validators",        "ETAG_VALIDATORS.md",                "ETag validators",              "Developer / API"),
        DocPage("autoparameterization",   "AUTOPARAMETERIZATION.md",           "Autoparameterization",         "Developer / API"),
    ]),
    Section("Internals & Architecture", "🧬", [
        DocPage("on-disk-format",         "ON_DISK_FORMAT.md",                 "On-disk format",               "Internals"),
        DocPage("delta-values",           "DELTA_VALUES.md",                   "Delta-chained values",         "Internals"),
        DocPage("column-snapshots",       "COLUMN_SNAPSHOTS.md",               "Column snapshots",             "Internals"),
        DocPage("convergent-encryption",  "CONVERGENT_ENCRYPTION.md",          "Convergent encryption",        "Internals"),
        DocPage("incremental-views",      "INCREMENTAL_VIEWS.md",              "Incremental views",            "Internals"),
        DocPage("learned-indexes",        "LEARNED_INDEXES.md",                "Learned indexes",              "Internals"),
        DocPage("oblivious-execution",    "OBLIVIOUS_EXECUTION.md",            "Oblivious execution",          "Internals"),
        DocPage("traffic-reduction",      "TRAFFIC_REDUCTION.md",              "Traffic reduction",            "Internals"),
        DocPage("transport-quic",         "TRANSPORT_QUIC.md",                 "QUIC transport",               "Internals"),
        DocPage("cas-replication",        "CAS_REPLICATION.md",                "CAS replication",              "Internals"),
    ]),
    Section("Research & Project", "📐", [
        DocPage("research-agenda",        "RESEARCH_AGENDA.md",                "Research agenda",              "Research"),
        DocPage("research-backlog",       "RESEARCH_BACKLOG.md",               "Research backlog",             "Research"),
        DocPage("true-status-matrix",     "TRUE_STATUS_MATRIX.md",             "True status matrix",           "Research"),
        DocPage("project-backlog",        "PROJECT_BACKLOG.md",                "Project backlog",              "Research"),
    ]),
]


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------

PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — SkeinDB Docs</title>
<meta name="description" content="{description}">
<link rel="icon" type="image/svg+xml" href="../favicon.svg">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="assets/docs.css">
</head>
<body>
<nav class="topnav">
  <div class="topnav-inner">
    <a class="nav-logo" href="../index.html">SkeinDB</a>
    <div class="nav-links">
      <a href="../index.html#features">Features</a>
      <a href="../index.html#architecture">Architecture</a>
      <a href="../roadmap.html">Roadmap</a>
      <a href="../pricing.html">Pricing</a>
      <a href="../licensing.html">Licensing</a>
      <a href="../contact.html">Contact</a>
      <a href="https://github.com/sponsors/pinkysworld" target="_blank" rel="noopener" class="nav-sponsor">❤ Sponsor</a>
      <a href="index.html" class="nav-docs-cta active">Docs</a>
    </div>
    <a class="nav-gh" href="https://github.com/pinkysworld/SkeinDB" target="_blank" rel="noopener">
      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>
      GitHub
    </a>
  </div>
</nav>

<div class="research-rail">
  <div class="research-rail-inner">
    <span class="research-rail-label">Research</span>
    <a href="../index.html#paper">Overview</a>
    <a href="../index.html#research">Tracks</a>
    <a href="research-agenda.html">Agenda</a>
  </div>
</div>

<div class="docs-shell">
  <aside class="sidebar" id="sidebar">
    <div class="sidebar-search">
      <input type="search" id="docsSearch" placeholder="Search docs (Ctrl+K)" autocomplete="off">
    </div>
    <nav class="sidebar-nav">
      {sidebar}
    </nav>
  </aside>

  <main class="content">
    <div class="breadcrumbs">
      <a href="index.html">Docs</a> <span>›</span> <span>{section_title}</span> <span>›</span> <span>{title}</span>
    </div>
    <article class="doc-article">
      {body}
      <hr>
      <p class="edit-link">
        <a href="https://github.com/pinkysworld/SkeinDB/edit/main/docs/{src}" target="_blank" rel="noopener">Edit this page on GitHub</a>
        &nbsp;&middot;&nbsp;
        <a href="https://github.com/pinkysworld/SkeinDB/blob/main/docs/{src}" target="_blank" rel="noopener">View source</a>
      </p>
    </article>
    {prev_next}
  </main>

  <aside class="toc" id="toc">
    <div class="toc-title">On this page</div>
    {toc}
  </aside>
</div>

<footer class="docs-footer">
  <p>SkeinDB &mdash; Apache-2.0 &mdash; <a href="../index.html">Home</a> &middot; <a href="../pricing.html">Pricing</a> &middot; <a href="../contact.html">Contact</a> &middot; <a href="https://github.com/pinkysworld/SkeinDB" target="_blank" rel="noopener">GitHub</a></p>
</footer>

<script src="assets/docs.js"></script>
</body>
</html>
"""


CSS = """/* SkeinDB docs — dark/indigo, matches site theme */
:root{--bg:#09090b;--bg-card:#18181b;--bg-card-hover:#1f1f23;--border:#27272a;--border-accent:#3f3f46;--text:#fafafa;--text-secondary:#a1a1aa;--text-dim:#71717a;--accent:#6366f1;--accent-light:#818cf8;--green:#10b981;--amber:#f59e0b;--cyan:#06b6d4;--rose:#f43f5e;--purple:#a855f7;--radius:10px;--radius-sm:6px}
*{margin:0;padding:0;box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:72px}
body{font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);line-height:1.7;-webkit-font-smoothing:antialiased}
a{color:var(--accent-light);text-decoration:none}
a:hover{color:var(--cyan)}
code{font-family:'JetBrains Mono',monospace;font-size:.88em;background:#111;border:1px solid var(--border);padding:.1em .4em;border-radius:5px}
pre{background:#0c0c0f;border:1px solid var(--border);border-radius:var(--radius);padding:1rem 1.25rem;overflow:auto;margin:1rem 0;font-size:.85rem;line-height:1.55}
pre code{background:transparent;border:none;padding:0}
img{max-width:100%;border:1px solid var(--border);border-radius:var(--radius);margin:1rem 0}
hr{border:none;border-top:1px solid var(--border);margin:2rem 0}

/* ---- top nav ---- */
.topnav{position:sticky;top:0;z-index:100;background:rgba(9,9,11,.9);backdrop-filter:blur(16px);border-bottom:1px solid var(--border);padding:0 2rem}
.topnav-inner{max-width:1600px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;height:56px;gap:1rem}
.nav-logo{font-size:1.1rem;font-weight:800;letter-spacing:-.02em;background:linear-gradient(135deg,var(--accent-light),var(--cyan));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;color:transparent;text-decoration:none}
.nav-links{display:flex;gap:1.4rem;align-items:center;flex-wrap:wrap}
.nav-links a{color:var(--text-secondary);font-size:.88rem;font-weight:500;transition:color .2s}
.nav-links a:hover,.nav-links a.active{color:var(--text)}
.nav-links a.nav-sponsor{color:#f472b6}
.nav-links a.nav-sponsor:hover{color:#f9a8d4}
.nav-links a.nav-docs-cta{color:var(--accent-light);font-weight:600;padding:4px 12px;border-radius:6px;background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2)}
.nav-links a.nav-docs-cta:hover,.nav-links a.nav-docs-cta.active{background:rgba(99,102,241,.15);border-color:rgba(99,102,241,.4);color:var(--accent-light)}
.nav-gh{display:inline-flex;align-items:center;gap:6px;background:var(--bg-card);border:1px solid var(--border);padding:6px 14px;border-radius:20px;color:var(--text);font-size:.82rem;font-weight:500}
.nav-gh svg{width:16px;height:16px;fill:currentColor}
.research-rail{border-bottom:1px solid var(--border);background:rgba(9,9,11,.72)}
.research-rail-inner{max-width:1600px;margin:0 auto;padding:.55rem 2rem;display:flex;align-items:center;gap:.6rem;flex-wrap:wrap}
.research-rail-label{font-size:.72rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--text-dim);margin-right:.25rem}
.research-rail a{color:var(--text-secondary);font-size:.82rem;font-weight:500;padding:4px 10px;border-radius:999px;border:1px solid transparent;transition:color .2s,background .2s,border-color .2s}
.research-rail a:hover,.research-rail a.active{color:var(--text);background:rgba(99,102,241,.08);border-color:rgba(99,102,241,.18)}

@media(max-width:980px){.topnav{padding:0 1rem}.topnav-inner{height:auto;padding:.8rem 0;flex-wrap:wrap}.nav-links{width:100%;order:3;gap:1rem}.research-rail-inner{padding:.55rem 1rem}}

/* ---- shell ---- */
.docs-shell{display:grid;grid-template-columns:260px minmax(0,1fr) 220px;gap:2rem;max-width:1600px;margin:0 auto;padding:2rem 1.5rem 4rem}
@media(max-width:1280px){.docs-shell{grid-template-columns:240px minmax(0,1fr)}.toc{display:none}}
@media(max-width:900px){.docs-shell{grid-template-columns:1fr;padding:1rem}.sidebar{position:static;max-height:none}}

/* ---- sidebar ---- */
.sidebar{position:sticky;top:72px;align-self:start;max-height:calc(100vh - 80px);overflow-y:auto;padding-right:.5rem}
.sidebar-search input{width:100%;background:#0c0c0f;border:1px solid var(--border);border-radius:var(--radius-sm);padding:.55rem .75rem;color:var(--text);font-size:.88rem;font-family:inherit;outline:none;transition:border-color .2s}
.sidebar-search input:focus{border-color:var(--accent)}
.sidebar-nav{margin-top:1rem}
.sidebar-nav .group-title{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-dim);margin:1.25rem 0 .5rem}
.sidebar-nav .group-title:first-child{margin-top:0}
.sidebar-nav ul{list-style:none;margin:0;padding:0}
.sidebar-nav li a{display:block;padding:.35rem .6rem;border-radius:6px;color:var(--text-secondary);font-size:.89rem;line-height:1.35}
.sidebar-nav li a:hover{color:var(--text);background:rgba(99,102,241,.06)}
.sidebar-nav li a.active{color:var(--text);background:rgba(99,102,241,.15);border-left:2px solid var(--accent-light);padding-left:.5rem}
.sidebar-nav li.hidden{display:none}

/* ---- content ---- */
.breadcrumbs{font-size:.8rem;color:var(--text-dim);margin-bottom:1rem}
.breadcrumbs a{color:var(--text-secondary)}
.breadcrumbs span{padding:0 .25rem}
.doc-article h1{font-size:clamp(1.8rem,3.5vw,2.4rem);font-weight:800;letter-spacing:-.025em;margin:.25rem 0 1.25rem}
.doc-article h2{font-size:1.4rem;font-weight:700;letter-spacing:-.015em;margin:2rem 0 .75rem;padding-top:.75rem;border-top:1px solid var(--border);scroll-margin-top:72px}
.doc-article h2:first-of-type{border-top:none;padding-top:0}
.doc-article h3{font-size:1.1rem;font-weight:600;margin:1.5rem 0 .5rem;scroll-margin-top:72px}
.doc-article h4{font-size:.98rem;font-weight:600;margin:1rem 0 .4rem;color:var(--text)}
.doc-article p{color:var(--text-secondary);margin:.6rem 0}
.doc-article ul,.doc-article ol{margin:.5rem 0 .75rem 1.4rem;color:var(--text-secondary)}
.doc-article li{margin:.25rem 0}
.doc-article blockquote{border-left:3px solid var(--accent);background:rgba(99,102,241,.06);padding:.8rem 1rem;border-radius:0 var(--radius) var(--radius) 0;margin:1rem 0;color:var(--text)}
.doc-article table{width:100%;border-collapse:collapse;margin:1rem 0;background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;font-size:.9rem}
.doc-article th,.doc-article td{text-align:left;padding:.55rem .85rem;border-bottom:1px solid var(--border);color:var(--text-secondary)}
.doc-article th{background:#111;color:var(--text);font-weight:600}
.doc-article tr:last-child td{border-bottom:none}
.doc-article a h2, .doc-article a h3{color:inherit}
.doc-article .headerlink{color:var(--text-dim);margin-left:.4em;text-decoration:none;opacity:0;transition:opacity .15s}
.doc-article h2:hover .headerlink,.doc-article h3:hover .headerlink{opacity:1}

/* callouts */
.doc-article .note,.doc-article .admonition{background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.3);border-radius:var(--radius);padding:.9rem 1.1rem;margin:1rem 0;color:var(--text)}

.edit-link{font-size:.82rem;color:var(--text-dim);margin-top:1rem}

/* ---- toc ---- */
.toc{position:sticky;top:72px;align-self:start;max-height:calc(100vh - 80px);overflow-y:auto;font-size:.82rem}
.toc-title{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-dim);margin-bottom:.6rem}
.toc ul{list-style:none;padding-left:0;margin:0;border-left:1px solid var(--border)}
.toc ul ul{padding-left:.85rem;margin:.15rem 0}
.toc a{display:block;padding:.25rem .7rem;color:var(--text-secondary);border-left:2px solid transparent;margin-left:-1px}
.toc a:hover{color:var(--text)}
.toc a.active{color:var(--accent-light);border-left-color:var(--accent-light)}

/* ---- prev/next ---- */
.prev-next{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:3rem}
@media(max-width:680px){.prev-next{grid-template-columns:1fr}}
.prev-next a{display:block;padding:1rem 1.25rem;background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);color:var(--text-secondary);transition:border-color .2s,color .2s}
.prev-next a:hover{border-color:var(--accent);color:var(--text)}
.prev-next a span{display:block;font-size:.72rem;color:var(--text-dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:.2rem}
.prev-next a.next{text-align:right}

/* ---- home/index grid ---- */
.docs-home-hero{padding:1.5rem 0 2rem;border-bottom:1px solid var(--border);margin-bottom:2rem}
.docs-home-hero h1{font-size:clamp(1.8rem,3.5vw,2.4rem);font-weight:800;letter-spacing:-.025em}
.docs-home-hero p{color:var(--text-secondary);max-width:70ch;margin-top:.75rem;font-size:1.02rem}
.home-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:1rem;margin:1.5rem 0}
.home-card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);padding:1.1rem 1.25rem;transition:border-color .2s,background .2s}
.home-card:hover{border-color:var(--accent);background:var(--bg-card-hover)}
.home-card h3{font-size:1rem;font-weight:600;margin:.1rem 0 .35rem;color:var(--text)}
.home-card p{color:var(--text-secondary);font-size:.88rem;line-height:1.55}
.home-card .home-icon{font-size:1.25rem}

/* ---- docs footer ---- */
.docs-footer{max-width:1600px;margin:0 auto;padding:1.5rem 2rem;border-top:1px solid var(--border);color:var(--text-dim);font-size:.82rem;text-align:center}
.docs-footer a{color:var(--text-secondary)}

/* ---- pygments — minimal dark ---- */
.codehilite{background:#0c0c0f}
.codehilite .k,.codehilite .kd,.codehilite .kn,.codehilite .kr,.codehilite .kt{color:#c4b5fd}
.codehilite .s,.codehilite .s1,.codehilite .s2{color:#86efac}
.codehilite .c,.codehilite .c1,.codehilite .cm{color:#71717a;font-style:italic}
.codehilite .mi,.codehilite .mf{color:#fbbf24}
.codehilite .nf,.codehilite .nc{color:#60a5fa}
.codehilite .o{color:#f472b6}
"""

JS = """/* Docs site interactions: search filter, active-TOC highlight, keyboard shortcut */
(function(){
  const search = document.getElementById('docsSearch');
  if (search) {
    search.addEventListener('input', e => {
      const q = e.target.value.trim().toLowerCase();
      document.querySelectorAll('.sidebar-nav li').forEach(li => {
        const text = li.textContent.toLowerCase();
        li.classList.toggle('hidden', q && !text.includes(q));
      });
    });
    document.addEventListener('keydown', e => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        search.focus();
        search.select();
      }
    });
  }
  // Highlight active TOC entry on scroll
  const toc = document.getElementById('toc');
  if (toc) {
    const links = Array.from(toc.querySelectorAll('a[href^="#"]'));
    const map = new Map();
    links.forEach(a => {
      const id = decodeURIComponent(a.getAttribute('href').slice(1));
      const el = document.getElementById(id);
      if (el) map.set(el, a);
    });
    if (map.size) {
      const io = new IntersectionObserver(entries => {
        entries.forEach(en => {
          if (en.isIntersecting) {
            links.forEach(l => l.classList.remove('active'));
            const a = map.get(en.target);
            if (a) a.classList.add('active');
          }
        });
      }, {rootMargin: '-20% 0px -70% 0px'});
      map.forEach((_a, el) => io.observe(el));
    }
  }
})();
"""


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9\- ]", "", text).strip().lower()
    return re.sub(r"\s+", "-", t)

def render_sidebar(current_slug: str) -> str:
    out = []
    for sec in NAV:
        out.append(f'<div class="group-title">{sec.icon} &nbsp;{html.escape(sec.title)}</div>')
        out.append("<ul>")
        for p in sec.pages:
            cls = ' class="active"' if p.slug == current_slug else ''
            out.append(f'<li><a href="{p.slug}.html"{cls}>{html.escape(p.title)}</a></li>')
        out.append("</ul>")
    return "\n".join(out)


def extract_title(md_text: str, fallback: str) -> str:
    for line in md_text.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def build_flat_list() -> list[DocPage]:
    return [p for sec in NAV for p in sec.pages]


def prev_next_html(pages: list[DocPage], idx: int) -> str:
    parts = ['<div class="prev-next">']
    if idx > 0:
        p = pages[idx - 1]
        parts.append(f'<a class="prev" href="{p.slug}.html"><span>← Previous</span>{html.escape(p.title)}</a>')
    else:
        parts.append('<span></span>')
    if idx < len(pages) - 1:
        n = pages[idx + 1]
        parts.append(f'<a class="next" href="{n.slug}.html"><span>Next →</span>{html.escape(n.title)}</a>')
    else:
        parts.append('<span></span>')
    parts.append('</div>')
    return "\n".join(parts)


def convert_markdown(md_text: str) -> tuple[str, str]:
    md = markdown.Markdown(
        extensions=[
            "fenced_code",
            "tables",
            "codehilite",
            "sane_lists",
            "attr_list",
            TocExtension(toc_depth="2-3", anchorlink=False, permalink="¶"),
        ],
        extension_configs={
            "codehilite": {"guess_lang": False, "noclasses": False, "css_class": "codehilite"},
        },
    )
    body = md.convert(md_text)
    toc = md.toc  # already HTML
    return body, toc


def rewrite_links(body: str, slug_map: dict[str, str]) -> str:
    """Rewrite relative links to other docs to point at the generated HTML pages."""
    def repl(match: re.Match) -> str:
        href = match.group(2)
        # skip external, anchors, mailto
        if href.startswith(("http://", "https://", "mailto:", "#", "/")):
            return match.group(0)
        # strip leading ./ and any directory prefix
        target = href.split("#", 1)
        path = target[0]
        anchor = ("#" + target[1]) if len(target) > 1 else ""
        # normalise path relative to docs/
        norm = path.replace("./", "").replace("docs/", "")
        if norm in slug_map:
          return f'{match.group(1)}="{slug_map[norm]}.html{anchor}"'
        if norm.endswith(".md"):
            # unknown md link — leave broken but readable
            return match.group(0)
        # images and other assets — make sure they resolve from /docs/site/guide/
        if norm.startswith("assets/"):
          return f'{match.group(1)}="{norm}{anchor}"'
        if any(path.endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
          return f'{match.group(1)}="../../figures/{os.path.basename(path)}{anchor}"'
        return match.group(0)
    return re.sub(r'(href|src)=\"([^\"]+)\"', repl, body)


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------

def ensure_tutorial_stub(page: DocPage) -> Path:
    """Tutorials live in docs/tutorials/*.md (hand-written). Create stubs if missing."""
    path = DOCS_DIR / page.src
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(f"# {page.title}\n\n_This tutorial is being written._\n")
    return path


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "assets").mkdir(exist_ok=True)
    (OUT_DIR / "assets" / "docs.css").write_text(CSS)
    (OUT_DIR / "assets" / "docs.js").write_text(JS)

    # Copy figures dir so image references resolve
    figures_src = DOCS_DIR / "figures"
    if figures_src.exists():
        figures_out = OUT_DIR.parent.parent / "figures"
        if figures_out.exists():
            pass  # keep existing
    # Also copy any screenshots dir if it exists
    screenshots_src = DOCS_DIR / "figures" / "screenshots"
    if screenshots_src.exists():
        screenshots_out = OUT_DIR / "assets" / "screenshots"
        screenshots_out.mkdir(exist_ok=True)
        for p in screenshots_src.iterdir():
            if p.is_file():
                shutil.copy2(p, screenshots_out / p.name)

    all_pages = build_flat_list()
    slug_map = {p.src: p.slug for p in all_pages}

    # ensure tutorial stubs exist
    for p in all_pages:
        if p.src.startswith("tutorials/"):
            ensure_tutorial_stub(p)

    for idx, page in enumerate(all_pages):
        src_path = DOCS_DIR / page.src
        if not src_path.exists():
            print(f"  warn: missing {src_path}")
            continue
        md_text = src_path.read_text(encoding="utf-8")
        title = extract_title(md_text, page.title)
        body, toc = convert_markdown(md_text)
        body = rewrite_links(body, slug_map)
        description = f"{title} — part of the SkeinDB documentation."
        sidebar_html = render_sidebar(page.slug)
        pn_html = prev_next_html(all_pages, idx)
        # find section
        section_title = page.section
        out = PAGE_TEMPLATE.format(
            title=html.escape(title),
            description=html.escape(description),
            sidebar=sidebar_html,
            section_title=html.escape(section_title),
            body=body,
            toc=toc or "<p style='color:var(--text-dim);font-size:.82rem'>(no sections)</p>",
            prev_next=pn_html,
            src=page.src,
        )
        (OUT_DIR / f"{page.slug}.html").write_text(out, encoding="utf-8")

    print(f"Wrote {len(all_pages)} pages to {OUT_DIR}")


if __name__ == "__main__":
    main()
