#!/usr/bin/env python3
"""
SkeinDB Experimental Evaluation Harness — v2 (Adaptive Refs)
=============================================================

Updated to model the adaptive ValueID ref planning introduced in commit c1d9d84:
  "Optimize adaptive dedup refs and document row encoding"

The new code (plan_value_refs_for_rows) only emits $skein_ref when the ref
encoding is actually smaller than inline storage for a given table snapshot.
This eliminates the negative dedup ratios seen in v1 for small-value columns.

Encoding model:
  Inline:  just the JSON-serialized value (e.g. 42 → "42", "active" → "\"active\"")
  Seed ref (1st occurrence):
    {"$skein_ref":{"kind":"cell","id":"<32hex>","lit":<value>}}
  Compact ref (subsequent):
    {"$skein_ref":{"kind":"cell","id":"<32hex>"}}
"""

import blake3
import json
import math
import os
import random
import string
import struct
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import numpy as np

# ===========================================================================
# Core: ValueID (matching Rust)
# ===========================================================================

def value_id(data: bytes) -> bytes:
    h = blake3.blake3(data).digest()
    return h[:16]

def canonical_cell_bytes(value: Any) -> bytes:
    if value is None:
        return b"\x00"
    elif isinstance(value, bool):
        return b"\x01" + (b"\x01" if value else b"\x00")
    elif isinstance(value, int):
        return b"\x02" + struct.pack("<q", value)
    elif isinstance(value, float):
        return b"\x03" + struct.pack("<d", value)
    elif isinstance(value, str):
        encoded = value.encode("utf-8")
        return b"\x04" + struct.pack("<I", len(encoded)) + encoded
    else:
        j = json.dumps(value, sort_keys=True).encode("utf-8")
        return b"\x06" + struct.pack("<I", len(j)) + j


# ===========================================================================
# Adaptive Ref Planning (matching new Rust implementation)
# ===========================================================================

def json_inline_len(value: Any) -> int:
    """Size of value when stored inline as JSON."""
    return len(json.dumps(value))

def skein_ref_seed_len(vid_hex: str, value: Any) -> int:
    """Size of first occurrence: {"$skein_ref":{"kind":"cell","id":"<hex>","lit":<value>}}"""
    ref_obj = {"$skein_ref": {"kind": "cell", "id": vid_hex, "lit": value}}
    return len(json.dumps(ref_obj))

def skein_ref_compact_len(vid_hex: str) -> int:
    """Size of subsequent refs: {"$skein_ref":{"kind":"cell","id":"<hex>"}}"""
    ref_obj = {"$skein_ref": {"kind": "cell", "id": vid_hex}}
    return len(json.dumps(ref_obj))

def plan_adaptive_refs(all_values: list[tuple[str, Any]]) -> set:
    """
    Replicate plan_value_refs_for_rows: decide which ValueIDs are worth ref'ing.
    Returns set of vid_hex strings that should use refs.
    
    all_values: list of (vid_hex, original_value) for every cell in the table.
    """
    # Count occurrences and record metadata per unique ValueID
    candidates = {}  # vid_hex -> {count, value, inline_len}
    for vid_hex, value in all_values:
        if vid_hex not in candidates:
            candidates[vid_hex] = {
                "count": 1,
                "value": value,
                "inline_len": json_inline_len(value),
            }
        else:
            candidates[vid_hex]["count"] += 1

    use_refs = set()
    for vid_hex, info in candidates.items():
        if info["count"] < 2:
            continue
        
        seed_len = skein_ref_seed_len(vid_hex, info["value"])
        compact_len = skein_ref_compact_len(vid_hex)
        
        inline_total = info["inline_len"] * info["count"]
        refs_total = seed_len + (info["count"] - 1) * compact_len
        
        if refs_total < inline_total:
            use_refs.add(vid_hex)
    
    return use_refs


# ===========================================================================
# Adaptive Deduplication Measurement
# ===========================================================================

def measure_adaptive_dedup(table_name: str, rows: list[dict]) -> dict:
    """
    Measure deduplication using the adaptive ref planning.
    Models the actual on-disk JSON encoding with $skein_ref.
    """
    if not rows:
        return {}
    
    columns = list(rows[0].keys())
    n_rows = len(rows)
    n_cols = len(columns)
    
    # Phase 1: Collect all cell values with their ValueIDs
    all_cells = []  # (vid_hex, value, column)
    for row in rows:
        for col in columns:
            val = row[col]
            raw = canonical_cell_bytes(val)
            vid = value_id(raw)
            vid_hex = vid.hex()
            all_cells.append((vid_hex, val, col))
    
    # Phase 2: Run adaptive ref planner (table-wide)
    all_values = [(vid_hex, val) for vid_hex, val, _ in all_cells]
    use_refs = plan_adaptive_refs(all_values)
    
    # Phase 3: Calculate actual on-disk sizes
    # Inline total: every cell stored as JSON directly
    inline_total = 0
    for vid_hex, val, col in all_cells:
        inline_total += json_inline_len(val)
    
    # Adaptive total: ref'd cells use $skein_ref, others stay inline
    adaptive_total = 0
    seen_refs = set()
    for vid_hex, val, col in all_cells:
        if vid_hex in use_refs:
            if vid_hex not in seen_refs:
                # First occurrence: seed ref (includes literal)
                adaptive_total += skein_ref_seed_len(vid_hex, val)
                seen_refs.add(vid_hex)
            else:
                # Subsequent: compact ref
                adaptive_total += skein_ref_compact_len(vid_hex)
        else:
            # Not worth ref'ing — store inline
            adaptive_total += json_inline_len(val)
    
    # Per-column analysis
    col_stats = {}
    for col in columns:
        col_cells = [(vh, v) for vh, v, c in all_cells if c == col]
        col_inline = sum(json_inline_len(v) for _, v in col_cells)
        
        # Per-column ref planning
        col_plan = plan_adaptive_refs(col_cells)
        col_adaptive = 0
        col_seen = set()
        for vh, v in col_cells:
            if vh in col_plan:
                if vh not in col_seen:
                    col_adaptive += skein_ref_seed_len(vh, v)
                    col_seen.add(vh)
                else:
                    col_adaptive += skein_ref_compact_len(vh)
            else:
                col_adaptive += json_inline_len(v)
        
        distinct = len(set(vh for vh, _ in col_cells))
        refs_used = len(col_plan)
        ratio = col_inline / max(1, col_adaptive)
        savings = (1 - col_adaptive / max(1, col_inline)) * 100
        
        col_stats[col] = {
            "distinct": distinct,
            "refs_used": refs_used,
            "refs_skipped": distinct - refs_used,
            "inline_bytes": col_inline,
            "adaptive_bytes": col_adaptive,
            "dedup_ratio": round(ratio, 3),
            "savings_pct": round(savings, 1),
            "avg_value_json_len": round(col_inline / n_rows, 1),
        }
    
    # Aggregate
    agg_ratio = inline_total / max(1, adaptive_total)
    savings_pct = (1 - adaptive_total / max(1, inline_total)) * 100
    total_refs_used = len(use_refs)
    total_distinct = len(set(vh for vh, _, _ in all_cells))
    
    return {
        "table": table_name,
        "rows": n_rows,
        "columns": n_cols,
        "total_cells": n_rows * n_cols,
        "total_distinct_values": total_distinct,
        "values_ref_profitable": total_refs_used,
        "values_kept_inline": total_distinct - total_refs_used,
        "inline_total_bytes": inline_total,
        "adaptive_total_bytes": adaptive_total,
        "bytes_saved": inline_total - adaptive_total,
        "aggregate_dedup_ratio": round(agg_ratio, 3),
        "savings_pct": round(savings_pct, 1),
        "per_column": col_stats,
    }


# ===========================================================================
# QueryPatch (unchanged from v1)
# ===========================================================================

@dataclass
class QueryPatchResult:
    added: list
    removed: list
    updated: list
    moved: list
    base_etag: str = ""
    current_etag: str = ""

    @property
    def patch_size_estimate(self) -> int:
        patch = {
            "base_etag": self.base_etag, "etag": self.current_etag,
            "added": [{"pk": pk, "row": row} for pk, row in self.added],
            "removed": self.removed,
            "updated": [{"pk": pk, "row": row} for pk, row in self.updated],
            "moved": [{"pk": pk, "pos": pos} for pk, pos in self.moved],
        }
        return len(json.dumps(patch))

    @property
    def operation_count(self) -> int:
        return len(self.added) + len(self.removed) + len(self.updated) + len(self.moved)


def compute_query_patch(base_rows, current_rows, base_order, current_order):
    base_pks = set(base_rows.keys())
    current_pks = set(current_rows.keys())
    added = [(pk, current_rows[pk]) for pk in current_order if pk not in base_pks]
    removed = [pk for pk in base_order if pk not in current_pks]
    updated = [(pk, current_rows[pk]) for pk in current_order
               if pk in base_pks and pk in current_pks and base_rows[pk] != current_rows[pk]]
    base_pos = {pk: i for i, pk in enumerate(base_order)}
    cur_pos = {pk: i for i, pk in enumerate(current_order)}
    upd_pks = {u[0] for u in updated}
    moved = [(pk, cur_pos[pk]) for pk in current_order
             if pk in base_pos and pk in cur_pos and base_pos[pk] != cur_pos[pk] and pk not in upd_pks]
    return QueryPatchResult(added=added, removed=removed, updated=updated, moved=moved)


def measure_querypatch_bandwidth(rows, pk_col, window_size=50,
                                  change_rates=None, n_trials=50, seed=99):
    if change_rates is None:
        change_rates = [0.0, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50, 1.0]
    rng = random.Random(seed)
    results = []
    for rate in change_rates:
        trial_results = []
        for trial in range(n_trials):
            if len(rows) <= window_size:
                base_window = list(range(len(rows)))
            else:
                start = rng.randint(0, len(rows) - window_size)
                base_window = list(range(start, start + window_size))
            base_rows = {rows[i][pk_col]: rows[i] for i in base_window}
            base_order = [rows[i][pk_col] for i in base_window]
            current_rows = dict(base_rows)
            current_order = list(base_order)
            n_changes = max(0, int(round(window_size * rate)))
            changes_made = 0
            if n_changes > 0:
                n_remove = rng.randint(0, min(n_changes, len(current_order) // 2))
                n_update = rng.randint(0, min(n_changes - n_remove, len(current_order)))
                n_add = n_changes - n_remove - n_update
                for _ in range(n_remove):
                    if current_order:
                        pk = rng.choice(current_order)
                        current_order.remove(pk); del current_rows[pk]; changes_made += 1
                for pk in rng.sample(list(current_rows.keys()), min(n_update, len(current_rows))):
                    row = dict(current_rows[pk])
                    keys = [k for k in row if k != pk_col]
                    if keys:
                        mk = rng.choice(keys)
                        row[mk] = row[mk] + rng.uniform(-100, 100) if isinstance(row[mk], (int, float)) else f"mod_{rng.randint(1,99999)}"
                    current_rows[pk] = row; changes_made += 1
                for _ in range(n_add):
                    npk = rng.randint(900000, 999999)
                    while npk in current_rows: npk = rng.randint(900000, 999999)
                    nr = dict(rng.choice(list(base_rows.values()))); nr[pk_col] = npk
                    current_rows[npk] = nr; current_order.insert(rng.randint(0, len(current_order)), npk); changes_made += 1
            patch = compute_query_patch(base_rows, current_rows, base_order, current_order)
            full_size = len(json.dumps(list(current_rows.values())))
            patch_size = patch.patch_size_estimate
            effective = 80 if rate == 0.0 and changes_made == 0 else patch_size
            trial_results.append({"full": full_size, "patch": patch_size, "effective": effective, "ops": patch.operation_count})
        results.append({
            "change_rate": rate,
            "avg_full_bytes": round(np.mean([t["full"] for t in trial_results])),
            "avg_patch_bytes": round(np.mean([t["patch"] for t in trial_results])),
            "avg_effective_bytes": round(np.mean([t["effective"] for t in trial_results])),
            "bandwidth_reduction_pct": round((1 - np.mean([t["effective"] for t in trial_results]) / max(1, np.mean([t["full"] for t in trial_results]))) * 100, 1),
            "avg_ops": round(np.mean([t["ops"] for t in trial_results]), 1),
        })
    return results


# ===========================================================================
# Data Generators (same as v1)
# ===========================================================================

def gen_tpch_lineitem(n_rows, seed=42):
    rng = random.Random(seed)
    statuses = ["O", "F"]
    shipmodes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
    instructions = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
    return_flags = ["R", "A", "N"]
    n_orders = max(100, n_rows // 4)
    n_parts = max(50, n_rows // 20)
    n_suppliers = max(10, n_rows // 100)
    dates = [19920101 + d for d in range(2557)]
    rows = []
    for i in range(n_rows):
        rows.append({
            "l_orderkey": rng.randint(1, n_orders),
            "l_partkey": rng.randint(1, n_parts),
            "l_suppkey": rng.randint(1, n_suppliers),
            "l_linenumber": rng.randint(1, 7),
            "l_quantity": round(rng.uniform(1, 50), 2),
            "l_extendedprice": round(rng.uniform(900, 105000), 2),
            "l_discount": round(rng.choice([0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]), 2),
            "l_tax": round(rng.choice([0.0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08]), 2),
            "l_returnflag": rng.choice(return_flags),
            "l_linestatus": rng.choice(statuses),
            "l_shipdate": rng.choice(dates),
            "l_commitdate": rng.choice(dates),
            "l_receiptdate": rng.choice(dates),
            "l_shipinstruct": rng.choice(instructions),
            "l_shipmode": rng.choice(shipmodes),
            "l_comment": f"comment_{rng.randint(1, n_rows // 2)}",
        })
    return rows

def gen_tpch_orders(n_rows, seed=43):
    rng = random.Random(seed)
    statuses = ["O", "F", "P"]
    priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
    n_customers = max(50, n_rows // 10)
    clerks = [f"Clerk#{i:09d}" for i in range(max(10, n_rows // 100))]
    dates = [19920101 + d for d in range(2557)]
    rows = []
    for i in range(n_rows):
        rows.append({
            "o_orderkey": i + 1, "o_custkey": rng.randint(1, n_customers),
            "o_orderstatus": rng.choice(statuses),
            "o_totalprice": round(rng.uniform(800, 600000), 2),
            "o_orderdate": rng.choice(dates), "o_orderpriority": rng.choice(priorities),
            "o_clerk": rng.choice(clerks), "o_shippriority": rng.randint(0, 1),
            "o_comment": f"order_comment_{rng.randint(1, n_rows // 3)}",
        })
    return rows

def gen_tpch_customer(n_rows, seed=44):
    rng = random.Random(seed)
    segments = ["BUILDING", "AUTOMOBILE", "MACHINERY", "HOUSEHOLD", "FURNITURE"]
    rows = []
    for i in range(n_rows):
        rows.append({
            "c_custkey": i + 1,
            "c_name": f"Customer#{i+1:09d}",
            "c_address": f"{rng.randint(1,9999)} {''.join(rng.choices(string.ascii_uppercase, k=10))} St",
            "c_nationkey": rng.randint(0, 24),
            "c_phone": f"{rng.randint(10,34)}-{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            "c_acctbal": round(rng.uniform(-999.99, 9999.99), 2),
            "c_mktsegment": rng.choice(segments),
            "c_comment": f"cust_comment_{rng.randint(1, n_rows // 3)}",
        })
    return rows

def gen_web_dashboard(n_rows, seed=50):
    rng = random.Random(seed)
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    categories = ["electronics", "clothing", "food", "books", "home"]
    regions = ["US-East", "US-West", "EU", "APAC", "LATAM"]
    payments = ["credit_card", "paypal", "bank_transfer", "crypto"]
    currencies = ["USD", "EUR", "GBP", "JPY"]
    n_cust = max(50, n_rows // 5)
    rows = []
    for i in range(n_rows):
        rows.append({
            "order_id": i + 1, "customer_id": rng.randint(1, n_cust),
            "customer_name": f"Customer {rng.randint(1, n_cust)}",
            "status": rng.choice(statuses), "category": rng.choice(categories),
            "region": rng.choice(regions), "payment_method": rng.choice(payments),
            "currency": rng.choice(currencies),
            "amount": round(rng.uniform(5.0, 5000.0), 2),
            "quantity": rng.randint(1, 20),
            "created_at": f"2024-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
            "is_priority": rng.choice([True, False]),
        })
    return rows

def gen_user_profiles(n_rows, seed=60):
    rng = random.Random(seed)
    themes = ["dark", "light", "system"]
    languages = ["en", "de", "fr", "es", "ja", "zh", "ko", "pt"]
    timezones = ["UTC", "US/Eastern", "US/Pacific", "Europe/Berlin", "Europe/London", "Asia/Tokyo", "Asia/Shanghai"]
    plans = ["free", "pro", "enterprise"]
    roles = ["user", "admin", "moderator", "viewer"]
    pref_combos = []
    for theme in themes:
        for lang in languages[:5]:
            pref_combos.append(json.dumps({"theme": theme, "language": lang,
                "notifications": {"email": True, "push": True, "sms": False},
                "dashboard": {"widgets": ["activity", "stats", "alerts"]}}, sort_keys=True))
    rows = []
    for i in range(n_rows):
        rows.append({
            "user_id": i + 1, "email": f"user{rng.randint(1, n_rows)}@example.com",
            "display_name": f"User {rng.randint(1, n_rows)}",
            "plan": rng.choice(plans), "role": rng.choice(roles),
            "timezone": rng.choice(timezones),
            "preferences_json": rng.choice(pref_combos),
            "status": rng.choice(["active", "inactive", "suspended"]),
            "login_count": rng.randint(0, 5000),
        })
    return rows

def gen_config_store(n_rows, seed=70):
    """Config/template store — many large repeated JSON values."""
    rng = random.Random(seed)
    templates = []
    for i in range(20):
        templates.append(json.dumps({
            "version": f"v{i//5+1}.{i%5}",
            "settings": {
                "max_retries": rng.choice([3, 5, 10]),
                "timeout_ms": rng.choice([1000, 3000, 5000, 10000]),
                "feature_flags": {f"flag_{j}": rng.choice([True, False]) for j in range(10)},
                "routing": {"primary": f"region-{rng.randint(1,5)}", "fallback": f"region-{rng.randint(1,5)}"},
                "rate_limits": {"requests_per_sec": rng.choice([100, 500, 1000]), "burst": rng.choice([10, 50, 100])},
            },
            "metadata": {"created_by": f"admin_{rng.randint(1,5)}", "approved": True,
                          "description": f"Configuration template variant {i} for production deployment with standard parameters"}
        }, sort_keys=True))
    
    envs = ["production", "staging", "development", "testing"]
    services = [f"service-{i}" for i in range(50)]
    rows = []
    for i in range(n_rows):
        rows.append({
            "config_id": i + 1,
            "service": rng.choice(services),
            "environment": rng.choice(envs),
            "config_json": rng.choice(templates),
            "active": rng.choice([True, False]),
            "priority": rng.choice(["low", "medium", "high", "critical"]),
        })
    return rows


# ===========================================================================
# Main
# ===========================================================================

def main():
    print("=" * 80)
    print("SkeinDB Experimental Evaluation v2 — Adaptive Ref Planning")
    print("(models commit c1d9d84: plan_value_refs_for_rows)")
    print("=" * 80)

    # -------------------------------------------------------------------
    # Experiment 1: Adaptive Deduplication
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("EXPERIMENT 1: Adaptive Cell-Interning Deduplication")
    print("=" * 80)
    print("\nKey change: refs are only emitted when they produce net byte savings.")
    print("Small values (ints, short strings) are kept inline; only large repeated")
    print("values get $skein_ref encoding.\n")

    datasets = [
        ("TPC-H LINEITEM (100K)",  "lineitem",   gen_tpch_lineitem(100_000)),
        ("TPC-H ORDERS (100K)",    "orders",      gen_tpch_orders(100_000)),
        ("TPC-H CUSTOMER (15K)",   "customer",    gen_tpch_customer(15_000)),
        ("Web Dashboard (100K)",   "dashboard",   gen_web_dashboard(100_000)),
        ("User Profiles (50K)",    "profiles",    gen_user_profiles(50_000)),
        ("Config Store (50K)",     "configs",     gen_config_store(50_000)),
    ]

    all_results = {}
    for label, name, rows in datasets:
        print(f"--- {label} ---")
        result = measure_adaptive_dedup(name, rows)
        all_results[name] = result

        print(f"  Rows: {result['rows']:,}  Columns: {result['columns']}")
        print(f"  Distinct values: {result['total_distinct_values']:,}"
              f"  (ref-profitable: {result['values_ref_profitable']:,},"
              f"  kept inline: {result['values_kept_inline']:,})")
        print(f"  Inline total:   {result['inline_total_bytes']:>12,} bytes")
        print(f"  Adaptive total: {result['adaptive_total_bytes']:>12,} bytes")
        print(f"  Bytes saved:    {result['bytes_saved']:>12,} bytes")
        print(f"  Dedup ratio:    {result['aggregate_dedup_ratio']:.3f}×")
        print(f"  Savings:        {result['savings_pct']:.1f}%")

        # Guarantee: adaptive should never be WORSE than inline
        if result['adaptive_total_bytes'] > result['inline_total_bytes']:
            print(f"  ⚠️  WARNING: adaptive is LARGER than inline!")
        else:
            print(f"  ✓  Adaptive ≤ Inline (no regression)")
        print()

        cols_sorted = sorted(result["per_column"].items(),
                              key=lambda x: x[1]["savings_pct"], reverse=True)
        print(f"  Per-column breakdown:")
        print(f"    {'Column':25s} {'Distinct':>8s} {'Refs':>5s} {'Skip':>5s}"
              f" {'AvgLen':>7s} {'Savings':>8s} {'Ratio':>7s}")
        print(f"    {'-'*25} {'-'*8} {'-'*5} {'-'*5} {'-'*7} {'-'*8} {'-'*7}")
        for col, cs in cols_sorted:
            marker = "★" if cs["savings_pct"] > 1.0 else "·" if cs["savings_pct"] == 0.0 else " "
            print(f"  {marker} {col:25s} {cs['distinct']:>8,} {cs['refs_used']:>5,}"
                  f" {cs['refs_skipped']:>5,} {cs['avg_value_json_len']:>7.1f}"
                  f" {cs['savings_pct']:>7.1f}% {cs['dedup_ratio']:>6.3f}×")
        print()

    # -------------------------------------------------------------------
    # Experiment 2: QueryPatch (unchanged)
    # -------------------------------------------------------------------
    print("=" * 80)
    print("EXPERIMENT 2: QueryPatch Bandwidth (unchanged from v1)")
    print("=" * 80)

    dash_rows = gen_web_dashboard(10000)
    for ws in [50, 200]:
        qp = measure_querypatch_bandwidth(dash_rows, "order_id", window_size=ws, n_trials=50)
        print(f"\n  Window: {ws} rows, 50 trials")
        print(f"  {'Δ Rate':>8s} {'Full':>9s} {'Patch':>9s} {'Effective':>10s} {'Savings':>8s}")
        print(f"  {'-'*8} {'-'*9} {'-'*9} {'-'*10} {'-'*8}")
        for r in qp:
            print(f"  {r['change_rate']:>7.0%} {r['avg_full_bytes']:>8,}B"
                  f" {r['avg_patch_bytes']:>8,}B {r['avg_effective_bytes']:>9,}B"
                  f" {r['bandwidth_reduction_pct']:>7.1f}%")

    # -------------------------------------------------------------------
    # Experiment 3: ValueID Latency
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("EXPERIMENT 3: ValueID Latency")
    print("=" * 80)

    test_vals = [os.urandom(random.choice([8, 16, 64, 256, 1024])) for _ in range(200_000)]
    for _ in test_vals[:1000]: value_id(_)  # warm
    t0 = time.perf_counter()
    for v in test_vals: value_id(v)
    elapsed = time.perf_counter() - t0
    print(f"\n  200K hashes in {elapsed:.3f}s → {elapsed/200_000*1e9:.0f} ns/op"
          f" → {200_000/elapsed/1e6:.2f} M ops/sec")

    # -------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("SUMMARY: v1 (naive) vs v2 (adaptive)")
    print("=" * 80)
    
    # v1 results (hardcoded from previous run for comparison)
    v1_results = {
        "lineitem": {"ratio": 0.488, "savings": -104.7},
        "orders":   {"ratio": 0.477, "savings": -109.4},
        "customer": {"ratio": 0.372, "savings": -169.1},
        "dashboard":{"ratio": 0.487, "savings": -105.4},
        "profiles": {"ratio": 1.159, "savings": 13.7},
    }

    print(f"\n  {'Dataset':25s} {'v1 Ratio':>10s} {'v1 Savings':>12s}"
          f" {'v2 Ratio':>10s} {'v2 Savings':>12s} {'Verdict':>10s}")
    print(f"  {'-'*25} {'-'*10} {'-'*12} {'-'*10} {'-'*12} {'-'*10}")
    
    for name, result in all_results.items():
        v1 = v1_results.get(name, {"ratio": None, "savings": None})
        v1r = f"{v1['ratio']:.3f}×" if v1["ratio"] else "—"
        v1s = f"{v1['savings']:.1f}%" if v1["savings"] is not None else "—"
        v2r = f"{result['aggregate_dedup_ratio']:.3f}×"
        v2s = f"{result['savings_pct']:.1f}%"
        
        if result['savings_pct'] > 0:
            verdict = "✓ WIN" if result['savings_pct'] > 1.0 else "✓ NEUTRAL"
        elif result['savings_pct'] == 0:
            verdict = "= EVEN"
        else:
            verdict = "✗ REGRESS"
        
        print(f"  {name:25s} {v1r:>10s} {v1s:>12s} {v2r:>10s} {v2s:>12s} {verdict:>10s}")

    print()
    print("  Key insight: Adaptive planning guarantees R ≥ 1.0 by construction.")
    print("  Values that don't benefit from ref encoding are kept inline.")
    print("  Savings scale with value size × repetition count.")


if __name__ == "__main__":
    main()
