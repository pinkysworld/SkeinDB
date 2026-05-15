#!/usr/bin/env python3
"""Small SkeinDB vector RAG retrieval example.

This sample intentionally uses a deterministic toy embedding function so it can run
without external model or LLM credentials. It demonstrates the application shape:
seed content, insert embeddings, retrieve nearest chunks, and assemble a grounded
prompt that an application can send to its own generation model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


DOCS = [
    {
        "id": 1,
        "title": "Time travel",
        "body": "SkeinDB supports MVCC as_of reads, history retention, and replay bundles for deterministic recovery.",
    },
    {
        "id": 2,
        "title": "Vector search",
        "body": "SkeinDB stores embedding literals, builds an HNSW index, and exposes vector.search plus vector.benchmark.",
    },
    {
        "id": 3,
        "title": "Forensics",
        "body": "The audit WAL forms a BLAKE3 hash chain with checkpoint anchors, Merkle proofs, and forensic export bundles.",
    },
]


@dataclass
class RpcClient:
    base_url: str
    next_id: int = 1

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "skeinql": "1.0",
            "id": f"rag-{self.next_id}",
            "method": method,
            "params": params,
        }
        self.next_id += 1
        request = urllib.request.Request(
            f"{self.base_url.rstrip('/')}/api/v1/rpc",
            data=json.dumps(payload).encode("utf-8"),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise SystemExit(f"RPC transport failed for {method}: {exc}") from exc
        if not data.get("ok"):
            error = data.get("error") or {}
            code = error.get("code", "rpc_error")
            message = error.get("message", "unknown error")
            raise SystemExit(f"RPC {method} failed: {code}: {message}")
        return data.get("result") or {}


def toy_embedding(text: str, dims: int = 8) -> list[float]:
    values = [0.0 for _ in range(dims)]
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=4).digest()
        bucket = digest[0] % dims
        sign = 1.0 if digest[1] & 1 else -1.0
        values[bucket] += sign
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [round(value / norm, 6) for value in values]


def lit_u64(value: int) -> dict[str, Any]:
    return {"t": "u64", "v": value}


def lit_str(value: str) -> dict[str, Any]:
    return {"t": "str", "v": value}


def lit_embedding(text: str) -> dict[str, Any]:
    vector = toy_embedding(text)
    return {"t": "embedding", "dims": len(vector), "v": vector, "model": "toy-hash-v1"}


def seed_schema(client: RpcClient) -> None:
    client.call("schema.create_database", {"db": "rag"})
    client.call(
        "schema.create_table",
        {
            "db": "rag",
            "table": "chunks",
            "if_not_exists": True,
            "columns": [
                {"name": "id", "type": {"kind": "u64"}, "nullable": False},
                {"name": "title", "type": {"kind": "str"}, "nullable": False},
                {"name": "body", "type": {"kind": "str"}, "nullable": False},
                {"name": "embedding", "type": {"kind": "embedding"}, "nullable": False},
            ],
            "primary_key": ["id"],
        },
    )


def insert_docs(client: RpcClient) -> None:
    for doc in DOCS:
        client.call(
            "data.delete",
            {
                "table": {"db": "rag", "table": "chunks"},
                "where": {"op": "eq", "a": {"col": "id"}, "b": {"lit": lit_u64(doc["id"])}},
                "limit": 1,
            },
        )
        client.call(
            "data.insert",
            {
                "into": {"db": "rag", "table": "chunks"},
                "rows": [
                    {
                        "id": lit_u64(doc["id"]),
                        "title": lit_str(doc["title"]),
                        "body": lit_str(doc["body"]),
                        "embedding": lit_embedding(f"{doc['title']} {doc['body']}"),
                    }
                ]
            },
        )
    client.call(
        "vector.insert",
        {
            "table": {"db": "rag", "table": "chunks"},
            "column": "embedding",
            "rows": [
                {
                    "pk": [lit_u64(doc["id"])],
                    "embedding": lit_embedding(f"{doc['title']} {doc['body']}"),
                }
                for doc in DOCS
            ],
            "upsert": True,
        },
    )


def retrieve(client: RpcClient, question: str, k: int) -> list[dict[str, Any]]:
    result = client.call(
        "vector.search",
        {
            "table": {"db": "rag", "table": "chunks"},
            "column": "embedding",
            "query": lit_embedding(question),
            "k": k,
            "metric": "cosine",
            "include_row": True,
            "use_lsh": False,
        },
    )
    return result.get("matches") or []


def build_prompt(question: str, matches: list[dict[str, Any]]) -> str:
    context_lines = []
    for rank, match in enumerate(matches, start=1):
        row = match.get("row") or {}
        title = row.get("title", {}).get("v", "untitled")
        body = row.get("body", {}).get("v", "")
        score = match.get("score", 0.0)
        context_lines.append(f"[{rank}] {title} (score={score:.3f})\n{body}")
    context = "\n\n".join(context_lines) or "No matching context was retrieved."
    return (
        "Answer the question using only the retrieved SkeinDB context.\n\n"
        f"Question: {question}\n\n"
        f"Context:\n{context}\n\n"
        "Answer:"
    )


def self_test() -> None:
    vector = toy_embedding("vector search benchmark")
    prompt = build_prompt(
        "How does SkeinDB retrieve vectors?",
        [
            {
                "score": 0.99,
                "row": {
                    "title": {"t": "str", "v": "Vector search"},
                    "body": {"t": "str", "v": "SkeinDB exposes vector.search and vector.benchmark."},
                },
            }
        ],
    )
    assert len(vector) == 8
    assert all(-1.0 <= value <= 1.0 for value in vector)
    assert "Question:" in prompt and "Context:" in prompt and "vector.search" in prompt
    print(json.dumps({"ok": True, "dims": len(vector), "prompt_chars": len(prompt)}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SkeinDB vector RAG example")
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="SkeinDB HTTP base URL")
    parser.add_argument("--question", default="How can I evaluate vector retrieval in SkeinDB?")
    parser.add_argument("--k", type=int, default=2)
    parser.add_argument("--self-test", action="store_true", help="Run deterministic local checks without contacting SkeinDB")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return

    client = RpcClient(args.url)
    seed_schema(client)
    insert_docs(client)
    matches = retrieve(client, args.question, args.k)
    print(build_prompt(args.question, matches))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
