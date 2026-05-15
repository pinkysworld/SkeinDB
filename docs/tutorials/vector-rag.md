# Vector RAG retrieval

This tutorial builds a tiny retrieval-augmented generation pipeline on SkeinDB's native vector RPCs. It uses the sample app at [`samples/vector_rag_pipeline.py`](https://github.com/pinkysworld/SkeinDB/blob/main/samples/vector_rag_pipeline.py), deterministic toy embeddings, and no external model or API key.

Prerequisite: [Quickstart](quickstart.html) completed and `skeindb` running on `http://127.0.0.1:8080`.

## 1. Start SkeinDB

```bash
skeindb serve --data ./data --http 8080 --mysql 3306
```

The sample talks to `POST /api/v1/rpc`, the same SkeinQL endpoint used by the other tutorials.

## 2. Check the sample locally

The sample has a local self-test path that exercises the deterministic embedding and prompt assembly without contacting a server:

```bash
python3 samples/vector_rag_pipeline.py --self-test
```

Expected output is a small JSON object with `"ok": true` and `"dims": 8`.

## 3. Run the retrieval pipeline

```bash
python3 samples/vector_rag_pipeline.py \
  --url http://127.0.0.1:8080 \
  --question "How can I evaluate vector retrieval in SkeinDB?" \
  --k 2
```

The script performs this flow:

1. Creates a `rag.chunks` table with `id`, `title`, `body`, and an `embedding` column.
2. Inserts three short document chunks through `data.insert`.
3. Upserts deterministic embedding literals with `vector.insert`.
4. Searches the nearest chunks with `vector.search` and `include_row: true`.
5. Prints a grounded prompt containing the question and retrieved context.

The generated prompt is intentionally the handoff point, not a built-in LLM call. Applications can send it to their own generation model, store it for audit, or keep the retrieval stage separate for evaluation.

## 4. Inspect the vector index

After the sample runs, inspect the embedding column statistics:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0",
    "id":"rag-status",
    "method":"vector.index.status",
    "params":{"table":{"db":"rag","table":"chunks"},"column":"embedding"}
  }' | jq
```

For this tiny dataset the index is deliberately small, but the response proves that the embedding column is registered and searchable.

## 5. Evaluate retrieval quality

Use `vector.benchmark` with the same deterministic query embedding to compare exact and indexed results:

```bash
curl -s -XPOST http://127.0.0.1:8080/api/v1/rpc \
  -H 'Content-Type: application/json' \
  -d '{
    "skeinql":"1.0",
    "id":"rag-benchmark",
    "method":"vector.benchmark",
    "params":{
      "table":{"db":"rag","table":"chunks"},
      "column":"embedding",
      "queries":[{"t":"embedding","dims":8,"v":[0.57735,0.0,0.288675,0.288675,0.288675,0.288675,0.0,0.57735],"model":"toy-hash-v1"}],
      "k":2,
      "metric":"cosine"
    }
  }' | jq
```

In production code, generate embeddings with the model used by your application and keep the `model` label stable. The sample's `toy-hash-v1` vectors are only there to make the walkthrough deterministic and credential-free.

## Next

- [SkeinQL reference](skeinql.html)
- [SkeinQL research extensions](skeinql-research.html)
- [Admin console tour](admin-tour.html)
