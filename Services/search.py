from fastapi import FastAPI
from pydantic import BaseModel
from httpx import AsyncClient
from sklearn.metrics.pairwise import cosine_similarity
import os
import uvicorn

app = FastAPI()

STORE_URL = "http://localhost:8003"
EMBED_URL = "http://localhost:8002"

# What the user sends to search
class SearchRequest(BaseModel):
    query: str
    top_k: int = 5                      # how many results to return
    # optional filters — all are optional, user can mix and match
    court: str | None = None            # e.g. "Delhi High Court"
    case_number: str | None = None      # e.g. "CV-2024-00123"
    date: str | None = None             # e.g. "2024-01-15"
    lawyer: str | None = None           # e.g. "Advocate Sharma"

@app.get("/health")
def health():
    return {"status": "ok", "service": "search"}

@app.post("/search")
async def search(data: SearchRequest):
    async with AsyncClient() as client:

        # Embed the user's query
        embed_response = await client.post(
            f"{EMBED_URL}/embed",
            json={
                "chunks": [{
                    "document_id": "query",
                    "chunk_text": data.query,
                    "chunk_index": 0,
                    "filename": "query",
                    "ingestion_timestamp": "",
                    "case_number": "",
                    "date": "",
                    "court": "",
                    "lawyers": []
                }]
            }
        )
        query_vector = embed_response.json()["chunks"][0]["vector"]

        # Fetching all chunks from store
        store_response = await client.get(f"{STORE_URL}/chunks")
        # catch bad responses before they crash
        if store_response.status_code != 200:
            return {
                "error": f"Store service returned {store_response.status_code}",
                "detail": store_response.text
            }
        all_chunks = store_response.json()["chunks"]

    # Step 3: Apply metadata filters BEFORE doing vector math
    # This is the "hybrid" part — filter first, then rank by similarity
    filtered_chunks = []
    for chunk in all_chunks:  # This loop removes chunks that don’t match the metadata filters

        # Filter by court if provided
        if data.court and chunk["court"].lower() != data.court.lower():
            continue

        # Filter by case_number if provided
        if data.case_number and chunk["case_number"] != data.case_number:
            continue

        # Filter by date if provided
        if data.date and chunk["date"] != data.date:
            continue

        # Filter by lawyer if provided (check inside the list)
        if data.lawyer:
            lawyers_lower = [l.lower() for l in chunk["lawyers"]]
            if data.lawyer.lower() not in lawyers_lower:
                continue

        filtered_chunks.append(chunk)

    #  Score remaining chunks by vector similarity 
    results = []
    for chunk in filtered_chunks:
        score = cosine_similarity(
            [query_vector],
            [chunk["vector"]]
        )[0][0]

        results.append({
            "document_id": chunk["document_id"],
            "chunk_text": chunk["chunk_text"],
            "chunk_index": chunk["chunk_index"],
            "filename": chunk["filename"],
            "case_number": chunk["case_number"],
            "court": chunk["court"],
            "date": chunk["date"],
            "lawyers": chunk["lawyers"],
            "score": round(float(score), 4)
        })

    # Sorting by score and returning top k
    results.sort(key=lambda x: x["score"], reverse=True)

    return {
        "query": data.query,
        "filters_applied": {
            "court": data.court,
            "case_number": data.case_number,
            "date": data.date,
            "lawyer": data.lawyer
        },
        "total_after_filter": len(filtered_chunks),
        "top_k": data.top_k,
        "results": results[:data.top_k]
    }

if __name__ == "__main__":
    uvicorn.run("search:app", host="0.0.0.0", port=8004, reload=True)