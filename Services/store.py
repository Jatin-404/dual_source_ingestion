from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()

# will be replaced by PostgreSQL later
# Structure: { document_id: { metadata + list of chunks } }
db = {}

# One chunk with its vector
class ChunkItem(BaseModel):
    document_id: str
    chunk_text: str
    chunk_index: int
    filename: str
    ingestion_timestamp: str
    case_number: str
    date: str
    court: str
    lawyers: list[str]
    vector: list[float]

# What embed service sends here 
class StoreRequest(BaseModel):
    chunks: list[ChunkItem]

@app.get("/health")
def health():
    return {"status": "ok", "service": "store"}

@app.post("/store")
def store_chunks(data: StoreRequest):
    stored_count = 0

    for chunk in data.chunks:
        doc_id = chunk.document_id

        # If this document isn't in db yet, create its entry
        if doc_id not in db:
            db[doc_id] = {
                "document_id": doc_id,
                "case_number": chunk.case_number,
                "court": chunk.court,
                "date": chunk.date,
                "lawyers": chunk.lawyers,
                "filename": chunk.filename,
                "chunks": []
            }

        # Append the chunk under its document
        db[doc_id]["chunks"].append(chunk.model_dump())
        stored_count += 1

    return {
        "status": "stored",
        "stored_count": stored_count,
        "total_documents": len(db)
    }

# Get all chunks (mainly for search service)
@app.get("/chunks")
def get_all_chunks():
    all_chunks = []
    for doc in db.values():
        all_chunks.extend(doc["chunks"])
    return {"total": len(all_chunks), "chunks": all_chunks}

# Get chunks for a specific document ---
@app.get("/chunks/{document_id}")
def get_chunks_by_document(document_id: str):
    if document_id not in db:
        return {"error": "document not found"}
    return db[document_id]

# all documents stored
@app.get("/documents")
def get_all_documents():
    summary = []
    for doc_id, doc in db.items():
        summary.append({
            "document_id": doc_id,
            "case_number": doc["case_number"],
            "court": doc["court"],
            "filename": doc["filename"],
            "total_chunks": len(doc["chunks"])
        })
    return {"total_documents": len(summary), "documents": summary}

if __name__ == "__main__":
    uvicorn.run("store:app", host="0.0.0.0", port=8003, reload=True)