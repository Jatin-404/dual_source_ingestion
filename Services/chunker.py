from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel
from datetime import datetime, timezone
import uuid

app = FastAPI()

class CaseMetadata(BaseModel):
    case_number: str                
    date: str                       
    court: str                      
    lawyers: list[str] 


class ChunkRequest(BaseModel):
    content_text: str               
    metadata: CaseMetadata          # case info → stamped on every chunk
    document_id: str | None = None
    filename: str                   
    chunk_size: int = 500

# response

class Chunk(BaseModel):
    document_id: str
    chunk_text: str
    chunk_index: int
    filename: str
    ingestion_timestamp: str
    # metadata fields — same on every chunk of a case
    case_number: str
    date: str
    court: str
    lawyers: list[str]


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "chunker" 
    }


@app.post("/chunk")
def chunk_document(data: ChunkRequest):
    doc_id = data.document_id or str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    # Chunks generation
    text = data.content_text.strip()
    raw_chunks = []
    for i in range(0, len(text), data.chunk_size):
        raw_chunks.append(text[i : i + data.chunk_size])

    # Stamp case metadata on every chunk
    chunks = []
    for index, chunk_text in enumerate(raw_chunks):
        chunks.append(Chunk(
            document_id=doc_id,
            chunk_text=chunk_text,
            chunk_index=index,
            filename=data.filename,
            ingestion_timestamp=timestamp,
            case_number=data.metadata.case_number,
            date=data.metadata.date,
            court=data.metadata.court,
            lawyers=data.metadata.lawyers
        ))

    return {
        "document_id": doc_id,
        "total_chunks": len(chunks),
        "case_number": data.metadata.case_number,   # useful for gateway logs
        "court": data.metadata.court,
        "chunks": [c.model_dump() for c in chunks]   # model_dump() converts a Pydantic model to a Python dictionary.
    }



if __name__ == "__main__":
    uvicorn.run("chunker:app", host="0.0.0.0", port=8001, reload=True)