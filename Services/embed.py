from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
from fastapi.concurrency import run_in_threadpool
from sentence_transformers import SentenceTransformer
import uvicorn

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Loading embedding model...")
    app.state.model = SentenceTransformer("BAAI/bge-large-en-v1.5", local_files_only=True)
    print("Model loaded!")
    yield
    print("Shutting down embed service")

app = FastAPI(lifespan=lifespan)

class ChunkItem(BaseModel):   # Same as chunk validation in chunker
    document_id: str
    chunk_text: str
    chunk_index: int
    filename: str
    ingestion_timestamp: str
    case_number: str
    date: str
    court: str
    lawyers: list[str]

class EmbedRequest(BaseModel):
    chunks: list[ChunkItem]

@app.get("/health")
def health():
    return {"status": "ok", "service": "embed"}

@app.post("/embed")
async def embed_chunks(data: EmbedRequest):
    model = app.state.model

    # Extract just the text from each chunk for batch embedding
    texts = [chunk.chunk_text for chunk in data.chunks]

    # Encode all at once (faster than one by one)
    vectors = await run_in_threadpool(model.encode, texts)

    # Attach vector back to each chunk
    result = []
    for chunk, vector in zip(data.chunks, vectors):
        chunk_with_vector = chunk.model_dump()
        chunk_with_vector["vector"] = vector.tolist()
        result.append(chunk_with_vector)

    return {
        "total_embedded": len(result),
        "chunks": result        # same chunks, now each has a "vector" field
    }

if __name__ == "__main__":
    uvicorn.run("embed:app", host="0.0.0.0", port=8002, reload=True)