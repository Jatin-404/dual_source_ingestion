from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
import psycopg2
import psycopg2.extras  #for dict cursor
from pgvector.psycopg2 import register_vector
import json
import uuid
import uvicorn



# DB CONNECTION

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "rag_db",
    "user": "postgres",
    "password": "postgres"
}

def get_connection():
    """
    Creates and returns a fresh DB connection.
    Also registers the vector type so psycopg2
    understands pgvector columns.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    register_vector(conn)   # ← tells psycopg2 how to handle vector type
    return conn


# STARTUP — test connection when service starts

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to PostgreSQL...")
    try:
        conn = get_connection()
        conn.close()
        print("PostgreSQL connected successfully!")
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
    yield
    print("Store service shutting down")

app = FastAPI(lifespan=lifespan)

# MODELS

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
    conn = get_connection()
    cursor = conn.cursor()
    stored_count = 0

    try:
        for chunk in data.chunks:
            cursor.execute("""
                INSERT INTO chunks (
                    chunk_id, document_id, chunk_text, chunk_index,
                    filename, ingestion_timestamp, case_number,
                    date, court, lawyers, embedding
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chunk_id) DO NOTHING
            """, (
                str(uuid.uuid4()),          # chunk_id — generate fresh one
                chunk.document_id,
                chunk.chunk_text,
                chunk.chunk_index,
                chunk.filename,
                chunk.ingestion_timestamp,
                chunk.case_number,
                chunk.date,
                chunk.court,
                json.dumps(chunk.lawyers),  # list → JSON string
                chunk.vector                # psycopg2 + pgvector handles this
            ))
            stored_count += 1

        conn.commit()   # ← IMPORTANT: save changes to disk

    except Exception as e:
        conn.rollback() # ← undo everything if something failed
        raise e

    finally:
        cursor.close()
        conn.close()    # ← always close connection

    return {
        "status": "stored",
        "stored_count": stored_count
    }

# Get all chunks (mainly for search service)
@app.get("/chunks")
def get_all_chunks():
    conn = get_connection()
    # DictCursor → rows come back as dicts not tuples
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        cursor.execute("SELECT * FROM chunks")
        rows = cursor.fetchall()

        chunks = []
        for row in rows:
            chunk = dict(row)
            chunk["lawyers"] = json.loads(chunk["lawyers"])  # JSON string → list
            chunk["vector"] = chunk.pop("embedding").tolist()  # vector → list
            chunks.append(chunk)

        return {"total": len(chunks), "chunks": chunks}

    finally:
        cursor.close()
        conn.close()

# Get chunks for a specific document ---
@app.get("/chunks/{document_id}")
def get_chunks_by_document(document_id: str):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        cursor.execute(
            "SELECT * FROM chunks WHERE document_id = %s",
            (document_id,)   # ← always use %s, never f-strings (SQL injection)
        )
        rows = cursor.fetchall()

        if not rows:
            return {"error": "document not found"}

        chunks = []
        for row in rows:
            chunk = dict(row)
            chunk["lawyers"] = json.loads(chunk["lawyers"])
            chunk["vector"] = chunk.pop("embedding").tolist()
            chunks.append(chunk)

        return {"document_id": document_id, "chunks": chunks}

    finally:
        cursor.close()
        conn.close()

# all documents stored
@app.get("/documents")
def get_all_documents():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        # GROUP BY to get one row per document with chunk count
        cursor.execute("""
            SELECT 
                document_id,
                case_number,
                court,
                filename,
                COUNT(*) as total_chunks
            FROM chunks
            GROUP BY document_id, case_number, court, filename
        """)
        rows = cursor.fetchall()

        return {
            "total_documents": len(rows),
            "documents": [dict(row) for row in rows]
        }

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    uvicorn.run("store:app", host="0.0.0.0", port=8003, reload=True)


'''

 Key concepts you just learned

 Concept | What it does |

|`get_connection()` | Opens a fresh DB connection |
| `register_vector()` | Teaches psycopg2 the vector type |
| `%s` placeholders | Safe way to pass values — never use f-strings in SQL |
| `conn.commit()` | Saves changes to disk permanently |
| `conn.rollback()` | Undoes everything if an error happened mid-way |
| `conn.close()` | Always close after use |
| `DictCursor` | Returns rows as `{"column": value}` instead of `(value, value)` |
| `ON CONFLICT DO NOTHING` | Skip duplicate chunk_ids silently |


'''



