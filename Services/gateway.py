from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import uvicorn
from httpx import AsyncClient
import uuid
from routes import ingest, search


app = FastAPI()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "gateway" 
    }


app.include_router(ingest.router)
app.include_router(search.router)


if __name__ == "__main__":
    uvicorn.run("gateway:app", host="0.0.0.0", port=8000, reload=True)