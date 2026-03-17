from fastapi import APIRouter, BackgroundTasks, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from httpx import AsyncClient
import uuid
import asyncio
import json


router = APIRouter(prefix='/ingest', tags=["Ingestion"])

CHUNKER_URL = "http://localhost:8001"
EMBED_URL   = "http://localhost:8002"
STORE_URL   = "http://localhost:8003"

jobs = {}

class CaseMetadata(BaseModel):
    case_number: str                
    date: str                       
    court: str                      
    lawyers: list[str] 

class IngestRequest(BaseModel):
    content_text: str               
    metadata: CaseMetadata          # case info → stamped on every chunk
    document_id: str | None = None
    filename: str                   
    chunk_size: int = 500


async def ingest_one(request: IngestRequest, client: AsyncClient):

    chunk_response = await client.post(f"{CHUNKER_URL}/chunk",
                                        json={
                                            "content_text": request.content_text,
                                            "metadata": request.metadata.model_dump(),
                                            "filename": request.filename,
                                            "chunk_size": request.chunk_size
                                        }) 
    chunk_data = chunk_response.json()
    chunks = chunk_data["chunks"]
    document_id = chunk_data["document_id"]


    embed_response = await client.post(f"{EMBED_URL}/embed",
                                        json={
                                            "chunks" : chunks
                                        })
    
    embed_chunks = embed_response.json()["chunks"]


    await client.post(f"{STORE_URL}/store",
                        json={"chunks": embed_chunks})

    return {
        "document_id": document_id,
        "toatl_chunks": len(chunks),
        "case_number": request.metadata.case_number,
        "court": request.metadata.court
    }
    
async def run_ingest_batch(requests: list[IngestRequest], job_id: str):
    jobs[job_id] = {"status": "processing"}
    try:
        async with AsyncClient(timeout= 60) as client:
            jobs[job_id] = {"status": "processing"}
            # run all cases in parallel
            tasks = [ingest_one(req, client) for req in requests]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # separte success from failures

        completed = []
        failed = []
        for req , result in zip(requests,results):
            if isinstance(result, Exception):
                failed.append({
                    "case_number": req.metadata.case_number,
                    "error": str(result)
                })
            else:
                completed.append(result)
        jobs[job_id] = {
            "status": "completed",
            "total_cases": len(requests),
            "succeded": len(completed),
            "failed": len(failed),
            "results": completed,
            "errors": failed
        }

    except Exception as e:
        jobs[job_id] = {"status": "failed", "error": str(e)}


@router.post('/')
async def ingest(requests: list[IngestRequest], background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "queued"}

    background_tasks.add_task(run_ingest_batch, requests, job_id)
    return{
        "job_id": job_id,
        "status": "queued",
        "msg": "ingestion started in background"
    }

@router.post("/upload")
async def ingest_upload(
    backgroud_tasks: BackgroundTasks,
    content_file: UploadFile = File(...),
    metadata_file: UploadFile = File(...),
    chunk_size: int = Form(default=500)  # optional sent as form field
):
    # step-1 reading both files 
    content_bytes = await content_file.read()     # what does await key word do here and is the info stored after reading in bytes?
    metadata_bytes = await metadata_file.read()

    # step-2 decode content file as plain text

    try: 
        content_text = content_bytes.decode("utf-8")
    except Exception:
        raise HTTPException(status_code=400, detail="Content file must be valid UTF-8 text file")
    
    # step-3 parsing metadata file as json

    try:
        metadata_dict = json.loads(metadata_bytes.decode("utf-8"))
        metadata = CaseMetadata(**metadata_dict)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Metadata file must be a valid JSON")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Metadata file missing fields: {str(e)}")
    
    # step-4 Build ingest request (same shape as before)

    request = IngestRequest(
        content_text=content_text,
        metadata=metadata,
        filename=content_file.filename,
        chunk_size=chunk_size
    )

    # step 5 handoff to same background pipeline

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "queued"}
    backgroud_tasks.add_task(run_ingest_batch, [request], job_id)

    return{
        "job_id": job_id,
        "status": "queued",
        "filename": content_file.filename,
        "metadata_file": metadata_file.filename,
        "msg": "File ingestion started in background"  
    }




@router.get('/jobs/{job_id}')
async def get_job(job_id : str):
    if job_id not in jobs:
        return{"errror": "job not found"}
    else:
        return{
            "job_id": job_id,
            **jobs[job_id]
        }
    