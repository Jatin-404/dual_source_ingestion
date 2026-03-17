from fastapi import APIRouter
from pydantic import BaseModel
from httpx import AsyncClient

router = APIRouter(prefix="/search",tags=['Search'] )
SEARCH_URL  = "http://localhost:8004"


class SearchRequest(BaseModel):
    query: str
    top_k: int = 5
    court: str | None = None
    case_number: str | None = None
    date: str | None = None
    lawyer: str | None = None




@router.post('/')
async def search(request: SearchRequest):
    async with AsyncClient(timeout=60) as client:
        response = await client.post(f"{SEARCH_URL}/search",
                               json=request.model_dump())
        
        return response.json()
    
