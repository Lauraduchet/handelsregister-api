from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import subprocess
import sys

app = FastAPI(title="Handelsregister API")

class SearchRequest(BaseModel):
    schlagwoerter: str
    schlagwortOptionen: Optional[str] = "all"

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/search")
def search(request: SearchRequest):
    try:
        result = subprocess.run(
            [sys.executable, "-m", "handelsregister", "-s", request.schlagwoerter, "-so", request.schlagwortOptionen],
            capture_output=True, text=True, timeout=60
        )
        return {"query": request.schlagwoerter, "output": result.stdout}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
