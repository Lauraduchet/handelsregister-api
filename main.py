from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
from bs4 import BeautifulSoup

app = FastAPI(title="Handelsregister API")

class SearchRequest(BaseModel):
    schlagwoerter: str

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/search")
def search(request: SearchRequest):
    try:
        session = requests.Session()
        url = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"
        
        # Get initial page for session
        session.get(url)
        
        # Search request
        data = {
            "form:schlagwoerter": request.schlagwoerter,
            "form:schlagwortOptionen": "1",
            "form:btnSuche": "Suchen",
        }
        
        response = session.post(url, data=data)
        soup = BeautifulSoup(response.text, "lxml")
        
        results = []
        rows = soup.select("table.ergebnisListe tr")
        for row in rows[1:]:  # Skip header
            cols = row.find_all("td")
            if len(cols) >= 3:
                results.append({
                    "firma": cols[1].get_text(strip=True),
                    "sitz": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "register": cols[3].get_text(strip=True) if len(cols) > 3 else ""
                })
        
        return {"query": request.schlagwoerter, "count": len(results), "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
