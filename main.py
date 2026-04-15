from fastapi import FastAPI, HTTPException
import requests
from bs4 import BeautifulSoup

app = FastAPI(title="Handelsregister API")

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/search")
def search(schlagwoerter: str):
    try:
        session = requests.Session()
        url = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"
        
        session.get(url)
        
        data = {
            "form:schlagwoerter": schlagwoerter,
            "form:schlagwortOptionen": "1",
            "form:btnSuche": "Suchen",
        }
        
        response = session.post(url, data=data)
        soup = BeautifulSoup(response.text, "lxml")
        
        results = []
        rows = soup.select("table.ergebnisListe tr")
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) >= 3:
                results.append({
                    "firma": cols[1].get_text(strip=True),
                    "sitz": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "register": cols[3].get_text(strip=True) if len(cols) > 3 else ""
                })
        
        return {"query": schlagwoerter, "count": len(results), "results": results}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
