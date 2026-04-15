from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Handelsregister API")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

BASE_URL = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"


def get_viewstate(soup: BeautifulSoup) -> str:
    """Extract JSF ViewState token from page."""
    tag = soup.find("input", {"name": "javax.faces.ViewState"})
    if tag:
        return tag.get("value", "")
    # Alternativ: hidden field mit id
    tag = soup.find("input", {"id": "j_id1:javax.faces.ViewState:0"})
    if tag:
        return tag.get("value", "")
    return ""


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # Schritt 1: GET — Seite laden + ViewState holen
        resp1 = session.get(BASE_URL, timeout=30)
        resp1.raise_for_status()

        soup1 = BeautifulSoup(resp1.text, "lxml")
        viewstate = get_viewstate(soup1)

        logger.info(f"GET status={resp1.status_code}, ViewState={'found' if viewstate else 'MISSING'}")

        if not viewstate:
            logger.warning(f"Page snippet: {resp1.text[:500]}")
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": "ViewState nicht gefunden — Seite evtl. geblockt oder veraendert."
            }

        # Schritt 2: POST — Suche absenden mit ViewState
        form_data = {
            "form": "form",
            "form:schlagwoerter": schlagwoerter,
            "form:schlagwortOptionen": "1",
            "form:btnSuche": "Suchen",
            "javax.faces.ViewState": viewstate,
        }

        resp2 = session.post(BASE_URL, data=form_data, timeout=30)
        resp2.raise_for_status()

        logger.info(f"POST status={resp2.status_code}, length={len(resp2.text)}")

        soup2 = BeautifulSoup(resp2.text, "lxml")

        # Schritt 3: Ergebnisse parsen
        results = []

        # Variante 1: Tabelle mit class ergebnisListe
        rows = soup2.select("table.ergebnisListe tr")

        # Variante 2: Falls andere Klasse/Struktur
        if not rows:
            rows = soup2.select("table.resultList tr")
        if not rows:
            rows = soup2.select("table tbody tr")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                results.append({
                    "firma": cols[1].get_text(strip=True),
                    "sitz": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "register": cols[3].get_text(strip=True) if len(cols) > 3 else "",
                })

        # Debug: wenn immer noch nichts, logge einen Snippet
        if not results:
            snippet = soup2.get_text(separator=" ", strip=True)[:500]
            logger.info(f"Keine Treffer. Page-Text: {snippet}")

        return {
            "query": schlagwoerter,
            "count": len(results),
            "results": results,
        }

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Handelsregister timeout")
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
        raise HTTPException(status_code=502, detail=f"Handelsregister nicht erreichbar: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
