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
    "Accept-Language": "de-DE,de;q=0.9",
}

# Laut bundesAPI Doku: einfacher POST an diese URL
SEARCH_URL = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"
WELCOME_URL = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"


def get_session_with_cookie() -> requests.Session:
    """Session mit akzeptiertem Cookie erstellen."""
    session = requests.Session()
    session.headers.update(HEADERS)
    # Cookie direkt setzen statt Consent-Flow
    session.cookies.set("cookieAccepted", "true", domain="www.handelsregister.de")
    # Welcome-Seite aufrufen um JSESSIONID zu bekommen
    session.get(WELCOME_URL, timeout=30, allow_redirects=True)
    return session


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        session = get_session_with_cookie()

        # Laut bundesAPI Doku: einfache Parameter, KEIN "form:" Prefix
        post_data = {
            "schlagwoerter": schlagwoerter,
            "schlagwortOptionen": 1,
            "btnSuche": "Suchen",
            "suchTyp": "e",
            "ergebnisseProSeite": 100,
        }

        logger.info(f"POST {SEARCH_URL} mit schlagwoerter='{schlagwoerter}'")
        resp = session.post(SEARCH_URL, data=post_data, timeout=30, allow_redirects=True)
        logger.info(f"  -> status={resp.status_code}, length={len(resp.text)}, url={resp.url}")

        if resp.status_code != 200:
            snippet = resp.text[:300] if resp.text else "leer"
            logger.error(f"  -> Fehler-Snippet: {snippet}")
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": f"Status {resp.status_code}",
                "debug_snippet": snippet,
            }

        soup = BeautifulSoup(resp.text, "lxml")
        results = []

        rows = (
            soup.select("table.ergebnisListe tr")
            or soup.select("table.resultList tr")
            or soup.select("table tbody tr")
        )

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 3:
                results.append({
                    "firma": cols[1].get_text(strip=True) if len(cols) > 1 else "",
                    "sitz": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "register": cols[3].get_text(strip=True) if len(cols) > 3 else "",
                    "status": cols[4].get_text(strip=True) if len(cols) > 4 else "",
                })

        if not results:
            page_text = soup.get_text(separator=" ", strip=True)[:500]
            logger.info(f"Keine Treffer. Text: {page_text}")
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "debug_snippet": page_text[:300],
            }

        return {"query": schlagwoerter, "count": len(results), "results": results}

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Timeout")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        logger.error(f"Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
def debug_fetch():
    """Zeigt was wir vom Handelsregister bekommen."""
    try:
        session = get_session_with_cookie()
        logger.info(f"Cookies nach Welcome: {dict(session.cookies)}")

        # Teste POST wie die bundesAPI Doku es beschreibt
        post_data = {
            "schlagwoerter": "Siemens",
            "schlagwortOptionen": 1,
            "btnSuche": "Suchen",
            "suchTyp": "e",
            "ergebnisseProSeite": 10,
        }
        resp = session.post(SEARCH_URL, data=post_data, timeout=30, allow_redirects=True)

        soup = BeautifulSoup(resp.text, "lxml")
        tables = soup.find_all("table")

        return {
            "cookies": dict(session.cookies),
            "post_status": resp.status_code,
            "post_final_url": str(resp.url),
            "content_length": len(resp.text),
            "tables_found": len(tables),
            "page_title": soup.title.string.strip() if soup.title and soup.title.string else None,
            "page_snippet": soup.get_text(separator=" ", strip=True)[:500],
        }
    except Exception as e:
        return {"error": str(e)}
