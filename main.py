from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
import logging
import re

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Handelsregister API")

# Browser-aehnliche Headers (wichtig gegen Anti-Bot)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

# Aktuelle URL (Mai 2024+)
SEARCH_URL = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"
WELCOME_URL = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"


def extract_viewstate(soup: BeautifulSoup) -> str:
    """JSF ViewState aus der Seite extrahieren."""
    # Methode 1: Standard hidden input
    tag = soup.find("input", {"name": "javax.faces.ViewState"})
    if tag and tag.get("value"):
        return tag["value"]
    # Methode 2: mit id
    tag = soup.find("input", {"id": re.compile(r"ViewState")})
    if tag and tag.get("value"):
        return tag["value"]
    return ""


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # ---- Schritt 1: Welcome-Seite laden (Cookie + evtl. Redirect) ----
        logger.info(f"Schritt 1: GET {WELCOME_URL}")
        resp_welcome = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
        logger.info(
            f"  -> status={resp_welcome.status_code}, "
            f"final_url={resp_welcome.url}, "
            f"length={len(resp_welcome.text)}"
        )

        # ---- Schritt 2: Suchseite laden (ViewState holen) ----
        logger.info(f"Schritt 2: GET {SEARCH_URL}")
        resp_form = session.get(SEARCH_URL, timeout=30, allow_redirects=True)
        logger.info(
            f"  -> status={resp_form.status_code}, "
            f"final_url={resp_form.url}, "
            f"length={len(resp_form.text)}"
        )

        if resp_form.status_code != 200:
            snippet = resp_form.text[:500]
            logger.error(f"Suchseite nicht erreichbar: {snippet}")
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": f"Handelsregister antwortet mit Status {resp_form.status_code}",
                "debug_snippet": snippet,
            }

        soup_form = BeautifulSoup(resp_form.text, "lxml")
        viewstate = extract_viewstate(soup_form)
        logger.info(f"  -> ViewState: {'gefunden (' + viewstate[:30] + '...)' if viewstate else 'NICHT GEFUNDEN'}")

        if not viewstate:
            # Seite evtl. Consent/Block — zeige was wir bekommen haben
            page_text = soup_form.get_text(separator=" ", strip=True)[:500]
            logger.warning(f"Kein ViewState! Seitentext: {page_text}")
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": "ViewState nicht gefunden. Seite evtl. geblockt oder veraendert.",
                "debug_snippet": page_text,
            }

        # ---- Schritt 3: POST — Suche absenden ----
        form_data = {
            "form": "form",
            "form:schlagwoerter": schlagwoerter,
            "form:schlagwortOptionen": "1",
            "form:btnSuche": "Suchen",
            "javax.faces.ViewState": viewstate,
        }

        logger.info(f"Schritt 3: POST {SEARCH_URL} mit schlagwoerter='{schlagwoerter}'")
        resp_search = session.post(
            SEARCH_URL,
            data=form_data,
            timeout=30,
            allow_redirects=True,
        )
        logger.info(
            f"  -> status={resp_search.status_code}, "
            f"length={len(resp_search.text)}"
        )

        if resp_search.status_code != 200:
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": f"Suche fehlgeschlagen mit Status {resp_search.status_code}",
            }

        # ---- Schritt 4: Ergebnisse parsen ----
        soup_results = BeautifulSoup(resp_search.text, "lxml")
        results = []

        # Mehrere Selektoren probieren (Seite aendert sich gelegentlich)
        rows = (
            soup_results.select("table.ergebnisListe tr")
            or soup_results.select("table.resultList tr")
            or soup_results.select("table.RegPortErworben_ergebnisListe tr")
            or soup_results.select("#ergebnisForm table tr")
            or soup_results.select("table tbody tr")
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
            page_text = soup_results.get_text(separator=" ", strip=True)[:800]
            logger.info(f"Keine Treffer gefunden. Seitentext: {page_text}")

            # Prüfe ob "keine Ergebnisse" oder ob Parsing-Problem
            if any(
                kw in page_text.lower()
                for kw in ["keine treffer", "keine ergebnisse", "no results", "0 treffer"]
            ):
                info = "Keine Treffer im Handelsregister fuer diese Suche."
            else:
                info = "Ergebnisse konnten nicht geparst werden. Seitenstruktur evtl. veraendert."

            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "info": info,
                "debug_snippet": page_text[:300],
            }

        return {
            "query": schlagwoerter,
            "count": len(results),
            "results": results,
        }

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Handelsregister Timeout (30s)")
    except requests.RequestException as e:
        logger.error(f"Request-Fehler: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Handelsregister nicht erreichbar: {e}",
        )
    except Exception as e:
        logger.error(f"Unerwarteter Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
def debug_fetch():
    """Debug-Endpoint: zeigt was wir vom Handelsregister zurueckbekommen."""
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        r1 = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
        r2 = session.get(SEARCH_URL, timeout=30, allow_redirects=True)

        soup = BeautifulSoup(r2.text, "lxml")
        vs = extract_viewstate(soup)

        return {
            "welcome_status": r1.status_code,
            "welcome_final_url": str(r1.url),
            "search_status": r2.status_code,
            "search_final_url": str(r2.url),
            "viewstate_found": bool(vs),
            "cookies": dict(session.cookies),
            "page_title": soup.title.string if soup.title else None,
            "page_snippet": soup.get_text(separator=" ", strip=True)[:500],
        }
    except Exception as e:
        return {"error": str(e)}
