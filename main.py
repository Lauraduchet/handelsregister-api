from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
import logging
import re

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Handelsregister API")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

WELCOME_URL = "https://www.handelsregister.de/rp_web/erweitertesuche/welcome.xhtml"
SEARCH_URL = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"


def extract_viewstate(soup: BeautifulSoup) -> str:
    tag = soup.find("input", {"name": "javax.faces.ViewState"})
    if tag and tag.get("value"):
        return tag["value"]
    tag = soup.find("input", {"id": re.compile(r"ViewState")})
    if tag and tag.get("value"):
        return tag["value"]
    return ""


def accept_cookie_consent(session: requests.Session) -> bool:
    """Lade Welcome-Seite und akzeptiere den Cookie-Consent."""
    logger.info("Schritt 1: GET Welcome-Seite")
    resp = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
    logger.info(f"  -> status={resp.status_code}, url={resp.url}")

    soup = BeautifulSoup(resp.text, "lxml")
    viewstate = extract_viewstate(soup)

    # Finde den "Verstanden"-Button / Consent-Form
    # Typisch: ein Form mit einem Submit-Button "Verstanden"
    consent_btn = soup.find("button", string=re.compile(r"Verstanden", re.I))
    if not consent_btn:
        consent_btn = soup.find("input", {"value": re.compile(r"Verstanden", re.I)})
    if not consent_btn:
        # Suche nach Link
        consent_link = soup.find("a", string=re.compile(r"Verstanden", re.I))
        if consent_link and consent_link.get("href"):
            logger.info(f"  -> Consent-Link gefunden: {consent_link['href']}")
            r2 = session.get(
                "https://www.handelsregister.de" + consent_link["href"]
                if consent_link["href"].startswith("/")
                else consent_link["href"],
                timeout=30,
                allow_redirects=True,
            )
            logger.info(f"  -> Consent-Link status={r2.status_code}")
            return r2.status_code == 200

    # Finde das umgebende Form
    form = soup.find("form")
    if not form:
        logger.warning("Kein Form auf Welcome-Seite gefunden")
        return False

    form_action = form.get("action", "")
    if form_action and not form_action.startswith("http"):
        form_action = "https://www.handelsregister.de" + form_action

    # Alle hidden inputs sammeln
    form_data = {}
    for inp in form.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        value = inp.get("value", "")
        if name:
            form_data[name] = value

    # Button-Name finden
    if consent_btn:
        btn_name = consent_btn.get("name")
        btn_value = consent_btn.get("value", "Verstanden")
        if btn_name:
            form_data[btn_name] = btn_value

    # ViewState sicherstellen
    if viewstate and "javax.faces.ViewState" not in form_data:
        form_data["javax.faces.ViewState"] = viewstate

    post_url = form_action or WELCOME_URL
    logger.info(f"Schritt 2: POST Consent an {post_url}")
    logger.info(f"  -> form_data keys: {list(form_data.keys())}")

    resp2 = session.post(post_url, data=form_data, timeout=30, allow_redirects=True)
    logger.info(f"  -> status={resp2.status_code}, url={resp2.url}, length={len(resp2.text)}")

    return resp2.status_code == 200


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # ---- Cookie-Consent akzeptieren ----
        consent_ok = accept_cookie_consent(session)
        logger.info(f"Consent akzeptiert: {consent_ok}")

        # ---- Suchseite laden (ViewState holen) ----
        logger.info(f"Schritt 3: GET Suchseite {SEARCH_URL}")
        resp_form = session.get(SEARCH_URL, timeout=30, allow_redirects=True)
        logger.info(
            f"  -> status={resp_form.status_code}, "
            f"url={resp_form.url}, length={len(resp_form.text)}"
        )

        if resp_form.status_code != 200:
            # Fallback: vielleicht sind wir schon auf der Suchseite nach Consent
            logger.info("Suchseite nicht 200 — versuche Welcome-URL als Fallback")
            resp_form = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
            logger.info(f"  -> Fallback status={resp_form.status_code}")

        soup_form = BeautifulSoup(resp_form.text, "lxml")
        viewstate = extract_viewstate(soup_form)

        page_text = soup_form.get_text(separator=" ", strip=True)[:300]
        logger.info(f"  -> ViewState: {'gefunden' if viewstate else 'NICHT GEFUNDEN'}")
        logger.info(f"  -> Seitentext: {page_text[:200]}")

        if not viewstate:
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": "ViewState nicht gefunden nach Consent.",
                "debug_snippet": page_text,
            }

        # ---- Suche absenden ----
        # Finde den form-Namen aus der Seite
        form_tag = soup_form.find("form")
        form_id = form_tag.get("id", "form") if form_tag else "form"

        form_data = {}
        # Alle hidden inputs
        if form_tag:
            for inp in form_tag.find_all("input", {"type": "hidden"}):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    form_data[name] = value

        # Suchparameter setzen
        form_data.update({
            form_id: form_id,
            f"{form_id}:schlagwoerter": schlagwoerter,
            f"{form_id}:schlagwortOptionen": "1",
            f"{form_id}:btnSuche": "Suchen",
            "javax.faces.ViewState": viewstate,
        })

        form_action = SEARCH_URL
        if form_tag and form_tag.get("action"):
            fa = form_tag["action"]
            if fa.startswith("/"):
                form_action = "https://www.handelsregister.de" + fa
            elif fa.startswith("http"):
                form_action = fa

        logger.info(f"Schritt 4: POST Suche an {form_action}")
        resp_search = session.post(
            form_action,
            data=form_data,
            timeout=30,
            allow_redirects=True,
        )
        logger.info(
            f"  -> status={resp_search.status_code}, length={len(resp_search.text)}"
        )

        # ---- Ergebnisse parsen ----
        soup_results = BeautifulSoup(resp_search.text, "lxml")
        results = []

        # Verschiedene Selektoren
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
            page_text = soup_results.get_text(separator=" ", strip=True)[:500]
            logger.info(f"Keine Treffer. Seitentext: {page_text}")

            if any(
                kw in page_text.lower()
                for kw in ["keine treffer", "keine ergebnisse", "no results", "0 treffer"]
            ):
                info_msg = "Keine Treffer im Handelsregister fuer diese Suche."
            else:
                info_msg = "Ergebnisse konnten nicht geparst werden."

            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "info": info_msg,
                "debug_snippet": page_text[:300],
            }

        return {
            "query": schlagwoerter,
            "count": len(results),
            "results": results,
        }

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Handelsregister Timeout")
    except requests.RequestException as e:
        logger.error(f"Request-Fehler: {e}")
        raise HTTPException(status_code=502, detail=f"Handelsregister nicht erreichbar: {e}")
    except Exception as e:
        logger.error(f"Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
def debug_fetch():
    """Zeigt was nach Consent vom Handelsregister zurueckkommt."""
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        consent_ok = accept_cookie_consent(session)

        r = session.get(SEARCH_URL, timeout=30, allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        vs = extract_viewstate(soup)

        return {
            "consent_accepted": consent_ok,
            "search_status": r.status_code,
            "search_final_url": str(r.url),
            "viewstate_found": bool(vs),
            "cookies": dict(session.cookies),
            "page_title": soup.title.string.strip() if soup.title else None,
            "page_snippet": soup.get_text(separator=" ", strip=True)[:500],
        }
    except Exception as e:
        return {"error": str(e)}
