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


def resolve_form_action(action: str, page_url: str) -> str:
    """Resolve form action relative to the page URL."""
    if not action or action == "#" or action == "":
        return page_url
    if action.startswith("http"):
        return action
    if action.startswith("/"):
        return "https://www.handelsregister.de" + action
    # relative
    base = page_url.rsplit("/", 1)[0]
    return base + "/" + action


def accept_cookie_consent(session: requests.Session) -> dict:
    """Lade Welcome-Seite und akzeptiere den Cookie-Consent."""
    logger.info("Schritt 1: GET Welcome-Seite")
    resp = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
    page_url = str(resp.url)
    logger.info(f"  -> status={resp.status_code}, url={page_url}")

    soup = BeautifulSoup(resp.text, "lxml")

    # Alle Forms und Buttons sammeln fuer Debug
    all_forms = soup.find_all("form")
    logger.info(f"  -> {len(all_forms)} Form(s) gefunden")

    # Suche nach dem Consent-Button / Link
    # Methode 1: Button mit "Verstanden"
    consent_btn = None
    for btn in soup.find_all(["button", "input", "a"]):
        text = btn.get_text(strip=True) or btn.get("value", "")
        if "verstanden" in text.lower():
            consent_btn = btn
            logger.info(f"  -> Consent-Element gefunden: <{btn.name}> text='{text}'")
            break

    if not consent_btn:
        logger.warning("Kein Verstanden-Button gefunden")
        return {"ok": False, "reason": "no_button", "page_text": soup.get_text(separator=" ", strip=True)[:300]}

    # Finde das umgebende Form
    form = consent_btn.find_parent("form")
    if not form:
        form = all_forms[0] if all_forms else None

    if not form:
        return {"ok": False, "reason": "no_form"}

    form_action = resolve_form_action(form.get("action", ""), page_url)
    form_id = form.get("id", "")
    logger.info(f"  -> Form id='{form_id}', action='{form_action}'")

    # Alle hidden inputs + sonstige inputs sammeln
    form_data = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        value = inp.get("value", "")
        if name:
            form_data[name] = value

    # Button-spezifisch
    btn_name = consent_btn.get("name")
    btn_value = consent_btn.get("value", "")
    if btn_name:
        form_data[btn_name] = btn_value

    # Falls es ein <a> mit onclick / jsf.ajax ist
    if consent_btn.name == "a":
        onclick = consent_btn.get("onclick", "")
        btn_id = consent_btn.get("id", "")
        if btn_id:
            form_data[btn_id] = btn_id
        # JSF partial submit
        if "mojarra" in onclick.lower() or "jsf" in onclick.lower() or "ajax" in onclick.lower():
            form_data["javax.faces.partial.ajax"] = "true"
            form_data["javax.faces.source"] = btn_id
            form_data["javax.faces.partial.execute"] = "@all"
            form_data["javax.faces.partial.render"] = "@all"

    logger.info(f"  -> POST form_data keys: {list(form_data.keys())}")

    resp2 = session.post(form_action, data=form_data, timeout=30, allow_redirects=True)
    logger.info(f"  -> Consent POST status={resp2.status_code}, url={resp2.url}, length={len(resp2.text)}")

    return {"ok": resp2.status_code == 200, "status": resp2.status_code, "url": str(resp2.url)}


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # ---- Cookie-Consent ----
        consent = accept_cookie_consent(session)
        logger.info(f"Consent-Ergebnis: {consent}")

        # ---- Suchseite laden ----
        logger.info(f"Schritt 3: GET Suchseite {SEARCH_URL}")
        resp_form = session.get(SEARCH_URL, timeout=30, allow_redirects=True)
        logger.info(f"  -> status={resp_form.status_code}, url={resp_form.url}, length={len(resp_form.text)}")

        # Falls 400 -> versuche Welcome-URL (evtl. Redirect nach Consent)
        if resp_form.status_code != 200:
            logger.info("Suchseite nicht 200 — versuche Welcome-URL")
            resp_form = session.get(WELCOME_URL, timeout=30, allow_redirects=True)
            logger.info(f"  -> Fallback status={resp_form.status_code}")

        soup_form = BeautifulSoup(resp_form.text, "lxml")
        viewstate = extract_viewstate(soup_form)

        if not viewstate:
            page_text = soup_form.get_text(separator=" ", strip=True)[:300]
            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "error": "ViewState nicht gefunden nach Consent.",
                "debug_snippet": page_text,
            }

        # ---- Form-Daten aus der Seite extrahieren ----
        form_tag = soup_form.find("form")
        form_id = form_tag.get("id", "form") if form_tag else "form"
        form_action = resolve_form_action(
            form_tag.get("action", "") if form_tag else "",
            str(resp_form.url),
        )

        form_data = {}
        if form_tag:
            for inp in form_tag.find_all("input", {"type": "hidden"}):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    form_data[name] = value

        form_data.update({
            form_id: form_id,
            f"{form_id}:schlagwoerter": schlagwoerter,
            f"{form_id}:schlagwortOptionen": "1",
            f"{form_id}:btnSuche": "Suchen",
            "javax.faces.ViewState": viewstate,
        })

        logger.info(f"Schritt 4: POST Suche an {form_action}")
        resp_search = session.post(form_action, data=form_data, timeout=30, allow_redirects=True)
        logger.info(f"  -> status={resp_search.status_code}, length={len(resp_search.text)}")

        # ---- Ergebnisse parsen ----
        soup_results = BeautifulSoup(resp_search.text, "lxml")
        results = []

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

            if any(kw in page_text.lower() for kw in ["keine treffer", "keine ergebnisse", "0 treffer"]):
                info_msg = "Keine Treffer im Handelsregister."
            else:
                info_msg = "Ergebnisse konnten nicht geparst werden."

            return {
                "query": schlagwoerter,
                "count": 0,
                "results": [],
                "info": info_msg,
                "debug_snippet": page_text[:300],
            }

        return {"query": schlagwoerter, "count": len(results), "results": results}

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Handelsregister Timeout")
    except requests.RequestException as e:
        logger.error(f"Request-Fehler: {e}")
        raise HTTPException(status_code=502, detail=f"Nicht erreichbar: {e}")
    except Exception as e:
        logger.error(f"Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
def debug_fetch():
    """Zeigt was nach Consent vom Handelsregister zurueckkommt."""
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        consent = accept_cookie_consent(session)

        r = session.get(SEARCH_URL, timeout=30, allow_redirects=True)
        soup = BeautifulSoup(r.text, "lxml")
        vs = extract_viewstate(soup)

        # Finde alle Forms + ihre IDs auf der Suchseite
        forms = [{"id": f.get("id"), "action": f.get("action")} for f in soup.find_all("form")]

        return {
            "consent": consent,
            "search_status": r.status_code,
            "search_final_url": str(r.url),
            "viewstate_found": bool(vs),
            "cookies": dict(session.cookies),
            "page_title": soup.title.string.strip() if soup.title and soup.title.string else None,
            "forms_on_page": forms,
            "page_snippet": soup.get_text(separator=" ", strip=True)[:500],
        }
    except Exception as e:
        return {"error": str(e)}
