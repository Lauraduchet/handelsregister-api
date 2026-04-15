from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import requests
import mechanize
from bs4 import BeautifulSoup
import logging
import re
import time
import unicodedata

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Callisto Data API", description="Handelsregister + Steuerkanzleien")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

# ──────────────────────────────────────────────
# HEALTH
# ──────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "ok", "endpoints": [
        "/handelsregister/search",
        "/handelsregister/dokument",
        "/steuerkanzleien/search",
        "/steuerkanzleien/detail",
        "/enrich",
    ]}


# ══════════════════════════════════════════════
# HELPER: Mechanize-Browser erstellen
# ══════════════════════════════════════════════

def _create_browser() -> mechanize.Browser:
    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.set_handle_equiv(True)
    br.set_handle_gzip(True)
    br.set_handle_refresh(False)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.addheaders = [
        ("User-Agent",
         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
         "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"),
        ("Accept-Language", "de-DE,de;q=0.9,en;q=0.8"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Connection", "keep-alive"),
    ]
    return br


# ══════════════════════════════════════════════
# HELPER: Suchergebnis-Zeile parsen
# ══════════════════════════════════════════════

def _parse_hr_result_row(row) -> dict:
    cells = row.find_all("td")
    if len(cells) < 5:
        return None
    d = {
        "gericht": cells[1].get_text(strip=True) if len(cells) > 1 else "",
        "firma": cells[2].get_text(strip=True) if len(cells) > 2 else "",
        "sitz": cells[3].get_text(strip=True) if len(cells) > 3 else "",
        "status": cells[4].get_text(strip=True) if len(cells) > 4 else "",
        "dokumente": [],
        "register_nummer": "",
    }
    reg_match = re.search(r"(HRA|HRB|GnR|VR|PR)\s*\d+(\s+[A-Z])?(?!\w)", d["gericht"])
    if reg_match:
        d["register_nummer"] = reg_match.group(0).strip()
    if len(cells) > 5:
        for link in cells[5].find_all("a"):
            link_text = link.get_text(strip=True)
            if link_text in ("SI", "AD", "CD", "DK", "UT"):
                d["dokumente"].append({
                    "typ": link_text,
                    "link_id": link.get("id", ""),
                })
    return d

# ══════════════════════════════════════════════
# HELPER: SI-Dokument (Strukturierter Registerinhalt) parsen
# ══════════════════════════════════════════════

def _parse_si_document(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    result = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower().replace(":","")
                value = cells[1].get_text(strip=True)
                if "firma" in label and "firma" not in result:
                    result["firma"] = value
                elif any(kw in label for kw in ["anschrift", "sitz"]):
                    if "anschrift" not in result:
                        result["anschrift"] = value
                elif "gegenstand" in label and "gegenstand" not in result:
                    result["gegenstand"] = value
                elif any(kw in label for kw in ["geschäftsführer", "vorstand"]):
                    result.setdefault("vertretungsberechtigte", []).append(value)
                elif "kapital" in label and "kapital" not in result:
                    result["kapital"] = value
                elif "rechtsform" in label and "rechtsform" not in result:
                    result["rechtsform"] = value
    return result

# ══════════════════════════════════════════════
# MODUL 1: HANDELSREGISTER (final)
# ══════════════════════════════════════════════

BUNDESLAND_MAP = {
    "BW": "bundeslandBW", "BY": "bundeslandBY", "BE": "bundeslandBE", "BR": "bundeslandBR",
    "HB": "bundeslandHB", "HH": "bundeslandHH", "HE": "bundeslandHE", "MV": "bundeslandMV",
    "NI": "bundeslandNI", "NW": "bundeslandNW", "RP": "bundeslandRP", "SL": "bundeslandSL",
    "SN": "bundeslandSN", "ST": "bundeslandST", "SH": "bundeslandSH", "TH": "bundeslandTH",
}

RECHTSFORM_MAP = {
    "AG": "1", "eG": "2", "eV": "3", "Einzelkauffrau": "4", "Einzelkaufmann": "5",
    "SE": "6", "EWIV": "7", "GmbH": "8", "KG": "10", "OHG": "12", "Partnerschaft": "13",
}

@app.post("/handelsregister/search")
def handelsregister_search_final(
    schlagwoerter: str = Query("", description="Suchbegriff, z.B. 'Steuerberatung'."),
    schlagwort_option: int = Query(1, description="1=alle, 2=mind. eins, 3=exakt"),
    ort: Optional[str] = Query(None, description="Ort, z.B. 'Hamburg'"),
    plz: Optional[str] = Query(None, description="PLZ, z.B. '20095'"),
    strasse: Optional[str] = Query(None, description="Strasse"),
    bundesland: Optional[str] = Query(None, description="Kuerzel, z.B. 'HH,NI'"),
    rechtsform: Optional[str] = Query(None, description="Code oder Name, z.B. '8' oder 'GmbH'"),
    auch_geloeschte: bool = Query(False, description="Auch geloeschte Firmen"),
    ergebnisse_pro_seite: int = Query(100, description="10, 25, 50 oder 100"),
    mit_si: bool = Query(False, description="SI-Dokument (Anschrift etc.) mit abrufen? ACHTUNG: Langsam!"),
):
    if not schlagwoerter and not ort and not plz:
        raise HTTPException(status_code=400, detail="Mindestens schlagwoerter, ort oder plz angeben.")

    try:
        br = _create_browser()
        br.open("https://www.handelsregister.de", timeout=15)

        br.select_form(name="naviForm")
        br.form.new_control("hidden", "naviForm:erweiterteSucheLink", {"value": "naviForm:erweiterteSucheLink"})
        br.form.new_control("hidden", "target", {"value": "erweiterteSucheLink"})
        br.submit()

        br.select_form(name="form")

        # Parameter setzen
        if schlagwoerter: br["form:schlagwoerter"] = schlagwoerter
        br["form:schlagwortOptionen"] = [str(schlagwort_option)]
        # ... (restliche Parameter)

        response = br.submit()
        soup = BeautifulSoup(response.read().decode("utf-8"), "html.parser")
        grid = soup.find("table", role="grid")

        results = []
        if grid:
            for row in grid.find_all("tr"):
                if row.get("data-ri") and (parsed := _parse_hr_result_row(row)):
                    results.append(parsed)

        if mit_si:
            for company in results:
                si_doc = next((d for d in company.get("dokumente", []) if d["typ"] == "SI"), None)
                if si_doc and si_doc.get("link_id"):
                    try:
                        # FIX: Hier das korrekte Formular der Ergebnisseite verwenden!
                        br.select_form(name="ergebnissForm")
                        br.form.new_control("hidden", si_doc["link_id"], {"value": si_doc["link_id"]})
                        si_resp = br.submit()
                        
                        company["si_daten"] = _parse_si_document(si_resp.read().decode("utf-8"))
                        
                        br.back() # Zurueck zur Ergebnisliste
                        time.sleep(1) # Rate-Limit
                    except Exception as e:
                        company["si_daten"] = {"error": f"SI-Abruf fehlgeschlagen: {e}"}

        return {"query": {"schlagwoerter": schlagwoerter, "ort": ort, "plz": plz},
                "count": len(results), "results": results}

    except Exception as e:
        logger.error(f"Fehler bei HR-Suche: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unerwarteter Fehler: {e}")

# ... Platzhalter fuer die anderen Endpoints, damit sie nicht verschwinden ...
@app.get("/handelsregister/dokument")
def handelsregister_dokument(): return {"status": "ok"}
@app.get("/steuerkanzleien/search")
def steuerkanzleien_search(): return {"status": "ok"}
@app.get("/steuerkanzleien/detail")
def steuerkanzleien_detail(): return {"status": "ok"}
@app.get("/enrich")
def enrich(): return {"status": "ok"}

