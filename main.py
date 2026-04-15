from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import requests
import mechanize
from bs4 import BeautifulSoup
import logging
import re
import time
import unicodedata
import urllib.parse

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
# HELPER: Mechanize-Browser
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
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"),
        ("Accept-Language", "de-DE,de;q=0.9,en;q=0.8"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Connection", "keep-alive"),
    ]
    return br


# ══════════════════════════════════════════════
# HELPER: Erweiterte Suche oeffnen
# ══════════════════════════════════════════════

def _open_erweiterte_suche(br: mechanize.Browser):
    """Oeffnet handelsregister.de und navigiert zur erweiterten Suche."""
    br.open("https://www.handelsregister.de", timeout=15)
    br.select_form(name="naviForm")
    br.form.new_control("hidden", "naviForm:erweiterteSucheLink",
                        {"value": "naviForm:erweiterteSucheLink"})
    br.form.new_control("hidden", "target",
                        {"value": "erweiterteSucheLink"})
    br.submit()


# ══════════════════════════════════════════════
# HELPER: Formularfelder debuggen
# ══════════════════════════════════════════════

def _debug_form_controls(br: mechanize.Browser) -> list:
    """Gibt alle Control-Namen des aktuellen Formulars zurueck (fuer Debugging)."""
    controls = []
    for ctrl in br.form.controls:
        controls.append({
            "name": ctrl.name,
            "type": ctrl.type,
            "value": ctrl.value if hasattr(ctrl, "value") else "",
        })
    return controls


# ══════════════════════════════════════════════
# HELPER: Formularfeld sicher setzen
# ══════════════════════════════════════════════

def _set_form_field(br: mechanize.Browser, field_name: str, value, is_list=False):
    """Setzt ein Formularfeld. Wirft einen klaren Fehler statt silent pass."""
    try:
        if is_list:
            br[field_name] = value if isinstance(value, list) else [str(value)]
        else:
            br[field_name] = value
        return True
    except mechanize.ControlNotFoundError:
        # Versuche alternative Feldnamen (form: prefix varianten)
        logger.warning(f"Feld '{field_name}' nicht gefunden. Verfuegbare Controls: "
                       f"{[c.name for c in br.form.controls]}")
        return False


# ══════════════════════════════════════════════
# HELPER: HR-Suchergebnis-Zeile parsen
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
        "dokumente": [], "register_nummer": "",
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
                    "onclick": link.get("onclick", ""),
                })
    return d


# ══════════════════════════════════════════════
# HELPER: SI-Dokument via JSF-POST abrufen
# ══════════════════════════════════════════════

def _fetch_si_document(br: mechanize.Browser, si_link_id: str) -> str:
    """
    Ruft ein SI-Dokument ab, indem der JSF-POST korrekt simuliert wird.
    
    Der SI-Link ist kein normaler Hyperlink, sondern ein PrimeFaces/JSF
    CommandLink, der per onclick ein Formular submittet. Wir muessen:
    1. Das ergebnissForm selektieren
    2. Den Link-ID als javax.faces.source setzen
    3. Den Submit ausfuehren
    """
    # Versuche zunaechst das Formular 'ergebnissForm' zu finden
    form_found = False
    for form in br.forms():
        if form.name and "ergebniss" in form.name.lower():
            br.select_form(name=form.name)
            form_found = True
            break

    if not form_found:
        # Fallback: alle Formulare durchgehen und das mit dem SI-Link finden
        for form in br.forms():
            br.form = form
            for ctrl in form.controls:
                if ctrl.name and si_link_id in str(ctrl.name):
                    form_found = True
                    break
            if form_found:
                break

    if not form_found:
        # Letzter Fallback: erstes nicht-navi Formular
        for form in br.forms():
            if form.name and form.name != "naviForm":
                br.select_form(name=form.name)
                form_found = True
                break

    if not form_found:
        raise Exception("Kein passendes Formular fuer SI-Abruf gefunden")

    form_name = br.form.name or "ergebnissForm"

    # javax.faces ViewState extrahieren
    try:
        vs_ctrl = br.form.find_control("javax.faces.ViewState")
        view_state = vs_ctrl.value
    except mechanize.ControlNotFoundError:
        view_state = None

    # JSF-POST Parameter setzen
    # Der SI-Link-Klick sendet das Formular mit dem Link-ID als source
    try:
        br.form.new_control("hidden", si_link_id, {"value": si_link_id})
    except Exception:
        pass

    try:
        br.form.new_control("hidden", "javax.faces.source", {"value": si_link_id})
    except Exception:
        pass

    try:
        br.form.new_control("hidden", "javax.faces.partial.ajax", {"value": "false"})
    except Exception:
        pass

    # Fix fuer mechanize readonly controls
    for ctrl in br.form.controls:
        ctrl.readonly = False

    response = br.submit()
    return response.read().decode("utf-8")


# ══════════════════════════════════════════════
# HELPER: SI-Dokument HTML parsen
# ══════════════════════════════════════════════

def _parse_si_document(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    result = {"firma": "", "anschrift": "", "plz_ort": "", "gegenstand": "",
              "geschaeftsfuehrer": [], "kapital": "", "rechtsform": "", "vertretung": "",
              "raw_text_snippet": text[:2000]}

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)

            if "firma" in label and not result["firma"]:
                result["firma"] = value
            elif any(kw in label for kw in ["anschrift", "geschaeftsanschrift",
                                             "geschäftsanschrift", "sitz"]):
                if not result["anschrift"]:
                    result["anschrift"] = value
            elif "gegenstand" in label and not result["gegenstand"]:
                result["gegenstand"] = value
            elif any(kw in label for kw in ["geschäftsführer", "geschaeftsfuehrer",
                                             "vorstand", "persoenlich"]):
                result["geschaeftsfuehrer"].append(value)
            elif "kapital" in label:
                result["kapital"] = value
            elif "rechtsform" in label:
                result["rechtsform"] = value
            elif "vertretung" in label:
                result["vertretung"] = value
    return result


# ══════════════════════════════════════════════
# MODUL 1: HANDELSREGISTER
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


# Mapping: API-Parameter -> moegliche mechanize Form-Control-Namen (in Praeferenzreihenfolge)
# Falls sich die JSF-IDs aendern, hier anpassen.
FIELD_CANDIDATES = {
    "schlagwoerter": ["form:schlagwoerter"],
    "schlagwortOptionen": ["form:schlagwortOptionen"],
    "ergebnisseProSeite": ["form:ergebnisseProSeite"],
    "ort": ["form:ort", "form:niederlassung"],
    "postleitzahl": ["form:postleitzahl", "form:plz"],
    "strasse": ["form:strasse"],
    "rechtsform": ["form:rechtsform"],
}


def _find_and_set(br: mechanize.Browser, param_key: str, value, is_list=False) -> bool:
    """Versucht ein Feld ueber die Kandidatenliste zu setzen. Loggt Fehler."""
    candidates = FIELD_CANDIDATES.get(param_key, [f"form:{param_key}"])
    for name in candidates:
        try:
            if is_list:
                br[name] = value if isinstance(value, list) else [str(value)]
            else:
                br[name] = str(value)
            logger.info(f"Feld '{param_key}' -> '{name}' erfolgreich gesetzt auf: {value}")
            return True
        except (mechanize.ControlNotFoundError, mechanize.ItemNotFoundError):
            continue
    
    # Keiner der Kandidaten hat gepasst - logge verfuegbare Felder
    available = [c.name for c in br.form.controls if c.name]
    logger.error(f"FEHLER: Feld '{param_key}' nicht gefunden! "
                 f"Kandidaten: {candidates}, Verfuegbar: {available}")
    return False


@app.post("/handelsregister/search")
def handelsregister_search(
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
    debug_fields: bool = Query(False, description="Debug: Gibt alle Formularfelder zurueck"),
):
    if not schlagwoerter and not ort and not plz:
        raise HTTPException(status_code=400, detail="Mindestens schlagwoerter, ort oder plz angeben.")

    field_errors = []

    try:
        br = _create_browser()
        _open_erweiterte_suche(br)
        br.select_form(name="form")

        # Debug-Modus: Formularfelder ausgeben
        if debug_fields:
            controls = _debug_form_controls(br)
            return {"debug_form_controls": controls}

        # --- Felder setzen mit klarem Fehler-Reporting ---
        if schlagwoerter:
            if not _find_and_set(br, "schlagwoerter", schlagwoerter):
                field_errors.append("schlagwoerter")

        if not _find_and_set(br, "schlagwortOptionen", str(schlagwort_option), is_list=True):
            field_errors.append("schlagwortOptionen")

        _find_and_set(br, "ergebnisseProSeite", str(ergebnisse_pro_seite), is_list=True)

        if ort:
            if not _find_and_set(br, "ort", ort):
                field_errors.append("ort")

        if plz:
            if not _find_and_set(br, "postleitzahl", plz):
                field_errors.append("postleitzahl")

        if strasse:
            if not _find_and_set(br, "strasse", strasse):
                field_errors.append("strasse")

        if bundesland:
            for bl in bundesland.upper().split(","):
                bl = bl.strip()
                if bl in BUNDESLAND_MAP:
                    ctrl_name = f"form:{BUNDESLAND_MAP[bl]}"
                    try:
                        br.find_control(ctrl_name).value = ["on"]
                    except Exception:
                        field_errors.append(f"bundesland_{bl}")

        if rechtsform:
            rf_code = RECHTSFORM_MAP.get(rechtsform.strip(), rechtsform.strip())
            if not _find_and_set(br, "rechtsform", rf_code, is_list=True):
                field_errors.append("rechtsform")

        if auch_geloeschte:
            try:
                br.find_control("form:suchOptionenGeloescht").value = ["true"]
            except Exception:
                field_errors.append("suchOptionenGeloescht")

        # --- Suche absenden ---
        response = br.submit()
        result_html = response.read().decode("utf-8")
        soup = BeautifulSoup(result_html, "html.parser")
        grid = soup.find("table", role="grid")

        results = []
        if grid:
            for row in grid.find_all("tr"):
                if row.get("data-ri") is not None:
                    parsed = _parse_hr_result_row(row)
                    if parsed:
                        results.append(parsed)

        # --- SI-Dokumente abrufen ---
        if mit_si and results:
            for idx, company in enumerate(results):
                si_doc = next((d for d in company.get("dokumente", []) if d["typ"] == "SI"), None)
                if si_doc and si_doc.get("link_id"):
                    try:
                        si_html = _fetch_si_document(br, si_doc["link_id"])
                        company["si_daten"] = _parse_si_document(si_html)

                        # Nach SI-Abruf: zurueck zur Ergebnisliste navigieren
                        # Statt br.back() die Suche neu ausfuehren ist zuverlaessiger
                        # bei JSF, aber wir versuchen zunaechst back()
                        try:
                            br.back()
                        except Exception:
                            # Falls back() fehlschlaegt: Suche erneut starten
                            _open_erweiterte_suche(br)
                            br.select_form(name="form")
                            if schlagwoerter:
                                _find_and_set(br, "schlagwoerter", schlagwoerter)
                            _find_and_set(br, "schlagwortOptionen", str(schlagwort_option), is_list=True)
                            if ort:
                                _find_and_set(br, "ort", ort)
                            br.submit()

                        time.sleep(0.8)
                    except Exception as e:
                        company["si_daten"] = {"error": f"SI-Abruf fehlgeschlagen: {str(e)}"}
                else:
                    company["si_daten"] = {"error": "Kein SI-Link verfuegbar"}

        response_data = {
            "query": {
                "schlagwoerter": schlagwoerter, "ort": ort, "plz": plz,
                "strasse": strasse, "bundesland": bundesland, "rechtsform": rechtsform,
            },
            "count": len(results),
            "results": results,
        }

        if field_errors:
            response_data["_field_warnings"] = (
                f"Folgende Felder konnten NICHT gesetzt werden: {field_errors}. "
                f"Die Suche lief ohne diese Filter!"
            )

        return response_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════
# DEBUG-Endpoint: Formularfelder anzeigen
# ══════════════════════════════════════════════

@app.get("/handelsregister/debug-fields")
def handelsregister_debug_fields():
    """Gibt alle verfuegbaren Formularfelder der erweiterten Suche zurueck."""
    try:
        br = _create_browser()
        _open_erweiterte_suche(br)
        br.select_form(name="form")
        controls = _debug_form_controls(br)
        return {"form_name": br.form.name, "controls": controls}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════
# Einzeldokument abrufen
# ══════════════════════════════════════════════

@app.get("/handelsregister/dokument")
def handelsregister_dokument(firma: str = Query(..., description="Exakter Firmenname"),
                             typ: str = Query("SI", description="Dokumenttyp: SI, AD, CD")):
    try:
        br = _create_browser()
        _open_erweiterte_suche(br)

        br.select_form(name="form")
        _find_and_set(br, "schlagwoerter", firma)
        _find_and_set(br, "schlagwortOptionen", "3", is_list=True)
        response = br.submit()
        soup = BeautifulSoup(response.read().decode("utf-8"), "html.parser")
        grid = soup.find("table", role="grid")

        if not grid:
            raise HTTPException(status_code=404, detail="Firma nicht gefunden")

        first_row = next(
            (row for row in grid.find_all("tr") if row.get("data-ri") is not None), None
        )
        if not first_row:
            raise HTTPException(status_code=404, detail="Firma nicht gefunden")

        parsed = _parse_hr_result_row(first_row)
        target_doc = next((d for d in parsed.get("dokumente", []) if d["typ"] == typ), None)

        if not target_doc or not target_doc.get("link_id"):
            raise HTTPException(status_code=404, detail=f"{typ}-Dokument nicht verfuegbar.")

        # JSF-POST fuer Dokument-Abruf
        doc_html = _fetch_si_document(br, target_doc["link_id"])

        if typ == "SI":
            return {"firma": firma, "register_nummer": parsed.get("register_nummer"),
                    "sitz": parsed.get("sitz"), "dokument_typ": typ,
                    "daten": _parse_si_document(doc_html)}
        else:
            doc_soup = BeautifulSoup(doc_html, "html.parser")
            return {"firma": firma, "register_nummer": parsed.get("register_nummer"),
                    "dokument_typ": typ,
                    "text": doc_soup.get_text(separator="\n", strip=True)[:5000]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════
# MODUL 2: STEUERKANZLEIEN
# ══════════════════════════════════════════════

STV_BASE = "https://www.steuerberaterverzeichnis.de"

def _slugify(value: str) -> str:
    if not value:
        return ""
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf-8')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-')

@app.get("/steuerkanzleien/search")
def steuerkanzleien_search(
    city: Optional[str] = Query(None, description="Stadt, z.B. Hamburg"),
    page: int = Query(1, ge=1, description="Seitennummer")
):
    if not city:
        raise HTTPException(status_code=400, detail="Parameter 'city' ist erforderlich.")

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        city_slug = _slugify(city)
        search_url = f"{STV_BASE}/steuerberater-{city_slug}.html"
        if page > 1:
            search_url = f"{STV_BASE}/steuerberater-{city_slug}/seite-{page}.html"

        resp = session.get(search_url, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        results = _parse_stv_results(soup)

        total_pages = _get_total_pages(soup)

        return {"query": {"city": city}, "page": page, "total_pages": total_pages,
                "count": len(results), "results": results}

    except requests.HTTPError as e:
        if e.response.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Keine Ergebnisse fuer '{city}' gefunden (URL: {search_url})")
        raise HTTPException(status_code=502, detail=f"Fehler bei Zugriff auf {STV_BASE}: {e}")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nicht erreichbar: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _parse_stv_results(soup: BeautifulSoup) -> list:
    results = []
    entries = soup.select(".kanzlei-eintrag, .stb-eintrag, article.entry")
    for entry in entries:
        r = _extract_stv_entry(entry)
        if r.get("name"):
            results.append(r)
    return results


def _extract_stv_entry(entry) -> dict:
    result = {"name": "", "detail_url": "", "adresse": "", "telefon": "", "email": "", "website": ""}
    name_tag = entry.find(["h2", "h3"])
    if name_tag:
        result["name"] = name_tag.get_text(strip=True)
        link = name_tag.find("a", href=True)
        if link:
            result["detail_url"] = STV_BASE + link["href"]
    for p in entry.find_all("p"):
        text = p.get_text(strip=True)
        if not result["adresse"] and any(k in text for k in ["str.", " Str", "-Str"]):
            result["adresse"] = text
        elif not result["telefon"] and ("Tel" in text or "Fon" in text):
            result["telefon"] = text.split(":")[-1].strip()
    return result


def _get_total_pages(soup: BeautifulSoup) -> int:
    pag = soup.find(class_=re.compile(r"(pagination|pager)", re.I))
    if pag:
        nums = [int(a.get_text(strip=True)) for a in pag.find_all("a") if a.get_text(strip=True).isdigit()]
        if nums:
            return max(nums)
    return 1


@app.get("/steuerkanzleien/detail")
def steuerkanzleien_detail(url: str = Query(..., description="URL der Kanzlei-Detailseite")):
    return {"message": "Endpoint noch nicht vollstaendig implementiert"}

@app.get("/enrich")
def enrich(website: str = Query(..., description="Website-URL der Kanzlei")):
    return {"message": "Endpoint noch nicht vollstaendig implementiert"}
