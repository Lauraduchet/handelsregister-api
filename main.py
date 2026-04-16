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
        "/handelsregister/debug-fields",
        "/steuerberater/search",
        "/steuerberater/detail",
    ]}


# ══════════════════════════════════════════════════════════════════
#
#   MODUL 1: HANDELSREGISTER  (unveraendert aus vorheriger Version)
#
# ══════════════════════════════════════════════════════════════════

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


def _open_erweiterte_suche(br: mechanize.Browser):
    br.open("https://www.handelsregister.de", timeout=15)
    br.select_form(name="naviForm")
    br.form.new_control("hidden", "naviForm:erweiterteSucheLink",
                        {"value": "naviForm:erweiterteSucheLink"})
    br.form.new_control("hidden", "target",
                        {"value": "erweiterteSucheLink"})
    br.submit()


def _debug_form_controls(br: mechanize.Browser) -> list:
    controls = []
    for ctrl in br.form.controls:
        controls.append({
            "name": ctrl.name,
            "type": ctrl.type,
            "value": ctrl.value if hasattr(ctrl, "value") else "",
        })
    return controls


FIELD_CANDIDATES = {
    "schlagwoerter": ["form:schlagwoerter"],
    "schlagwortOptionen": ["form:schlagwortOptionen"],
    "ergebnisseProSeite": ["form:ergebnisseProSeite_input"],
    "ort": ["form:ort"],
    "postleitzahl": ["form:postleitzahl"],
    "strasse": ["form:strasse"],
    "rechtsform": ["form:rechtsform_input"],
}

BUNDESLAND_MAP = {
    "BW": "Baden-Wuerttemberg", "BY": "Bayern", "BE": "Berlin", "BR": "Brandenburg",
    "HB": "Bremen", "HH": "Hamburg", "HE": "Hessen", "MV": "Mecklenburg-Vorpommern",
    "NI": "Niedersachsen", "NW": "Nordrhein-Westfalen", "RP": "Rheinland-Pfalz",
    "SL": "Saarland", "SN": "Sachsen", "ST": "Sachsen-Anhalt",
    "SH": "Schleswig-Holstein", "TH": "Thueringen",
}

RECHTSFORM_MAP = {
    "AG": "1", "eG": "2", "eV": "3", "Einzelkauffrau": "4", "Einzelkaufmann": "5",
    "SE": "6", "EWIV": "7", "GmbH": "8", "KG": "10", "OHG": "12", "Partnerschaft": "13",
}


def _find_and_set(br, param_key, value, is_list=False):
    candidates = FIELD_CANDIDATES.get(param_key, [f"form:{param_key}"])
    for name in candidates:
        try:
            if is_list:
                br[name] = value if isinstance(value, list) else [str(value)]
            else:
                br[name] = str(value)
            return True
        except (mechanize.ControlNotFoundError, mechanize.ItemNotFoundError):
            continue
    available = [c.name for c in br.form.controls if c.name]
    logger.error(f"Feld '{param_key}' nicht gefunden! Kandidaten: {candidates}, Verfuegbar: {available}")
    return False


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


def _fetch_si_document(br, si_link_id):
    form_found = False
    for form in br.forms():
        if form.name and "ergebniss" in form.name.lower():
            br.select_form(name=form.name)
            form_found = True
            break
    if not form_found:
        for form in br.forms():
            if form.name and form.name != "naviForm":
                br.select_form(name=form.name)
                form_found = True
                break
    if not form_found:
        raise Exception("Kein passendes Formular fuer SI-Abruf gefunden")
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
    for ctrl in br.form.controls:
        ctrl.readonly = False
    response = br.submit()
    return response.read().decode("utf-8")


def _parse_si_document(html):
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


@app.post("/handelsregister/search")
def handelsregister_search(
    schlagwoerter: str = Query(""),
    schlagwort_option: int = Query(1),
    ort: Optional[str] = Query(None),
    plz: Optional[str] = Query(None),
    strasse: Optional[str] = Query(None),
    bundesland: Optional[str] = Query(None),
    rechtsform: Optional[str] = Query(None),
    auch_geloeschte: bool = Query(False),
    ergebnisse_pro_seite: int = Query(100),
    mit_si: bool = Query(False),
    debug_fields: bool = Query(False),
):
    if not schlagwoerter and not ort and not plz:
        raise HTTPException(status_code=400, detail="Mindestens schlagwoerter, ort oder plz angeben.")
    field_errors = []
    try:
        br = _create_browser()
        _open_erweiterte_suche(br)
        br.select_form(name="form")
        if debug_fields:
            return {"debug_form_controls": _debug_form_controls(br)}
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
                    ctrl_name = f"form:{BUNDESLAND_MAP[bl]}_input"
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
                br.find_control("form:auchGeloeschte_input").value = ["on"]
            except Exception:
                field_errors.append("auchGeloeschte")

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

        if mit_si and results:
            for company in results:
                si_doc = next((d for d in company.get("dokumente", []) if d["typ"] == "SI"), None)
                if si_doc and si_doc.get("link_id"):
                    try:
                        si_html = _fetch_si_document(br, si_doc["link_id"])
                        company["si_daten"] = _parse_si_document(si_html)
                        try:
                            br.back()
                        except Exception:
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

        resp = {
            "query": {"schlagwoerter": schlagwoerter, "ort": ort, "plz": plz,
                      "strasse": strasse, "bundesland": bundesland, "rechtsform": rechtsform},
            "count": len(results), "results": results,
        }
        if field_errors:
            resp["_field_warnings"] = f"Felder nicht gesetzt: {field_errors}"
        return resp
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/handelsregister/debug-fields")
def handelsregister_debug_fields():
    try:
        br = _create_browser()
        _open_erweiterte_suche(br)
        br.select_form(name="form")
        return {"form_name": br.form.name, "controls": _debug_form_controls(br)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/handelsregister/dokument")
def handelsregister_dokument(firma: str = Query(...), typ: str = Query("SI")):
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
        first_row = next((row for row in grid.find_all("tr") if row.get("data-ri") is not None), None)
        if not first_row:
            raise HTTPException(status_code=404, detail="Firma nicht gefunden")
        parsed = _parse_hr_result_row(first_row)
        target_doc = next((d for d in parsed.get("dokumente", []) if d["typ"] == typ), None)
        if not target_doc or not target_doc.get("link_id"):
            raise HTTPException(status_code=404, detail=f"{typ}-Dokument nicht verfuegbar.")
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


# ══════════════════════════════════════════════════════════════════
#
#   MODUL 2: STEUERBERATER.DE SCRAPER
#
# ══════════════════════════════════════════════════════════════════

STB_BASE = "https://www.steuerberater.de"


def _stb_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


# ──────────────────────────────────────────────
# LISTEN-SUCHE: /steuerberater/search
# ──────────────────────────────────────────────

@app.get("/steuerberater/search")
def steuerberater_search(
    stadt: Optional[str] = Query(None, description="Stadt, z.B. 'Hamburg', 'Berlin'"),
    plz: Optional[str] = Query(None, description="Postleitzahl, z.B. '20095'"),
    name: Optional[str] = Query(None, description="Name/Firmenname des Steuerberaters"),
    umkreis: int = Query(50, description="Umkreis in km (10, 20, 30, 40, 50)"),
    max_seiten: int = Query(1, description="Wie viele Ergebnisseiten scrapen? (1 Seite = ca. 10 Eintraege)"),
    mit_details: bool = Query(False, description="Detailseite jedes Eintrags direkt mit-crawlen? (langsamer, aber mehr Daten)"),
):
    """
    Durchsucht steuerberater.de nach Steuerberatern/Kanzleien.
    Gibt Name, Adresse, Berufsbezeichnung, Profil-URL zurueck.
    Mit mit_details=true zusaetzlich: Telefon, Fax, Homepage, Taetigkeitsfelder.
    """
    if not stadt and not plz and not name:
        raise HTTPException(status_code=400, detail="Mindestens stadt, plz oder name angeben.")

    try:
        session = _stb_session()

        # --- Suchparameter zusammenbauen ---
        params = {
            "auswahlLand": "1",
            "termsOfUse": "1",
            "submit": "Steuerberater suchen",
            "umkreis": str(umkreis),
            "branchen[]": "-1",
            "laender[]": "-1",
            "sprachen[]": "-1",
            "taetigkeitsbereiche[]": "-1",
            "zusaetze[]": "-1",
            "land": "-1",
        }
        if stadt:
            params["stadt"] = stadt
        if plz:
            params["plz"] = plz
        if name:
            params["name"] = name

        # Erste Seite laden
        search_url = f"{STB_BASE}/steuerberater-suchen"
        resp = session.get(search_url, params=params, timeout=20)
        resp.raise_for_status()

        all_results = []
        current_html = resp.text

        for page_num in range(max_seiten):
            page_results = _parse_stb_list(current_html)
            if not page_results:
                break
            all_results.extend(page_results)

            # Naechste Seite? (Pagination-Link suchen)
            if page_num < max_seiten - 1:
                soup = BeautifulSoup(current_html, "html.parser")
                next_link = soup.find("a", {"rel": "next"})
                if not next_link:
                    # Alternativ: "Weiter"-Link suchen
                    next_link = soup.find("a", string=re.compile(r"(Weiter|weiter|Next|next|>>|>)", re.I))
                if next_link and next_link.get("href"):
                    next_url = urllib.parse.urljoin(STB_BASE, next_link["href"])
                    time.sleep(0.5)
                    resp = session.get(next_url, timeout=20)
                    resp.raise_for_status()
                    current_html = resp.text
                else:
                    break  # Keine weiteren Seiten

        # --- Optional: Detailseiten crawlen ---
        if mit_details and all_results:
            for i, entry in enumerate(all_results):
                if entry.get("profil_url"):
                    try:
                        time.sleep(0.3)
                        detail = _fetch_stb_detail(session, entry["profil_url"])
                        entry.update(detail)
                    except Exception as e:
                        entry["detail_error"] = str(e)

        return {
            "query": {"stadt": stadt, "plz": plz, "name": name, "umkreis": umkreis},
            "count": len(all_results),
            "results": all_results,
        }

    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"steuerberater.de Fehler: {e}")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nicht erreichbar: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _parse_stb_list(html: str) -> list:
    """Parst die Suchergebnis-Liste von steuerberater.de."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # Alle Profil-Links finden (Pattern: /steuerberater-suchen/view/ID/name)
    profile_links = soup.find_all("a", href=re.compile(r"/steuerberater-suchen/view/\d+/"))

    # Deduplizieren (jedes Profil kommt mehrfach vor: Bild, Name, Profil-Button)
    seen_urls = set()
    unique_profiles = []
    for link in profile_links:
        href = link.get("href", "")
        full_url = urllib.parse.urljoin(STB_BASE, href.split("#")[0])
        if full_url not in seen_urls:
            seen_urls.add(full_url)
            unique_profiles.append((link, full_url))

    for link, profile_url in unique_profiles:
        entry = {
            "name": "",
            "berufsbezeichnung": "",
            "adresse": "",
            "profil_url": profile_url,
        }

        # Name extrahieren: Suche das naechste h2/h3/h4 Element in der Naehe
        # Oder den Text des Links selbst
        name_text = link.get_text(strip=True)
        if name_text and len(name_text) > 2:
            entry["name"] = name_text

        # Nach oben im DOM gehen, um den umgebenden Container zu finden
        container = link.find_parent(["div", "article", "li", "section"])
        if container:
            # Name aus Heading extrahieren
            heading = container.find(["h2", "h3", "h4", "h5"])
            if heading:
                entry["name"] = heading.get_text(strip=True)

            # Berufsbezeichnung (steht oft ueber dem Namen)
            full_text = container.get_text(separator="\n", strip=True)
            for line in full_text.split("\n"):
                line = line.strip()
                if any(kw in line.lower() for kw in
                       ["steuerberater", "wirtschaftsprüfer", "buchprüfer",
                        "rechtsanwalt", "steuerbevollmächtig"]):
                    if line != entry["name"]:
                        entry["berufsbezeichnung"] = line
                        break

            # Adresse (Pattern: "Strasse X in PLZ Ort")
            for line in full_text.split("\n"):
                line = line.strip()
                if re.search(r"\d{5}\s+\w", line) and " in " in line:
                    entry["adresse"] = line
                    break
                elif re.search(r"\d{5}\s+\w", line):
                    entry["adresse"] = line
                    break

        if entry["name"]:
            results.append(entry)

    return results


# ──────────────────────────────────────────────
# DETAIL-ABRUF: /steuerberater/detail
# ──────────────────────────────────────────────

@app.get("/steuerberater/detail")
def steuerberater_detail(
    url: str = Query(..., description="Profil-URL, z.B. 'https://www.steuerberater.de/steuerberater-suchen/view/8140/elena-haegler'"),
):
    """Crawlt die Detailseite eines Steuerberaters und extrahiert alle verfuegbaren Daten."""
    try:
        session = _stb_session()
        detail = _fetch_stb_detail(session, url)
        return {"url": url, **detail}
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Nicht erreichbar: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _fetch_stb_detail(session: requests.Session, url: str) -> dict:
    """Crawlt eine einzelne Detailseite und extrahiert strukturierte Daten."""
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    detail = {
        "name": "",
        "kanzlei_name": "",
        "berufsbezeichnung": "",
        "adresse": "",
        "telefon": "",
        "fax": "",
        "email": "",
        "homepage": "",
        "beschreibung": "",
        "taetigkeitsfelder": [],
        "branchen": [],
        "sprachen": [],
    }

    # --- Name & Kanzleiname ---
    # Der Kanzleiname steht typischerweise in einem h2 im Hauptbereich
    main_heading = soup.find("h2")
    if main_heading:
        detail["kanzlei_name"] = main_heading.get_text(strip=True)

    # Der persoenliche Name steht oft im Breadcrumb oder title
    breadcrumb = soup.find("ol", class_=re.compile(r"breadcrumb", re.I))
    if breadcrumb:
        items = breadcrumb.find_all("li")
        if items:
            last_item = items[-1].get_text(strip=True)
            if last_item:
                detail["name"] = last_item

    # Fallback: Aus dem <title> Tag
    if not detail["name"]:
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # Pattern: "Steuerberaterin Elena Haegler"
            match = re.search(r"(?:Steuerberater(?:in)?|Wirtschaftsprüfer(?:in)?)\s+(.+?)(?:\s*\||\s*-|\s*$)", title_text)
            if match:
                detail["name"] = match.group(1).strip()

    # --- Berufsbezeichnung ---
    # Steht oft direkt ueber oder unter dem Namen
    full_text = soup.get_text(separator="\n", strip=True)
    for line in full_text.split("\n"):
        line = line.strip()
        if any(kw in line.lower() for kw in ["steuerberater", "wirtschaftsprüfer",
                                               "buchprüfer", "rechtsanwalt",
                                               "steuerbevollmächtig"]):
            if len(line) < 100 and line not in [detail["name"], detail["kanzlei_name"]]:
                detail["berufsbezeichnung"] = line
                break

    # --- Adresse ---
    # Suche nach typischen Adress-Patterns im gesamten Text
    for line in full_text.split("\n"):
        line = line.strip()
        if re.search(r"\d{5}\s+\w", line) and len(line) < 200:
            # Bereinige die Adresse
            addr = line.strip()
            if addr and addr != detail["kanzlei_name"]:
                detail["adresse"] = addr
                break

    # --- Telefon & Fax ---
    # Suche nach tel:-Links
    tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
    for i, tl in enumerate(tel_links):
        number = tl.get_text(strip=True)
        if i == 0:
            detail["telefon"] = number
        elif i == 1:
            detail["fax"] = number

    # --- Homepage ---
    homepage_link = soup.find("a", string=re.compile(r"Homepage", re.I))
    if homepage_link and homepage_link.get("href"):
        href = homepage_link["href"]
        if href.startswith("http"):
            detail["homepage"] = href

    # --- E-Mail ---
    # Suche nach mailto-Links (nicht info@steuerberater.de)
    mailto_links = soup.find_all("a", href=re.compile(r"^mailto:"))
    for ml in mailto_links:
        email = ml.get("href", "").replace("mailto:", "").strip()
        if email and "steuerberater.de" not in email:
            detail["email"] = email
            break

    # --- Beschreibung ---
    blockquote = soup.find("blockquote")
    if blockquote:
        detail["beschreibung"] = blockquote.get_text(strip=True)

    # --- Taetigkeitsfelder ---
    taetig_section = _find_section_by_heading(soup, "Tätigkeitsfelder")
    if taetig_section:
        detail["taetigkeitsfelder"] = [t.strip() for t in taetig_section if t.strip()]

    # --- Branchen ---
    branchen_section = _find_section_by_heading(soup, "Branchen")
    if branchen_section:
        detail["branchen"] = [b.strip() for b in branchen_section if b.strip()]

    # --- Sprachen ---
    sprachen_section = _find_section_by_heading(soup, "Sprachen")
    if sprachen_section:
        detail["sprachen"] = [s.strip() for s in sprachen_section if s.strip()]

    return detail


def _find_section_by_heading(soup: BeautifulSoup, heading_text: str) -> list:
    """Findet eine Sektion anhand der Ueberschrift und gibt die Inhalte als Liste zurueck."""
    heading = soup.find(["h3", "h4", "h5"], string=re.compile(re.escape(heading_text), re.I))
    if not heading:
        return []

    items = []
    # Sammle alle Text-Elemente nach der Ueberschrift bis zur naechsten Ueberschrift
    sibling = heading.find_next_sibling()
    while sibling:
        if sibling.name in ["h3", "h4", "h5"]:
            break  # Naechste Sektion erreicht
        text = sibling.get_text(strip=True)
        if text:
            items.append(text)
        sibling = sibling.find_next_sibling()

    # Falls keine Siblings gefunden, suche im Parent-Container
    if not items:
        parent = heading.find_parent(["div", "section"])
        if parent:
            for child in parent.find_all(["span", "div", "p", "li"]):
                text = child.get_text(strip=True)
                if text and text != heading_text:
                    items.append(text)

    return items


