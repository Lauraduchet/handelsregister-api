from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import requests
import mechanize
from bs4 import BeautifulSoup
import logging
import re
import time
from urllib.parse import quote, urljoin

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Callisto Data API", description="Handelsregister + Steuerberater")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

STB_BASE = "https://www.steuerberater.de"

# ──────────────────────────────────────────────
# HEALTH
# ──────────────────────────────────────────────
@app.get("/")
def health():
    return {"status": "ok", "endpoints": [
        "/handelsregister/search",
        "/handelsregister/debug-fields",
        "/handelsregister/dokument",
        "/steuerberater/search",
    ]}

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

def _parse_result_row(row) -> dict:
    """Parst eine Ergebnis-Zeile aus der Handelsregister-Suche."""
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

    # Register-Nummer extrahieren
    reg_match = re.search(r"(HRA|HRB|GnR|VR|PR)\s*\d+(\s+[A-Z])?(?!\w)", d["gericht"])
    if reg_match:
        d["register_nummer"] = reg_match.group(0).strip()

    # Dokument-Links extrahieren (SI, AD, CD, DK etc.)
    if len(cells) > 5:
        doc_cell = cells[5]
        for link in doc_cell.find_all("a"):
            link_text = link.get_text(strip=True)
            if link_text in ("SI", "AD", "CD", "DK", "UT"):
                # onclick enthält die JSF-Action-ID
                onclick = link.get("onclick", "")
                link_id = link.get("id", "")
                d["dokumente"].append({
                    "typ": link_text,
                    "link_id": link_id,
                    "onclick": onclick,
                })

    return d


# ══════════════════════════════════════════════
# HELPER: SI-Dokument (Strukturierter Registerinhalt) parsen
# ══════════════════════════════════════════════

def _parse_si_document(html: str) -> dict:
    """Parst den Strukturierten Registerinhalt (SI) und extrahiert
    Firma, Anschrift, Geschaeftsfuehrer, Gegenstand etc."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n", strip=True)

    result = {
        "firma": "",
        "anschrift": "",
        "plz_ort": "",
        "gegenstand": "",
        "geschaeftsfuehrer": [],
        "kapital": "",
        "rechtsform": "",
        "vertretung": "",
        "raw_text_snippet": text[:2000],
    }

    lines = text.split("\n")

    for i, line in enumerate(lines):
        line_s = line.strip()
        line_lower = line_s.lower()

        # Firma
        if "firma:" in line_lower or "name:" in line_lower:
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else ""
            if val:
                result["firma"] = val

        # Sitz / Anschrift
        if any(kw in line_lower for kw in ["anschrift:", "sitz:", "geschaeftsanschrift:", "geschäftsanschrift:"]):
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else ""
            if val:
                result["anschrift"] = val
            # Oft steht die Adresse in der naechsten Zeile
            if not val and i + 1 < len(lines):
                result["anschrift"] = lines[i + 1].strip()

        # PLZ + Ort (eigenstaendige Zeile)
        if re.match(r"^\d{5}\s+\w", line_s) and not result["plz_ort"]:
            result["plz_ort"] = line_s

        # Strasse erkennen und als Anschrift nutzen falls leer
        if not result["anschrift"] and re.search(r"(stra(ss|ß)e|str\.|weg\s+\d|allee\s|platz\s|ring\s)", line_lower):
            result["anschrift"] = line_s

        # Gegenstand des Unternehmens
        if "gegenstand:" in line_lower or "gegenstand des unternehmens" in line_lower:
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else ""
            if not val and i + 1 < len(lines):
                val = lines[i + 1].strip()
            result["gegenstand"] = val

        # Geschaeftsfuehrer / Vorstand / persoenlich haftend
        if any(kw in line_lower for kw in [
            "geschaeftsfuehrer", "geschäftsführer",
            "vorstand", "persoenlich haftend", "persönlich haftend",
            "vertretungsberechtig", "inhaber", "einzelprokurist", "prokurist"
        ]):
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else line_s
            if val:
                result["geschaeftsfuehrer"].append(val)

        # Stammkapital / Grundkapital
        if any(kw in line_lower for kw in ["stammkapital", "grundkapital", "kapital"]):
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else line_s
            if val:
                result["kapital"] = val

        # Rechtsform
        if "rechtsform:" in line_lower:
            val = line_s.split(":", 1)[1].strip() if ":" in line_s else ""
            if val:
                result["rechtsform"] = val

    # Fallback: Tabellen im SI parsen (oft als <table> strukturiert)
    tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if "firma" in label and not result["firma"]:
                    result["firma"] = value
                elif any(kw in label for kw in ["anschrift", "geschaeftsanschrift", "geschäftsanschrift", "sitz"]):
                    if not result["anschrift"]:
                        result["anschrift"] = value
                elif "gegenstand" in label and not result["gegenstand"]:
                    result["gegenstand"] = value
                elif any(kw in label for kw in ["geschaeftsfuehrer", "geschäftsführer", "vorstand"]):
                    result["geschaeftsfuehrer"].append(value)
                elif any(kw in label for kw in ["kapital", "stammkapital"]):
                    result["kapital"] = value
                elif "rechtsform" in label:
                    result["rechtsform"] = value

    return result


# ══════════════════════════════════════════════
# MODUL 1: HANDELSREGISTER
# ══════════════════════════════════════════════

BUNDESLAND_MAP = {
    "BW": "bundeslandBW", "BY": "bundeslandBY",
    "BE": "bundeslandBE", "BR": "bundeslandBR",
    "HB": "bundeslandHB", "HH": "bundeslandHH",
    "HE": "bundeslandHE", "MV": "bundeslandMV",
    "NI": "bundeslandNI", "NW": "bundeslandNW",
    "RP": "bundeslandRP", "SL": "bundeslandSL",
    "SN": "bundeslandSN", "ST": "bundeslandST",
    "SH": "bundeslandSH", "TH": "bundeslandTH",
}

RECHTSFORM_MAP = {
    "AG": "1", "eG": "2", "eV": "3",
    "Einzelkauffrau": "4", "Einzelkaufmann": "5",
    "SE": "6", "EWIV": "7", "GmbH": "8",
    "KG": "10", "OHG": "12", "Partnerschaft": "13",
}


@app.post("/handelsregister/search")
def handelsregister_search(
    schlagwoerter: str = Query("", description="Suchbegriff, z.B. 'Steuerberatung'. Platzhalter: * und ?"),
    schlagwort_option: int = Query(1, description="1=alle enthalten, 2=mind. eins, 3=exakter Name"),
    ort: Optional[str] = Query(None, description="Ort, z.B. 'Hamburg'"),
    plz: Optional[str] = Query(None, description="PLZ, z.B. '20095' oder '20*'"),
    strasse: Optional[str] = Query(None, description="Strasse, z.B. 'Jungfernstieg'"),
    bundesland: Optional[str] = Query(None, description="Kuerzel, z.B. 'HH' oder 'HH,NI'"),
    rechtsform: Optional[str] = Query(None, description="Code oder Name, z.B. '8' oder 'GmbH'"),
    auch_geloeschte: bool = Query(False, description="Auch geloeschte Firmen"),
    ergebnisse_pro_seite: int = Query(100, description="10, 25, 50 oder 100"),
    mit_si: bool = Query(False, description="SI-Dokument (Anschrift etc.) gleich mit abrufen? ACHTUNG: Langsam, max 60/Stunde!"),
):
    """
    Erweiterte Suche im Handelsregister mit mechanize.
    Gibt Firmendaten zurueck. Optional mit SI-Dokumenten (Anschrift).
    """
    if not schlagwoerter and not ort and not plz:
        raise HTTPException(status_code=400, detail="Mindestens schlagwoerter, ort oder plz angeben.")

    try:
        br = _create_browser()

        # Schritt 1: Startseite oeffnen
        br.open("https://www.handelsregister.de", timeout=15)

        # Schritt 2: Zur erweiterten Suche navigieren
        br.select_form(name="naviForm")
        br.form.new_control("hidden", "naviForm:erweiterteSucheLink",
                            {"value": "naviForm:erweiterteSucheLink"})
        br.form.new_control("hidden", "target", {"value": "erweiterteSucheLink"})
        br.submit()

        # Schritt 3: Suchformular ausfuellen
        br.select_form(name="form")

        if schlagwoerter:
            br["form:schlagwoerter"] = schlagwoerter
        br["form:schlagwortOptionen"] = [str(schlagwort_option)]

        # Ergebnisse pro Seite
        try:
            br["form:ergebnisseProSeite"] = [str(ergebnisse_pro_seite)]
        except Exception:
            pass

        # Erweiterte Parameter
        if ort:
            try:
                br["form:ort"] = ort
            except Exception:
                pass

        if plz:
            try:
                br["form:postleitzahl"] = plz
            except Exception:
                pass

        if strasse:
            try:
                br["form:strasse"] = strasse
            except Exception:
                pass

        if bundesland:
            for bl in bundesland.upper().split(","):
                bl = bl.strip()
                if bl in BUNDESLAND_MAP:
                    try:
                        br.find_control(f"form:{BUNDESLAND_MAP[bl]}").value = ["on"]
                    except Exception:
                        pass

        if rechtsform:
            rf_code = rechtsform.strip()
            if rf_code in RECHTSFORM_MAP:
                rf_code = RECHTSFORM_MAP[rf_code]
            try:
                br["form:rechtsform"] = [rf_code]
            except Exception:
                pass

        if auch_geloeschte:
            try:
                br.find_control("form:suchOptionenGeloescht").value = ["true"]
            except Exception:
                pass

        # Schritt 4: Suche absenden
        response = br.submit()
        html = response.read().decode("utf-8")

        # Schritt 5: Ergebnisse parsen
        soup = BeautifulSoup(html, "html.parser")
        grid = soup.find("table", role="grid")

        results = []
        if grid:
            for row in grid.find_all("tr"):
                if row.get("data-ri") is not None:
                    parsed = _parse_result_row(row)
                    if parsed:
                        results.append(parsed)

        # Schritt 6: Optional SI-Dokumente abrufen
        if mit_si and results:
            logger.info(f"SI-Abruf fuer {len(results)} Firmen gestartet (max 60/h beachten!)")
            for idx, company in enumerate(results):
                si_docs = [d for d in company.get("dokumente", []) if d["typ"] == "SI"]
                if si_docs:
                    try:
                        # SI-Link klicken
                        si_link_id = si_docs[0]["link_id"]
                        if si_link_id:
                            # Zurueck zur Ergebnisseite und SI-Link folgen
                            si_resp = br.follow_link(id=si_link_id)
                            si_html = si_resp.read().decode("utf-8")
                            si_data = _parse_si_document(si_html)
                            company["si_daten"] = si_data
                            # Zurueck navigieren
                            br.back()
                            time.sleep(1)  # Rate-Limit beachten!
                    except Exception as e:
                        logger.warning(f"SI-Abruf fehlgeschlagen fuer {company.get('firma')}: {e}")
                        company["si_daten"] = {"error": str(e)}

        return {
            "query": {
                "schlagwoerter": schlagwoerter,
                "ort": ort, "plz": plz, "strasse": strasse,
                "bundesland": bundesland, "rechtsform": rechtsform,
            },
            "count": len(results),
            "results": results,
        }

    except Exception as e:
        logger.error(f"HR error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────────
# STANDALONE: SI-Dokument fuer eine bekannte Firma abrufen
# ──────────────────────────────────────────────

@app.get("/handelsregister/dokument")
def handelsregister_dokument(
    firma: str = Query(..., description="Exakter Firmenname fuer die Suche"),
    typ: str = Query("SI", description="Dokumenttyp: SI, AD, CD"),
):
    """
    Sucht eine Firma im Handelsregister und gibt das angeforderte
    Dokument (default: SI = Strukturierter Registerinhalt) zurueck.
    Liefert Anschrift, Geschaeftsfuehrer, Gegenstand etc.
    """
    try:
        br = _create_browser()

        # Startseite
        br.open("https://www.handelsregister.de", timeout=15)

        # Erweiterte Suche
        br.select_form(name="naviForm")
        br.form.new_control("hidden", "naviForm:erweiterteSucheLink",
                            {"value": "naviForm:erweiterteSucheLink"})
        br.form.new_control("hidden", "target", {"value": "erweiterteSucheLink"})
        br.submit()

        # Exakte Suche
        br.select_form(name="form")
        br["form:schlagwoerter"] = firma
        br["form:schlagwortOptionen"] = ["3"]  # exakter Name
        response = br.submit()
        html = response.read().decode("utf-8")

        soup = BeautifulSoup(html, "html.parser")
        grid = soup.find("table", role="grid")

        if not grid:
            return {"firma": firma, "error": "Keine Ergebnisse gefunden.", "count": 0}

        # Ersten Treffer nehmen
        first_row = None
        for row in grid.find_all("tr"):
            if row.get("data-ri") is not None:
                first_row = row
                break

        if not first_row:
            return {"firma": firma, "error": "Kein Treffer.", "count": 0}

        parsed = _parse_result_row(first_row)

        # Dokument-Link finden
        target_docs = [d for d in parsed.get("dokumente", []) if d["typ"] == typ]
        if not target_docs:
            return {
                "firma": firma, "treffer": parsed,
                "error": f"Kein {typ}-Dokument verfuegbar.",
                "verfuegbare_dokumente": [d["typ"] for d in parsed.get("dokumente", [])],
            }

        # Dokument abrufen
        doc_link_id = target_docs[0]["link_id"]
        doc_resp = br.follow_link(id=doc_link_id)
        doc_html = doc_resp.read().decode("utf-8")

        if typ == "SI":
            si_data = _parse_si_document(doc_html)
            return {
                "firma": firma,
                "register_nummer": parsed.get("register_nummer", ""),
                "sitz": parsed.get("sitz", ""),
                "dokument_typ": typ,
                "daten": si_data,
            }
        else:
            # AD/CD: Rohtext zurueckgeben
            doc_soup = BeautifulSoup(doc_html, "html.parser")
            return {
                "firma": firma,
                "register_nummer": parsed.get("register_nummer", ""),
                "dokument_typ": typ,
                "text": doc_soup.get_text(separator="\n", strip=True)[:5000],
            }

    except Exception as e:
        logger.error(f"HR Dokument error: {e}")
        raise HTTPException(status_code=500, detail=str(e))




# ══════════════════════════════════════════════
# MODUL 2: STEUERBERATER (steuerberater.de)
# ══════════════════════════════════════════════

@app.get("/steuerberater/search",
         summary="Steuerberater Search",
         description=(
             "Durchsucht steuerberater.de nach Steuerberatern/Kanzleien. "
             "Gibt Name, Adresse, Berufsbezeichnung, Profil-URL zurueck. "
             "Mit mit_details=true zusaetzlich: Telefon, Fax, Homepage, Taetigkeitsfelder."
         ))
def steuerberater_search(
    stadt: Optional[str] = Query(None, description="Stadt, z.B. 'Hamburg', 'Berlin'"),
    plz: Optional[str] = Query(None, description="Postleitzahl, z.B. '20095'"),
    name: Optional[str] = Query(None, description="Name/Firmenname des Steuerberaters"),
    umkreis: Optional[int] = Query(50, description="Umkreis in km (10, 20, 30, 40, 50)"),
    max_seiten: int = Query(1, ge=1, le=10, description="Wie viele Ergebnisseiten scrapen? (1 Seite = ca. 10 Eintraege)"),
    mit_details: bool = Query(False, description="Detailseite jedes Eintrags direkt mit-crawlen? (langsamer, aber mehr Daten)"),
):
    """Scrapes steuerberater.de search results."""
    if not stadt and not plz and not name:
        raise HTTPException(status_code=400, detail="Mindestens stadt, plz oder name angeben.")

    session = requests.Session()
    session.headers.update(HEADERS)
    all_results = []

    try:
        for page_num in range(1, max_seiten + 1):
            # Build search URL
            params = {}
            search_term_parts = []
            if stadt:
                search_term_parts.append(stadt)
            if plz:
                search_term_parts.append(plz)
            if name:
                search_term_parts.append(name)

            # steuerberater.de uses ?search=... and ?ort=... and ?page=...
            if stadt:
                params["ort"] = stadt
            if plz:
                params["plz"] = plz
            if name:
                params["search"] = name
            if page_num > 1:
                params["page"] = str(page_num)

            search_url = f"{STB_BASE}/steuerberater-suchen"
            resp = session.get(search_url, params=params, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Parse result entries
            page_results = _parse_stb_listing(soup)

            if not page_results:
                # Try alternative: use ?search= with city name
                if stadt and "ort" in params:
                    alt_params = dict(params)
                    del alt_params["ort"]
                    alt_params["search"] = stadt
                    if name:
                        alt_params["search"] = f"{name} {stadt}"
                    resp = session.get(search_url, params=alt_params, timeout=20)
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.text, "html.parser")
                    page_results = _parse_stb_listing(soup)

            if not page_results:
                break

            all_results.extend(page_results)

            # Check if there's a next page
            if not _has_next_page(soup, page_num):
                break

            time.sleep(0.5)  # Be polite

        # Optionally fetch detail pages
        if mit_details and all_results:
            for i, entry in enumerate(all_results):
                if entry.get("profil_url"):
                    try:
                        detail = _fetch_stb_detail(session, entry["profil_url"])
                        all_results[i].update(detail)
                    except Exception as e:
                        all_results[i]["detail_error"] = str(e)
                    time.sleep(0.3)

        return {
            "query": {
                "stadt": stadt,
                "plz": plz,
                "name": name,
                "umkreis": umkreis,
            },
            "count": len(all_results),
            "results": all_results,
        }

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="steuerberater.de timeout")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"steuerberater.de nicht erreichbar: {e}")
    except Exception as e:
        logger.exception("Steuerberater search error")
        raise HTTPException(status_code=500, detail=str(e))


def _parse_stb_listing(soup: BeautifulSoup) -> list:
    """Parse the listing page of steuerberater.de and extract entries."""
    results = []

    # Find all profile links: /steuerberater-suchen/view/ID/slug
    profile_links = soup.find_all("a", href=re.compile(r"/steuerberater-suchen/view/\d+/"))

    # Deduplicate by URL (each entry has multiple links: image, name, "Profil", "Kontakt")
    seen_urls = set()
    unique_entries = []
    for link in profile_links:
        href = link.get("href", "")
        # Normalize: remove #contact suffix
        clean_href = href.split("#")[0]
        if clean_href not in seen_urls:
            seen_urls.add(clean_href)
            unique_entries.append(clean_href)

    for profile_path in unique_entries:
        full_url = profile_path if profile_path.startswith("http") else f"{STB_BASE}{profile_path}"

        # Extract name from slug
        slug_match = re.search(r"/view/\d+/(.+)$", profile_path)
        slug_name = ""
        if slug_match:
            slug_name = slug_match.group(1).replace("-", " ").title()

        # Try to find more info from surrounding HTML
        entry_data = {
            "name": "",
            "berufsbezeichnung": "",
            "adresse": "",
            "ort": "",
            "profil_url": full_url,
        }

        # Walk up to find the parent container for this entry
        # Look for the link and gather text from siblings/parent
        link_el = soup.find("a", href=re.compile(re.escape(profile_path.split("#")[0]) + r"(?:#|$)"))
        if link_el:
            # Find the parent container (usually a card/div)
            parent = link_el.find_parent(["div", "article", "li", "section"])
            if parent:
                # Get all text within the parent
                texts = [t.strip() for t in parent.stripped_strings if t.strip()]

                # First meaningful text is usually the name
                for t in texts:
                    if t not in ("Profil", "Kontakt", "Premium", "Verifiziert", "Verifiziert Premium") and len(t) > 2:
                        entry_data["name"] = t
                        break

                # Look for "Steuerberater" / "Steuerberaterin" etc.
                for t in texts:
                    if any(kw in t.lower() for kw in ["steuerberater", "wirtschaftsprüfer", "rechtsanwalt", "vBP"]):
                        if t != entry_data["name"]:
                            entry_data["berufsbezeichnung"] = t
                            break

                # Look for address pattern (street with number)
                for t in texts:
                    if re.search(r"\d{5}", t):
                        entry_data["ort"] = t
                    elif re.search(r"(str\.|straße|stra[sß]e|weg\s|allee|platz|ring\s|gasse|damm)", t, re.I):
                        entry_data["adresse"] = t

        # Fallback: use slug as name
        if not entry_data["name"]:
            entry_data["name"] = slug_name

        results.append(entry_data)

    return results


def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Check if there's a next page link."""
    # Look for pagination links
    next_link = soup.find("a", href=re.compile(r"[?&]page=" + str(current_page + 1)))
    if next_link:
        return True
    # Also check for "next" / "weiter" / ">" buttons
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        if text in (">", ">>", "weiter", "next", "nächste"):
            return True
    return False


def _fetch_stb_detail(session: requests.Session, url: str) -> dict:
    """Fetch a single steuerberater.de profile page and extract details."""
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    detail = {
        "telefon": "",
        "fax": "",
        "homepage": "",
        "branchen": [],
        "berufsbezeichnungen": [],
        "eigenschaften": [],
    }

    # Get text content
    text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Name from h2
    h2 = soup.find("h2")
    if h2:
        detail["name_detail"] = h2.get_text(strip=True)

    # Phone: look for tel: links
    tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
    if tel_links:
        detail["telefon"] = tel_links[0].get_text(strip=True)

    # Fax: usually the line after "Fax" or a number without tel: link
    # On steuerberater.de, fax is displayed as plain text near the phone
    for i, line in enumerate(lines):
        # Phone number (backup)
        if not detail["telefon"] and re.match(r"^[\d\s/\-\+\(\)]{6,}$", line):
            detail["telefon"] = line
        # Fax: typically the second phone-like number, or after the tel link
        if "fax" in line.lower():
            fax_num = re.sub(r"^.*?fax\s*:?\s*", "", line, flags=re.I).strip()
            if fax_num:
                detail["fax"] = fax_num

    # On steuerberater.de detail pages, fax is shown as plain text
    # after the phone. Let's find it via the list items near the address
    list_items = soup.find_all("li")
    phone_found = False
    for li in list_items:
        li_text = li.get_text(strip=True)
        tel_link_in_li = li.find("a", href=re.compile(r"^tel:"))
        if tel_link_in_li:
            if not phone_found:
                detail["telefon"] = tel_link_in_li.get_text(strip=True)
                phone_found = True
            continue
        # A number after the phone li that's NOT a link -> likely fax
        if phone_found and not detail["fax"] and re.match(r"^[\d\s/\-\+\(\)]{6,}$", li_text):
            detail["fax"] = li_text

    # Homepage
    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True).lower() == "homepage":
            detail["homepage"] = a["href"]
            break

    # Sections: Branchen, Berufsbezeichnungen, Eigenschaften
    current_section = None
    for line in lines:
        ll = line.lower().strip()
        if ll == "branchen":
            current_section = "branchen"
            continue
        elif ll == "berufsbezeichnungen":
            current_section = "berufsbezeichnungen"
            continue
        elif ll == "eigenschaften":
            current_section = "eigenschaften"
            continue
        elif ll in ("info", "galerie", "bewertungen", "jetzt anliegen schildern",
                     "profil", "kontakt", "karte anzeigen"):
            current_section = None
            continue

        if current_section and line and len(line) > 1 and len(line) < 100:
            detail[current_section].append(line)

    return detail


