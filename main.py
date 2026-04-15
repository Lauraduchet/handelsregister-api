from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import requests
from bs4 import BeautifulSoup
import logging
import re

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
        "/steuerkanzleien/search",
        "/steuerkanzleien/detail",
        "/enrich"
    ]}


# ══════════════════════════════════════════════
# MODUL 1: HANDELSREGISTER (erweitert)
# ══════════════════════════════════════════════

HR_BASE_URL = "https://www.handelsregister.de/rp_web/erweitertesuche.xhtml"

# Mapping: Bundesland-Kuerzel -> Form-Parameter
BUNDESLAND_MAP = {
    "BW": "form:bundeslandBW", "BY": "form:bundeslandBY",
    "BE": "form:bundeslandBE", "BR": "form:bundeslandBR",
    "HB": "form:bundeslandHB", "HH": "form:bundeslandHH",
    "HE": "form:bundeslandHE", "MV": "form:bundeslandMV",
    "NI": "form:bundeslandNI", "NW": "form:bundeslandNW",
    "RP": "form:bundeslandRP", "SL": "form:bundeslandSL",
    "SN": "form:bundeslandSN", "ST": "form:bundeslandST",
    "SH": "form:bundeslandSH", "TH": "form:bundeslandTH",
}

# Mapping: Rechtsform-Name -> Code
RECHTSFORM_MAP = {
    "AG": "1", "eG": "2", "eV": "3",
    "Einzelkauffrau": "4", "Einzelkaufmann": "5",
    "SE": "6", "EWIV": "7", "GmbH": "8",
    "KG": "10", "OHG": "12", "Partnerschaft": "13",
}


def get_viewstate(soup: BeautifulSoup) -> str:
    tag = soup.find("input", {"name": "javax.faces.ViewState"})
    if tag:
        return tag.get("value", "")
    tag = soup.find("input", {"id": "j_id1:javax.faces.ViewState:0"})
    if tag:
        return tag.get("value", "")
    return ""


@app.post("/handelsregister/search")
def handelsregister_search(
    schlagwoerter: str = Query("", description="Suchbegriff, z.B. 'Steuerberatung'. Platzhalter: * und ?"),
    schlagwort_option: int = Query(1, description="1=alle enthalten, 2=mind. eins, 3=exakter Name"),
    ort: Optional[str] = Query(None, description="Ort / Niederlassungsort, z.B. 'Hamburg'"),
    plz: Optional[str] = Query(None, description="Postleitzahl, z.B. '20095'. Platzhalter erlaubt: 20*"),
    strasse: Optional[str] = Query(None, description="Strasse, z.B. 'Jungfernstieg'"),
    bundesland: Optional[str] = Query(None, description="Bundesland-Kuerzel, z.B. 'HH'. Kommagetrennt fuer mehrere: 'HH,NI'"),
    rechtsform: Optional[str] = Query(None, description="Rechtsform-Code (8=GmbH, 5=Einzelkaufmann, 1=AG, 10=KG, 12=OHG) oder Name"),
    auch_geloeschte: bool = Query(False, description="Auch geloeschte Firmen finden"),
    ergebnisse_pro_seite: int = Query(100, description="10, 25, 50 oder 100"),
):
    """
    Erweiterte Suche im Handelsregister.
    Mindestens schlagwoerter ODER ort ODER plz muss angegeben werden.
    """
    if not schlagwoerter and not ort and not plz:
        raise HTTPException(status_code=400, detail="Mindestens schlagwoerter, ort oder plz angeben.")

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # Schritt 1: GET - Seite laden + ViewState holen
        resp1 = session.get(HR_BASE_URL, timeout=30)
        resp1.raise_for_status()

        soup1 = BeautifulSoup(resp1.text, "lxml")
        viewstate = get_viewstate(soup1)

        logger.info(f"GET status={resp1.status_code}, ViewState={'found' if viewstate else 'MISSING'}")

        if not viewstate:
            logger.warning(f"Page snippet: {resp1.text[:500]}")
            return {
                "query": schlagwoerter, "count": 0, "results": [],
                "error": "ViewState nicht gefunden - Seite evtl. geblockt."
            }

        # Schritt 2: POST-Formular zusammenbauen
        form_data = {
            "form": "form",
            "form:schlagwoerter": schlagwoerter,
            "form:schlagwortOptionen": str(schlagwort_option),
            "form:btnSuche": "Suchen",
            "form:ergebnisseProSeite": str(ergebnisse_pro_seite),
            "javax.faces.ViewState": viewstate,
        }

        # Erweiterte Suche aktivieren
        if any([ort, plz, strasse, bundesland, rechtsform]):
            form_data["form:suchTyp"] = "e"

        # Ort
        if ort:
            form_data["form:ort"] = ort

        # PLZ
        if plz:
            form_data["form:postleitzahl"] = plz

        # Strasse
        if strasse:
            form_data["form:strasse"] = strasse

        # Bundeslaender
        if bundesland:
            for bl in bundesland.upper().split(","):
                bl = bl.strip()
                if bl in BUNDESLAND_MAP:
                    form_data[BUNDESLAND_MAP[bl]] = "on"

        # Rechtsform
        if rechtsform:
            rf_code = rechtsform.strip()
            # Falls Name statt Code angegeben
            if rf_code in RECHTSFORM_MAP:
                rf_code = RECHTSFORM_MAP[rf_code]
            form_data["form:rechtsform"] = rf_code

        # Geloeschte
        if auch_geloeschte:
            form_data["form:suchOptionenGeloescht"] = "true"

        # Schritt 3: POST absenden
        resp2 = session.post(HR_BASE_URL, data=form_data, timeout=30)
        resp2.raise_for_status()

        logger.info(f"POST status={resp2.status_code}, length={len(resp2.text)}")

        soup2 = BeautifulSoup(resp2.text, "lxml")

        # Schritt 4: Ergebnisse parsen
        results = []

        rows = soup2.select("table.ergebnisListe tr")
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

        # Debug: wenn keine Treffer
        if not results:
            snippet = soup2.get_text(separator=" ", strip=True)[:500]
            logger.info(f"Keine Treffer. Page-Text: {snippet}")

        return {
            "query": {
                "schlagwoerter": schlagwoerter,
                "ort": ort,
                "plz": plz,
                "strasse": strasse,
                "bundesland": bundesland,
                "rechtsform": rechtsform,
            },
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


# ══════════════════════════════════════════════
# MODUL 2: STEUERKANZLEIEN (steuerberaterverzeichnis.de)
# ══════════════════════════════════════════════

STV_BASE = "https://www.steuerberaterverzeichnis.de"


@app.get("/steuerkanzleien/search")
def steuerkanzleien_search(
    city: Optional[str] = Query(None, description="Stadt, z.B. Hamburg"),
    plz: Optional[str] = Query(None, description="Postleitzahl, z.B. 20095"),
    name: Optional[str] = Query(None, description="Name der Kanzlei oder Person"),
    page: int = Query(1, ge=1, description="Seitennummer"),
):
    """
    Sucht Steuerkanzleien auf steuerberaterverzeichnis.de.
    Mindestens city ODER plz ODER name muss angegeben werden.
    """
    if not city and not plz and not name:
        raise HTTPException(status_code=400, detail="Mindestens city, plz oder name angeben.")

    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # Suchseite laden (Cookies/Session)
        search_url = f"{STV_BASE}/steuerberater-suchen.html"
        session.get(search_url, timeout=15)

        # Suchformular absenden
        params = {}
        if city:
            params["ort"] = city
        if plz:
            params["plz"] = plz
        if name:
            params["name"] = name
        if page > 1:
            params["seite"] = str(page)

        # Versuch 1: GET mit Parametern
        resp = session.get(search_url, params=params, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = _parse_stv_results(soup)

        # Versuch 2: POST
        if not results:
            form_data = {}
            if city:
                form_data["ort"] = city
            if plz:
                form_data["plz"] = plz
            if name:
                form_data["name"] = name
            resp = session.post(search_url, data=form_data, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            results = _parse_stv_results(soup)

        # Versuch 3: Direkte Stadt-URL
        if not results and city:
            city_slug = (city.lower()
                         .replace(" ", "-")
                         .replace("ue", "ue").replace("oe", "oe")
                         .replace("ae", "ae").replace("ss", "ss"))
            for ch, repl in [("ü","ue"),("ö","oe"),("ä","ae"),("ß","ss")]:
                city_slug = city_slug.replace(ch, repl)
            city_url = f"{STV_BASE}/steuerberater-{city_slug}.html"
            resp = session.get(city_url, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                results = _parse_stv_results(soup)

        total_pages = _get_total_pages(soup)

        return {
            "query": {"city": city, "plz": plz, "name": name},
            "page": page,
            "total_pages": total_pages,
            "count": len(results),
            "results": results,
        }

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="steuerberaterverzeichnis.de timeout")
    except requests.RequestException as e:
        logger.error(f"STV request error: {e}")
        raise HTTPException(status_code=502, detail=f"steuerberaterverzeichnis.de nicht erreichbar: {e}")
    except Exception as e:
        logger.error(f"STV unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _parse_stv_results(soup: BeautifulSoup) -> list:
    results = []

    # Strategie 1: Kanzlei-Karten
    entries = soup.select(".kanzlei-eintrag, .stb-eintrag, .result-item, .list-item, article.entry")
    if entries:
        for entry in entries:
            result = _extract_entry(entry)
            if result.get("name"):
                results.append(result)

    # Strategie 2: Links zu Detailseiten
    if not results:
        links = soup.find_all("a", href=True)
        for link in links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if ("/steuerberater/" in href or "/kanzlei/" in href or "/profil/" in href) and text and len(text) > 3:
                detail_url = href if href.startswith("http") else f"{STV_BASE}{href}"
                results.append({
                    "name": text, "detail_url": detail_url,
                    "adresse": "", "telefon": "", "email": "", "website": "",
                })

    # Strategie 3: Tabellen
    if not results:
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    name = cols[0].get_text(strip=True)
                    if name:
                        link_tag = cols[0].find("a", href=True)
                        detail_url = ""
                        if link_tag:
                            href = link_tag["href"]
                            detail_url = href if href.startswith("http") else f"{STV_BASE}{href}"
                        results.append({
                            "name": name, "detail_url": detail_url,
                            "adresse": cols[1].get_text(strip=True) if len(cols) > 1 else "",
                            "telefon": cols[2].get_text(strip=True) if len(cols) > 2 else "",
                            "email": cols[3].get_text(strip=True) if len(cols) > 3 else "",
                            "website": "",
                        })

    return results


def _extract_entry(entry) -> dict:
    result = {"name": "", "detail_url": "", "adresse": "", "telefon": "", "email": "", "website": ""}

    name_tag = entry.find(["h2", "h3", "h4", "strong", "b"])
    if name_tag:
        result["name"] = name_tag.get_text(strip=True)
        link = name_tag.find("a", href=True)
        if link:
            href = link["href"]
            result["detail_url"] = href if href.startswith("http") else f"{STV_BASE}{href}"

    addr_tag = entry.find(class_=re.compile(r"(adress|address|ort|location|anschrift)", re.I))
    if addr_tag:
        result["adresse"] = addr_tag.get_text(strip=True)

    tel_tag = entry.find(class_=re.compile(r"(tel|phone|fon)", re.I))
    if tel_tag:
        result["telefon"] = tel_tag.get_text(strip=True)
    else:
        tel_link = entry.find("a", href=re.compile(r"^tel:"))
        if tel_link:
            result["telefon"] = tel_link.get_text(strip=True)

    mail_link = entry.find("a", href=re.compile(r"^mailto:"))
    if mail_link:
        result["email"] = mail_link["href"].replace("mailto:", "")

    web_link = entry.find("a", href=re.compile(r"^https?://(?!www\.steuerberater)"))
    if web_link:
        result["website"] = web_link["href"]

    return result


def _get_total_pages(soup: BeautifulSoup) -> int:
    pag = soup.find(class_=re.compile(r"(pagination|pager|seiten)", re.I))
    if pag:
        page_links = pag.find_all("a")
        nums = []
        for a in page_links:
            txt = a.get_text(strip=True)
            if txt.isdigit():
                nums.append(int(txt))
        if nums:
            return max(nums)
    return 1


# ──────────────────────────────────────────────
# DETAIL: Einzelne Kanzlei-Seite scrapen
# ──────────────────────────────────────────────

@app.get("/steuerkanzleien/detail")
def steuerkanzleien_detail(url: str = Query(..., description="URL der Kanzlei-Detailseite")):
    """
    Scrapt eine einzelne Kanzlei-Detailseite und extrahiert Kontaktdaten.
    """
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        resp = session.get(url, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator="\n", strip=True)

        result = {
            "url": url, "name": "", "inhaber": "",
            "adresse": "", "plz_ort": "",
            "telefon": "", "fax": "", "email": "", "website": "",
            "taetigkeiten": [],
            "raw_text_snippet": text[:1000],
        }

        h1 = soup.find("h1")
        if h1:
            result["name"] = h1.get_text(strip=True)

        lines = text.split("\n")
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()

            if any(kw in line_lower for kw in ["inhaber", "ansprechpartner", "geschaeftsfuehr", "geschäftsführ", "partner"]):
                if ":" in line:
                    result["inhaber"] = line.split(":", 1)[1].strip()
                elif i + 1 < len(lines):
                    result["inhaber"] = lines[i + 1].strip()

            if re.search(r"(stra(ss|ß)e|str\.|weg |allee |platz |ring )", line_lower):
                result["adresse"] = line.strip()
                if i + 1 < len(lines) and re.match(r"^\d{5}", lines[i + 1].strip()):
                    result["plz_ort"] = lines[i + 1].strip()

            if not result["plz_ort"] and re.match(r"^\d{5}\s+\w", line.strip()):
                result["plz_ort"] = line.strip()

            if any(kw in line_lower for kw in ["telefon", "tel.", "tel:", "fon:"]):
                tel = re.sub(r"^.*?(tel\.?|telefon|fon)\s*:?\s*", "", line, flags=re.I).strip()
                if tel:
                    result["telefon"] = tel

            if "fax" in line_lower:
                fax = re.sub(r"^.*?fax\s*:?\s*", "", line, flags=re.I).strip()
                if fax:
                    result["fax"] = fax

            if "@" in line:
                mail_match = re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", line)
                if mail_match:
                    result["email"] = mail_match.group(0)

            if "www." in line_lower or "http" in line_lower:
                url_match = re.search(r"(https?://[^\s]+|www\.[^\s]+)", line)
                if url_match:
                    result["website"] = url_match.group(0)

        if not result["email"]:
            mail_link = soup.find("a", href=re.compile(r"^mailto:"))
            if mail_link:
                result["email"] = mail_link["href"].replace("mailto:", "")

        if not result["website"]:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "steuerberater" not in href:
                    result["website"] = href
                    break

        for kw in ["taetigkeitsgebiet", "tätigkeitsgebiet", "schwerpunkt", "leistung", "fachgebiet"]:
            tag = soup.find(string=re.compile(kw, re.I))
            if tag:
                parent = tag.find_parent()
                if parent:
                    next_ul = parent.find_next("ul")
                    if next_ul:
                        result["taetigkeiten"] = [li.get_text(strip=True) for li in next_ul.find_all("li")]

        return result

    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Detail-Seite timeout")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Detail-Seite nicht erreichbar: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════
# MODUL 3: ENRICHMENT (Website-Impressum)
# ══════════════════════════════════════════════

@app.get("/enrich")
def enrich(website: str = Query(..., description="Website-URL der Kanzlei")):
    """
    Besucht die Website einer Kanzlei und extrahiert aus dem Impressum:
    E-Mail, Telefon, Geschaeftsfuehrer, Mitarbeiter-Hinweis.
    """
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        if not website.startswith("http"):
            website = "https://" + website

        base = website.rstrip("/")

        result = {
            "website": website, "email": "", "telefon": "",
            "geschaeftsfuehrer": "", "mitarbeiter_hinweis": "",
            "impressum_url": "",
        }

        # Impressum-URLs
        impressum_urls = [
            f"{base}/impressum", f"{base}/impressum/",
            f"{base}/impressum.html", f"{base}/de/impressum",
            f"{base}/kontakt", f"{base}/kontakt/",
            f"{base}/about", f"{base}/ueber-uns",
        ]

        # Hauptseite nach Impressum-Link durchsuchen
        try:
            main_resp = session.get(base, timeout=10)
            if main_resp.status_code == 200:
                main_soup = BeautifulSoup(main_resp.text, "html.parser")
                for a in main_soup.find_all("a", href=True):
                    if "impressum" in a["href"].lower() or "imprint" in a["href"].lower():
                        href = a["href"]
                        if not href.startswith("http"):
                            href = base + ("/" if not href.startswith("/") else "") + href
                        impressum_urls.insert(0, href)
                        break
        except Exception:
            pass

        # Impressum laden
        impressum_text = ""
        for imp_url in impressum_urls:
            try:
                resp = session.get(imp_url, timeout=10)
                if resp.status_code == 200 and len(resp.text) > 200:
                    result["impressum_url"] = imp_url
                    soup = BeautifulSoup(resp.text, "html.parser")
                    impressum_text = soup.get_text(separator="\n", strip=True)
                    break
            except Exception:
                continue

        if not impressum_text:
            return {**result, "error": "Kein Impressum gefunden."}

        # Daten extrahieren
        for line in impressum_text.split("\n"):
            line_s = line.strip()
            line_lower = line_s.lower()

            if not result["email"] and "@" in line_s:
                m = re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", line_s)
                if m:
                    result["email"] = m.group(0)

            if not result["telefon"] and any(kw in line_lower for kw in ["tel", "fon", "phone"]):
                tel = re.sub(r"^.*?(tel\.?|telefon|fon|phone)\s*:?\s*", "", line_s, flags=re.I).strip()
                if tel and len(tel) > 5:
                    result["telefon"] = tel

            if not result["geschaeftsfuehrer"] and any(kw in line_lower for kw in ["geschaeftsfuehr", "geschäftsführ", "inhaber", "vertretungsberechtigt"]):
                if ":" in line_s:
                    result["geschaeftsfuehrer"] = line_s.split(":", 1)[1].strip()
                else:
                    result["geschaeftsfuehrer"] = line_s

            if any(kw in line_lower for kw in ["mitarbeiter", "team", "beschaeftigte", "beschäftigte", "kollegen"]):
                nums = re.findall(r"\d+", line_s)
                if nums:
                    result["mitarbeiter_hinweis"] = line_s

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
