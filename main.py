from fastapi import FastAPI, HTTPException, Query
import mechanize
from bs4 import BeautifulSoup
import logging
import re

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="Handelsregister API")


def create_browser() -> mechanize.Browser:
    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.set_handle_equiv(True)
    br.set_handle_gzip(True)
    br.set_handle_refresh(False)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.addheaders = [
        ("User-Agent",
         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
         "AppleWebKit/537.36 (KHTML, like Gecko) "
         "Chrome/124.0.0.0 Safari/537.36"),
        ("Accept-Language", "de-DE,de;q=0.9,en;q=0.8"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Connection", "keep-alive"),
    ]
    return br


def parse_result(result):
    """Parse eine Ergebnis-Zeile (wie bundesAPI)."""
    cells = []
    for cell in result.find_all("td"):
        cells.append(cell.text.strip())

    if len(cells) < 5:
        return None

    d = {}
    d["court"] = cells[1]

    reg_match = re.search(
        r"(HRA|HRB|GnR|VR|PR)\s*\d+(\s+[A-Z])?(?!\w)", d["court"]
    )
    d["register_num"] = reg_match.group(0) if reg_match else None
    d["name"] = cells[2]
    d["state"] = cells[3]
    d["status"] = cells[4].strip()

    return d


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/search")
def search(schlagwoerter: str = Query(...)):
    try:
        br = create_browser()

        # Schritt 1: Startseite oeffnen
        logger.info("Schritt 1: Startseite oeffnen")
        br.open("https://www.handelsregister.de", timeout=30)
        logger.info(f"  -> Titel: {br.title()}")

        # Schritt 2: Zur erweiterten Suche navigieren
        logger.info("Schritt 2: Navigiere zur erweiterten Suche")
        br.select_form(name="naviForm")
        br.form.new_control(
            "hidden", "naviForm:erweiterteSucheLink",
            {"value": "naviForm:erweiterteSucheLink"},
        )
        br.form.new_control("hidden", "target", {"value": "erweiterteSucheLink"})
        resp_search = br.submit()
        logger.info(f"  -> Titel: {br.title()}")

        # Schritt 3: Suchformular ausfuellen und absenden
        logger.info(f"Schritt 3: Suche nach '{schlagwoerter}'")
        br.select_form(name="form")
        br["form:schlagwoerter"] = schlagwoerter
        br["form:schlagwortOptionen"] = ["1"]
        resp_result = br.submit()
        html = resp_result.read().decode("utf-8")
        logger.info(f"  -> Titel: {br.title()}, HTML-Laenge: {len(html)}")

        # Schritt 4: Ergebnisse parsen
        soup = BeautifulSoup(html, "html.parser")
        grid = soup.find("table", role="grid")

        results = []
        if grid:
            for row in grid.find_all("tr"):
                if row.get("data-ri") is not None:
                    d = parse_result(row)
                    if d:
                        results.append(d)

        if not results:
            page_text = soup.get_text(separator=" ", strip=True)[:300]
            logger.info(f"Keine Treffer. Text: {page_text}")

        return {
            "query": schlagwoerter,
            "count": len(results),
            "results": results,
        }

    except Exception as e:
        logger.error(f"Fehler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug")
def debug():
    """Debug: zeigt ob Navigation funktioniert."""
    try:
        br = create_browser()

        br.open("https://www.handelsregister.de", timeout=30)
        title1 = br.title()
        forms1 = [f.name for f in br.forms()]

        br.select_form(name="naviForm")
        br.form.new_control(
            "hidden", "naviForm:erweiterteSucheLink",
            {"value": "naviForm:erweiterteSucheLink"},
        )
        br.form.new_control("hidden", "target", {"value": "erweiterteSucheLink"})
        br.submit()
        title2 = br.title()
        forms2 = [f.name for f in br.forms()]

        return {
            "step1_title": title1,
            "step1_forms": forms1,
            "step2_title": title2,
            "step2_forms": forms2,
            "status": "ok",
        }
    except Exception as e:
        return {"error": str(e)}
