#!/usr/bin/env python3
"""
SLC Ice Times Scraper

Scrapes ice rink schedules from:
- Acord Ice Center (QuickScores PDF)
- County Ice Center (QuickScores PDF)
- SLC Sports Complex (QuickScores PDF)
- Cottonwood Heights Rec Center (HTML)

Outputs a unified schedules.json for the frontend.
"""

import json
import re
import sys
import os
from datetime import datetime, date, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional

import requests
import pdfplumber
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config: Define rink sources
# ---------------------------------------------------------------------------

QUICKSCORES_BASE = "https://www.quickscores.com"

# Each QuickScores rink has pages that link to current PDFs
RINKS_CONFIG = [
    {
        "id": "acord",
        "name": "Acord Ice Center",
        "phone": "385-468-1965",
        "address": "5353 West 3100 South, West Valley City, UT 84120",
        "website": "https://slco.org/acord-ice",
        "lat": 40.7006,
        "lng": -111.9774,
        "source_type": "quickscores",
        "org": "slchockey",
        # Pages that contain PDF links for this rink
        "pdf_pages": [
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15150",  # Drop-In/SP
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15154",  # Public Skate
        ],
        # Also check the central downloads page
        "downloads_page": f"{QUICKSCORES_BASE}/Orgs/Downloads.php?OrgDir=slchockey",
        # Keywords to match PDFs for this rink (in the link text or URL)
        "pdf_keywords": ["acord", "stick_and_puck", "drop_in_hockey"],
        "public_keywords": ["acord", "public"],
    },
    {
        "id": "county",
        "name": "County Ice Center",
        "phone": "385-468-1655",
        "address": "5201 South Murray Park Lane, Murray, UT 84107",
        "website": "https://slco.org/county-ice-center",
        "lat": 40.6436,
        "lng": -111.8878,
        "source_type": "quickscores",
        "org": "slchockey",
        "pdf_pages": [
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15151",  # Drop-In/SP
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15155",  # Public Skate
        ],
        "downloads_page": f"{QUICKSCORES_BASE}/Orgs/Downloads.php?OrgDir=slchockey",
        "pdf_keywords": ["cic", "county"],
        "public_keywords": ["cic", "county"],
    },
    {
        "id": "sports_complex",
        "name": "SLC Sports Complex",
        "phone": "385-468-1918",
        "address": "645 S Guardsman Way, Salt Lake City, UT 84108",
        "website": "https://slco.org/slc-sports-complex-ice",
        "lat": 40.7537,
        "lng": -111.8345,
        "source_type": "quickscores",
        "org": "sportscomplex",
        "pdf_pages": [
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=sportscomplex&ExtraMsgID=14779",  # Drop-In/SP
            f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=sportscomplex&ExtraMsgID=15841",  # Public Skate
        ],
        "downloads_page": f"{QUICKSCORES_BASE}/Orgs/Downloads.php?OrgDir=sportscomplex",
        "pdf_keywords": ["stick", "puck", "drop", "spdi"],
        "public_keywords": ["public"],
    },
    {
        "id": "cottonwood",
        "name": "Cottonwood Heights Rec Center",
        "phone": "(801) 943-3190",
        "address": "7500 S 2700 E, Cottonwood Heights, UT 84121",
        "website": "https://www.chparksandrecut.gov/ice-arena",
        "lat": 40.6198,
        "lng": -111.8106,
        "source_type": "html",
        "pages": {
            "stick_n_puck": "https://www.chparksandrecut.gov/stick-n-puck",
            "public_skate": "https://www.chparksandrecut.gov/public-ice-skating",
        },
    },
]

HEADERS = {
    "User-Agent": "SLC-Ice-Times-Bot/1.0 (community schedule aggregator)"
}

# ---------------------------------------------------------------------------
# PDF Link Discovery
# ---------------------------------------------------------------------------


def discover_pdf_links(rink_config: dict) -> list[dict]:
    """
    Scrape the rink's QuickScores pages to find current PDF download links.
    Returns list of {"url": ..., "label": ..., "category": "sp_di"|"public"}
    """
    found_pdfs = []
    seen_urls = set()

    # First check the ExtraMsg pages (most reliable - these are the rink-specific pages)
    for page_url in rink_config.get("pdf_pages", []):
        try:
            resp = requests.get(page_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/downloads/" in href and href.endswith(".pdf"):
                    full_url = href if href.startswith("http") else QUICKSCORES_BASE + href
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        label = link.get_text(strip=True)
                        category = classify_pdf(full_url, label)
                        found_pdfs.append({
                            "url": full_url,
                            "label": label,
                            "category": category,
                            "source": page_url,
                        })
        except Exception as e:
            print(f"  Warning: Failed to fetch {page_url}: {e}", file=sys.stderr)

    # Also check the downloads page for any we might have missed
    downloads_url = rink_config.get("downloads_page")
    if downloads_url:
        try:
            resp = requests.get(downloads_url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            rink_id = rink_config["id"]
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/downloads/" in href and href.endswith(".pdf"):
                    full_url = href if href.startswith("http") else QUICKSCORES_BASE + href
                    if full_url not in seen_urls:
                        label = link.get_text(strip=True)
                        url_lower = full_url.lower()
                        label_lower = label.lower()

                        # Check if this PDF belongs to our rink
                        is_ours = is_pdf_for_rink(rink_config, url_lower, label_lower)
                        if is_ours:
                            seen_urls.add(full_url)
                            category = classify_pdf(full_url, label)
                            found_pdfs.append({
                                "url": full_url,
                                "label": label,
                                "category": category,
                                "source": downloads_url,
                            })
        except Exception as e:
            print(f"  Warning: Failed to fetch {downloads_url}: {e}", file=sys.stderr)

    return found_pdfs


def is_pdf_for_rink(rink_config: dict, url_lower: str, label_lower: str) -> bool:
    """Check if a PDF from the downloads page belongs to a specific rink."""
    rink_id = rink_config["id"]

    if rink_id == "acord":
        # Acord PDFs: contain "acord" or specific Acord patterns
        # But NOT "CIC" (County Ice Center) patterns
        if "cic" in label_lower or "cic" in url_lower:
            return False
        if "acord" in label_lower or "acord" in url_lower:
            return True
        # Acord uses filenames like "Stick_and_Puck__Drop_In_Hockey__Schedule"
        if "stick_and_puck" in url_lower or "drop_in_hockey" in url_lower:
            return True
        # Public skate files named like "Public_March_2026"
        if "public_" in url_lower and "cic" not in url_lower:
            # Check it's not a CIC public file
            if "_public_skating" not in url_lower:
                return True
        return False

    elif rink_id == "county":
        # County Ice Center PDFs: labeled "CIC" in downloads
        if "cic" in label_lower:
            return True
        # URL patterns like "2026_March_SPDI" or "2026_March_Public_Skating"
        if re.search(r"\d{4}_\w+_spdi", url_lower):
            return True
        if re.search(r"\d{4}_\w+_public_skating", url_lower):
            return True
        return False

    elif rink_id == "sports_complex":
        # Sports Complex has its own org, so downloads page is separate
        return True  # Everything on their downloads page is theirs

    return False


def classify_pdf(url: str, label: str) -> str:
    """Classify a PDF as sp_di (stick & puck / drop-in) or public_skate."""
    combined = (url + " " + label).lower()
    if any(kw in combined for kw in ["public", "public_skating", "public skate"]):
        return "public_skate"
    if any(kw in combined for kw in ["stick", "puck", "drop", "spdi", "sp/di", "sp di"]):
        return "sp_di"
    if "freestyle" in combined:
        return "freestyle"
    return "unknown"


# ---------------------------------------------------------------------------
# PDF Parsing
# ---------------------------------------------------------------------------

MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]


def extract_month_year_from_pdf(text: str, url: str) -> tuple[int, int]:
    """Extract month and year from PDF text or URL."""
    # Try from text first - look for patterns like "March 2026"
    for i, month_name in enumerate(MONTH_NAMES, 1):
        pattern = rf"(?i){month_name}\s+(\d{{4}})"
        match = re.search(pattern, text)
        if match:
            return i, int(match.group(1))

    # Try from URL
    url_lower = url.lower()
    for i, month_name in enumerate(MONTH_NAMES, 1):
        if month_name in url_lower:
            year_match = re.search(r"20\d{2}", url_lower)
            if year_match:
                return i, int(year_match.group())

    raise ValueError(f"Could not determine month/year from PDF text or URL: {url}")


def parse_quickscores_pdf(pdf_bytes: bytes, url: str, category: str) -> list[dict]:
    """
    Parse a QuickScores schedule PDF into a list of session dicts.
    Returns: [{"date": "2026-03-01", "type": "stick_and_puck", "start": "09:00", "end": "10:00", "price": 7}, ...]
    """
    sessions = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    if not full_text.strip():
        print(f"  Warning: Empty text from PDF {url}", file=sys.stderr)
        return sessions

    try:
        month, year = extract_month_year_from_pdf(full_text, url)
    except ValueError as e:
        print(f"  Warning: {e}", file=sys.stderr)
        return sessions

    # Parse the calendar grid
    # The text comes out as lines. Day numbers appear as standalone numbers,
    # followed by session lines like "SP 10:15-11:15am" or "DI 7:45-9:15pm"
    # or for public skate: "12:00-1:30pm" or with labels

    lines = full_text.split("\n")
    current_day = None

    # Session pattern: optional type code, then time range
    # Examples: "SP 9:00-10:00am", "DI 7:45-9:15pm", "PS 12:00-1:30pm"
    # Some PDFs just have times without codes for public skate
    session_pattern = re.compile(
        r"(?:(SP|DI|PS|FS)\s+)?(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm|AM|PM)",
        re.IGNORECASE
    )

    # Day number pattern - standalone number 1-31
    day_pattern = re.compile(r"^(\d{1,2})$")

    # Skip header lines
    skip_patterns = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip header rows
        line_lower = line.lower()
        if any(line_lower.startswith(s) for s in skip_patterns):
            continue
        if "schedule" in line_lower and "subject" in line_lower:
            continue

        # Check for day number
        day_match = day_pattern.match(line)
        if day_match:
            day_num = int(day_match.group(1))
            if 1 <= day_num <= 31:
                current_day = day_num
                continue

        # Check for session times
        session_matches = session_pattern.finditer(line)
        for match in session_matches:
            if current_day is None:
                continue

            type_code = match.group(1)
            start_time_str = match.group(2)
            end_time_str = match.group(3)
            ampm = match.group(4).lower()

            # Determine session type
            if type_code:
                type_code = type_code.upper()
                if type_code == "SP":
                    session_type = "stick_and_puck"
                elif type_code == "DI":
                    session_type = "drop_in"
                elif type_code == "PS":
                    session_type = "public_skate"
                elif type_code == "FS":
                    session_type = "freestyle"
                else:
                    session_type = "unknown"
            else:
                # No type code - infer from PDF category
                if category == "public_skate":
                    session_type = "public_skate"
                elif category == "sp_di":
                    session_type = "stick_and_puck"  # default for SP/DI PDFs
                elif category == "freestyle":
                    session_type = "freestyle"
                else:
                    session_type = "unknown"

            # Convert times to 24h format
            start_24 = convert_to_24h(start_time_str, ampm, is_start=True)
            end_24 = convert_to_24h(end_time_str, ampm, is_start=False)

            try:
                session_date = date(year, month, current_day)
            except ValueError:
                continue  # Invalid date (e.g., Feb 30)

            sessions.append({
                "date": session_date.isoformat(),
                "type": session_type,
                "start": start_24,
                "end": end_24,
            })

    return sessions


def convert_to_24h(time_str: str, ampm: str, is_start: bool) -> str:
    """
    Convert a time string like '9:00' with am/pm to 24h format '09:00'.
    The ampm applies to the end time; for start times we need to infer.
    E.g., "7:45-9:15pm" means 7:45pm-9:15pm, but "9:00-10:00am" means 9:00am-10:00am.
    """
    hour, minute = map(int, time_str.split(":"))

    if ampm == "pm" and hour != 12:
        # For ranges like "7:45-9:15pm", both times are PM
        # But for "11:30-12:30pm", 11:30 is AM and 12:30 is PM
        if is_start and hour >= 1 and hour <= 6:
            # Likely PM (e.g., 5:15-6:15pm)
            hour += 12
        elif is_start and hour >= 7 and hour <= 11:
            # Could be AM crossing to PM (e.g., 11:30-12:30pm)
            # or PM (e.g., 7:45-9:15pm)
            # Heuristic: if hour < 12 and it's a start time with PM end,
            # most evening sessions start PM too
            # But morning sessions like 11:30-12:30pm the 11:30 is AM
            if hour >= 10:
                pass  # Keep as-is (AM), like 10:00am-12:00pm or 11:30-12:30pm
            else:
                hour += 12  # Evening session
        elif not is_start and hour != 12:
            hour += 12
    elif ampm == "am":
        if hour == 12:
            hour = 0

    return f"{hour:02d}:{minute:02d}"


# ---------------------------------------------------------------------------
# Cottonwood Heights HTML Scraping
# ---------------------------------------------------------------------------


def scrape_cottonwood_heights(rink_config: dict) -> list[dict]:
    """
    Scrape Cottonwood Heights Recreation Center schedules from their website.
    They use a recurring weekly schedule with monthly overrides noted at the top.
    """
    sessions = []
    today = date.today()

    # Scrape Stick 'n Puck
    snp_url = rink_config["pages"]["stick_n_puck"]
    try:
        resp = requests.get(snp_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        sessions.extend(parse_cottonwood_stick_n_puck(resp.text, today))
    except Exception as e:
        print(f"  Warning: Failed to scrape Cottonwood S&P: {e}", file=sys.stderr)

    # Scrape Public Skate
    ps_url = rink_config["pages"]["public_skate"]
    try:
        resp = requests.get(ps_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        sessions.extend(parse_cottonwood_public_skate(resp.text, today))
    except Exception as e:
        print(f"  Warning: Failed to scrape Cottonwood Public Skate: {e}", file=sys.stderr)

    return sessions


def parse_cottonwood_stick_n_puck(html: str, ref_date: date) -> list[dict]:
    """
    Parse Cottonwood Heights Stick 'n Puck page.
    They have a recurring weekly schedule plus monthly additions/cancellations.

    School year schedule (approx Sept 1 - June 1):
    - Monday: 5:30 - 6:30am
    - Tuesday: 11:30am-12:30pm, 12:30-1:30pm (no sessions March)
    - Wednesday: 5:30 - 6:30am

    Plus additional Saturday sessions noted in monthly tables.
    """
    sessions = []
    soup = BeautifulSoup(html, "html.parser")

    # Base recurring schedule (school year)
    # Monday = 0, Tuesday = 1, ..., Sunday = 6
    recurring = {
        0: [("05:30", "06:30")],  # Monday
        1: [("11:30", "12:30"), ("12:30", "13:30")],  # Tuesday
        2: [("05:30", "06:30")],  # Wednesday
    }

    # Look for monthly cancellations in the page text
    page_text = soup.get_text().lower()

    # Check for "no stick n' pucks on tuesdays for the month of march" type notices
    current_month_name = ref_date.strftime("%B").lower()
    if f"no stick" in page_text and "tuesday" in page_text and current_month_name in page_text:
        # Remove Tuesday sessions for current month
        tuesday_cancelled_months = [ref_date.month]
    else:
        tuesday_cancelled_months = []

    # Generate sessions for current month and next month
    for month_offset in range(2):
        if month_offset == 0:
            gen_month = ref_date.month
            gen_year = ref_date.year
        else:
            gen_month = ref_date.month + 1
            gen_year = ref_date.year
            if gen_month > 12:
                gen_month = 1
                gen_year += 1

        # Generate each day of the month
        day = 1
        while True:
            try:
                d = date(gen_year, gen_month, day)
            except ValueError:
                break

            weekday = d.weekday()
            if weekday in recurring:
                # Check if cancelled
                if weekday == 1 and gen_month in tuesday_cancelled_months:
                    day += 1
                    continue

                for start, end in recurring[weekday]:
                    sessions.append({
                        "date": d.isoformat(),
                        "type": "stick_and_puck",
                        "start": start,
                        "end": end,
                    })

            day += 1

    # Parse additional sessions from tables (Saturday extras etc.)
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        headers = [th.get_text(strip=True).lower() for th in (rows[0].find_all(["th", "td"]) if rows else [])]

        for row in rows[1:]:
            cells = row.find_all("td")
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                # Look for dates like "March 14"
                date_match = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})", text, re.IGNORECASE)
                if date_match:
                    month_name = date_match.group(1).lower()
                    day_num = int(date_match.group(2))
                    month_num = MONTH_NAMES.index(month_name) + 1
                    year = ref_date.year

                    # Get the time from the header
                    if i < len(headers):
                        time_match = re.search(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)", headers[i], re.IGNORECASE)
                        if time_match:
                            start_str = time_match.group(1)
                            end_str = time_match.group(2)
                            ampm = time_match.group(3).lower()
                            start_24 = convert_to_24h(start_str, ampm, is_start=True)
                            end_24 = convert_to_24h(end_str, ampm, is_start=False)

                            try:
                                session_date = date(year, month_num, day_num)
                                sessions.append({
                                    "date": session_date.isoformat(),
                                    "type": "stick_and_puck",
                                    "start": start_24,
                                    "end": end_24,
                                })
                            except ValueError:
                                pass

    return sessions


def parse_cottonwood_public_skate(html: str, ref_date: date) -> list[dict]:
    """
    Parse Cottonwood Heights public skate page.
    They post a calendar approximately the 27th of each month for the upcoming month.
    The page has embedded calendar images/tables.
    """
    # Cottonwood Heights public skate is harder - they often use images for calendars
    # For now, we'll note it as a source and extract what we can from text
    sessions = []
    soup = BeautifulSoup(html, "html.parser")

    # Look for any structured time data in the page
    page_text = soup.get_text()

    # Common public skate patterns: day-of-week based schedules
    # This is a best-effort parse - Cottonwood Heights doesn't structure this as well
    # We'll mark these as needing manual verification

    return sessions


# ---------------------------------------------------------------------------
# Main Scraper Logic
# ---------------------------------------------------------------------------


def scrape_rink(rink_config: dict) -> dict:
    """Scrape a single rink's schedule data."""
    print(f"Scraping: {rink_config['name']}...", file=sys.stderr)

    sessions = []

    if rink_config["source_type"] == "quickscores":
        # Discover PDF links
        pdfs = discover_pdf_links(rink_config)
        print(f"  Found {len(pdfs)} PDFs", file=sys.stderr)

        for pdf_info in pdfs:
            if pdf_info["category"] in ("unknown", "freestyle"):
                print(f"  Skipping ({pdf_info['category']}): {pdf_info['label']}", file=sys.stderr)
                continue

            print(f"  Downloading: {pdf_info['label']} ({pdf_info['category']})", file=sys.stderr)
            try:
                resp = requests.get(pdf_info["url"], headers=HEADERS, timeout=60)
                resp.raise_for_status()
                parsed = parse_quickscores_pdf(resp.content, pdf_info["url"], pdf_info["category"])
                print(f"    Parsed {len(parsed)} sessions", file=sys.stderr)
                sessions.extend(parsed)
            except Exception as e:
                print(f"    Error: {e}", file=sys.stderr)

    elif rink_config["source_type"] == "html":
        sessions = scrape_cottonwood_heights(rink_config)
        print(f"  Parsed {len(sessions)} sessions", file=sys.stderr)

    # Deduplicate sessions (same date+type+start+end)
    seen = set()
    unique_sessions = []
    for s in sessions:
        key = (s["date"], s["type"], s["start"], s["end"])
        if key not in seen:
            seen.add(key)
            unique_sessions.append(s)

    # Sort by date then start time
    unique_sessions.sort(key=lambda s: (s["date"], s["start"]))

    return {
        "id": rink_config["id"],
        "name": rink_config["name"],
        "phone": rink_config["phone"],
        "address": rink_config["address"],
        "website": rink_config.get("website", ""),
        "lat": rink_config.get("lat"),
        "lng": rink_config.get("lng"),
        "sessions": unique_sessions,
    }


def main():
    output_path = Path(__file__).parent.parent / "data" / "schedules.json"

    rinks_data = []
    for config in RINKS_CONFIG:
        rink_data = scrape_rink(config)
        rinks_data.append(rink_data)

    output = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "rinks": rinks_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    total_sessions = sum(len(r["sessions"]) for r in rinks_data)
    print(f"\nDone! {total_sessions} total sessions across {len(rinks_data)} rinks.", file=sys.stderr)
    print(f"Output: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
