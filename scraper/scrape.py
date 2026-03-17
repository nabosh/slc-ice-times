#!/usr/bin/env python3
"""
SLC Ice Times Scraper

Scrapes ice rink schedules from:
- Acord Ice Center (QuickScores PDF)
- County Ice Center (QuickScores PDF)
- SLC Sports Complex (QuickScores PDF)
- Cottonwood Heights Rec Center (HTML + PDF)

Outputs a unified schedules.json for the frontend.
"""

import json
import re
import sys
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path

import pdfplumber
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

QUICKSCORES_BASE = "https://www.quickscores.com"

HEADERS = {
    "User-Agent": "SLC-Ice-Times-Bot/1.0 (community schedule aggregator)"
}

MONTH_NAMES = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

RINKS = [
    {
        "id": "acord",
        "name": "Acord Ice Center",
        "phone": "385-468-1965",
        "address": "5353 West 3100 South, West Valley City, UT 84120",
        "website": "https://slco.org/acord-ice",
        "lat": 40.7006,
        "lng": -111.9774,
    },
    {
        "id": "county",
        "name": "County Ice Center",
        "phone": "385-468-1655",
        "address": "5201 South Murray Park Lane, Murray, UT 84107",
        "website": "https://slco.org/county-ice-center",
        "lat": 40.6436,
        "lng": -111.8878,
    },
    {
        "id": "sports_complex",
        "name": "SLC Sports Complex",
        "phone": "385-468-1918",
        "address": "645 S Guardsman Way, Salt Lake City, UT 84108",
        "website": "https://slco.org/slc-sports-complex-ice",
        "lat": 40.7537,
        "lng": -111.8345,
    },
    {
        "id": "cottonwood",
        "name": "Cottonwood Heights Rec Center",
        "phone": "(801) 943-3190",
        "address": "7500 S 2700 E, Cottonwood Heights, UT 84121",
        "website": "https://www.chparksandrecut.gov/ice-arena",
        "lat": 40.6198,
        "lng": -111.8106,
    },
]

# PDF source pages for each rink (ExtraMsg pages on QuickScores)
PDF_SOURCES = {
    "acord": [
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15150",  # SPDI
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15154",  # Public
    ],
    "county": [
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15151",  # SPDI
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=slchockey&ExtraMsgID=15155",  # Public
    ],
    "sports_complex": [
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=sportscomplex&ExtraMsgID=14779",  # SPDI
        f"{QUICKSCORES_BASE}/Orgs/ExtraMsg.php?OrgDir=sportscomplex&ExtraMsgID=15841",  # Public
    ],
}

COTTONWOOD_PAGES = {
    "stick_n_puck": "https://www.chparksandrecut.gov/stick-n-puck",
    "public_skate": "https://www.chparksandrecut.gov/public-ice-skating",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch(url: str, timeout: int = 30) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp


def log(msg: str):
    print(msg, file=sys.stderr)


def parse_time_12h(time_str: str, ampm: str) -> str:
    """Convert '9:00' + 'am' -> '09:00', '1:30' + 'pm' -> '13:30'."""
    time_str = time_str.strip().replace(".", ":")
    parts = time_str.split(":")
    hour = int(parts[0])
    minute = int(parts[1]) if len(parts) > 1 else 0
    ampm = ampm.lower().strip().rstrip(".")

    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0

    return f"{hour:02d}:{minute:02d}"


def extract_month_year(text: str, url: str) -> tuple[int, int]:
    """Extract month and year from PDF text or URL."""
    combined = text + " " + url

    # Try "Month Year" pattern (e.g., "March 2026")
    for i, name in enumerate(MONTH_NAMES, 1):
        m = re.search(rf"(?i){name}\s+(\d{{4}})", combined)
        if m:
            return i, int(m.group(1))

    # Try "Year_Month" or "Year Month" pattern (e.g., "2026_March" in URL)
    for i, name in enumerate(MONTH_NAMES, 1):
        m = re.search(rf"(?i)(\d{{4}})[_\s]+{name}", combined)
        if m:
            return i, int(m.group(1))

    # Try "Rev. M/D/YYYY" pattern (e.g., "Rev. 3/2/2026")
    m = re.search(r"Rev\.\s*(\d{1,2})/\d{1,2}/(\d{4})", text)
    if m:
        return int(m.group(1)), int(m.group(2))

    raise ValueError(f"Could not determine month/year from: {url}")


# ---------------------------------------------------------------------------
# PDF Link Discovery
# ---------------------------------------------------------------------------


def discover_pdfs(rink_id: str) -> list[dict]:
    """Find schedule PDF URLs from a rink's ExtraMsg pages.
    Returns [{"url": ..., "category": "spdi"|"public"}].
    """
    pages = PDF_SOURCES.get(rink_id, [])
    found = []
    seen = set()

    for page_url in pages:
        try:
            resp = fetch(page_url)
        except Exception as e:
            log(f"  Warning: Failed to fetch {page_url}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Determine category from which ExtraMsg page this is
        # SPDI pages: ExtraMsgIDs 15150, 15151, 14779
        # Public pages: ExtraMsgIDs 15154, 15155, 15841
        msg_id = re.search(r"ExtraMsgID=(\d+)", page_url)
        is_public_page = msg_id and msg_id.group(1) in ("15154", "15155", "15841")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            # Only org-specific PDFs, skip generic QuickScores help docs
            if not href.endswith(".pdf"):
                continue
            if "/downloads/files/" in href or "API/" in href:
                continue

            full_url = href if href.startswith("http") else QUICKSCORES_BASE + href
            if full_url in seen:
                continue
            seen.add(full_url)

            label = link.get_text(strip=True)
            combined = (full_url + " " + label).lower()

            # Skip freestyle PDFs
            if "freestyle" in combined:
                log(f"  Skipping freestyle: {label}")
                continue

            category = "public" if is_public_page or "public" in combined else "spdi"
            found.append({"url": full_url, "label": label, "category": category})

    return found


# ---------------------------------------------------------------------------
# PDF Parsing: Combined-cell format (Acord SPDI, County SPDI)
# ---------------------------------------------------------------------------
# Table cells look like: "1\nSP 10:15-11:15am\nSP 11:30-12:30pm"


def parse_combined_cell_table(table: list[list[str]], month: int, year: int, default_type: str) -> list[dict]:
    """Parse a calendar table where each cell has day number + sessions combined."""
    sessions = []
    # Pattern: optional type code, time range with am/pm
    pat = re.compile(
        r"(?:(SP|DI|PS)\s+)?(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)",
        re.IGNORECASE,
    )

    for row in table[1:]:  # Skip header row
        for cell in row:
            if not cell:
                continue
            lines = cell.strip().split("\n")
            if not lines:
                continue

            # First line (or part of it) should be the day number
            first = lines[0].strip()
            day_match = re.match(r"^(\d{1,2})", first)
            if not day_match:
                continue
            day = int(day_match.group(1))
            if day < 1 or day > 31:
                continue

            try:
                session_date = date(year, month, day)
            except ValueError:
                continue

            # Parse all session times from all lines in the cell
            cell_text = "\n".join(lines)
            for m in pat.finditer(cell_text):
                code = (m.group(1) or "").upper()
                start_str, end_str, ampm = m.group(2), m.group(3), m.group(4).lower()

                if code == "SP":
                    stype = "stick_and_puck"
                elif code == "DI":
                    stype = "drop_in"
                elif code == "PS":
                    stype = "public_skate"
                else:
                    stype = default_type

                start_h = int(start_str.split(":")[0])
                end_h = int(end_str.split(":")[0])

                # Determine AM/PM for start time when only end has AM/PM
                # "SP 7:45-9:15pm" -> both PM
                # "11:30-12:30pm" -> 11:30 AM, 12:30 PM (crossing noon)
                # "12:30-1:30pm" -> 12:30 PM, 1:30 PM
                if ampm == "pm":
                    if start_h == 12:
                        start_24 = parse_time_12h(start_str, "pm")  # 12:xx is PM (noon)
                    elif start_h >= 10 and end_h <= 12:
                        start_24 = parse_time_12h(start_str, "am")  # AM crossing to PM
                    else:
                        start_24 = parse_time_12h(start_str, "pm")
                    end_24 = parse_time_12h(end_str, "pm")
                else:
                    start_24 = parse_time_12h(start_str, "am")
                    end_24 = parse_time_12h(end_str, "am")

                sessions.append({
                    "date": session_date.isoformat(),
                    "type": stype,
                    "start": start_24,
                    "end": end_24,
                })

    return sessions


# ---------------------------------------------------------------------------
# PDF Parsing: Alternating-row format (Acord Public, County Public)
# ---------------------------------------------------------------------------
# Row of day numbers: ['1', '2', '3', '4', '5', '6', '7']
# Next row of times:  ['12:45-\n2:45p', '11:45a\n-2:45p', '', ...]


def parse_alternating_row_table(table: list[list[str]], month: int, year: int) -> list[dict]:
    """Parse calendar where day numbers and times are in alternating rows."""
    sessions = []

    rows = table[1:]  # Skip header
    i = 0
    while i < len(rows):
        row = rows[i]
        # Check if this is a day-number row (cells are just numbers or empty)
        is_day_row = all(
            cell is None or cell.strip() == "" or re.match(r"^\d{1,2}$", cell.strip())
            for cell in row
        )

        if is_day_row and i + 1 < len(rows):
            time_row = rows[i + 1]
            for col_idx in range(min(len(row), len(time_row))):
                day_cell = (row[col_idx] or "").strip()
                time_cell = (time_row[col_idx] or "").strip()

                if not day_cell or not time_cell:
                    continue

                day_match = re.match(r"^(\d{1,2})$", day_cell)
                if not day_match:
                    continue
                day = int(day_match.group(1))

                try:
                    session_date = date(year, month, day)
                except ValueError:
                    continue

                # Parse time entries from the cell
                # Normalize: remove newlines within time ranges
                time_text = re.sub(r"\s*\n\s*", "", time_cell)
                # Split multiple time ranges (separated by spaces or newlines in original)
                # Try to split on boundaries between time entries
                entries = re.findall(
                    r"(\d{1,2}(?::\d{2})?[ap]?)\s*-\s*(\d{1,2}(?::\d{2})?[ap]?)",
                    time_text, re.IGNORECASE,
                )

                if not entries:
                    continue

                for start_raw, end_raw in entries:
                    start_24, end_24 = parse_time_range_short(start_raw, end_raw)
                    if start_24 and end_24:
                        sessions.append({
                            "date": session_date.isoformat(),
                            "type": "public_skate",
                            "start": start_24,
                            "end": end_24,
                        })
            i += 2
        elif not is_day_row:
            # This might be a combined-cell row (like in some PDFs the format switches mid-table)
            for cell in row:
                if not cell:
                    continue
                cell_text = cell.strip()
                day_match = re.match(r"^(\d{1,2})\n", cell_text)
                if day_match:
                    day = int(day_match.group(1))
                    try:
                        session_date = date(year, month, day)
                    except ValueError:
                        i += 1
                        continue
                    # Parse times from remaining text
                    time_entries = re.findall(
                        r"(\d{1,2}(?::\d{2})?)\s*-\s*(\d{1,2}(?::\d{2})?)\s*(am|pm|a|p)?\s*(Open|Public)?",
                        cell_text, re.IGNORECASE,
                    )
                    for start_raw, end_raw, ampm, label in time_entries:
                        start_24, end_24 = parse_time_range_short(
                            start_raw + (ampm[0] if ampm else ""),
                            end_raw + (ampm[0] if ampm else "p"),
                        )
                        if start_24 and end_24:
                            sessions.append({
                                "date": session_date.isoformat(),
                                "type": "public_skate",
                                "start": start_24,
                                "end": end_24,
                            })
            i += 1
        else:
            i += 1

    return sessions


def parse_time_range_short(start: str, end: str) -> tuple[str, str]:
    """Parse abbreviated time like '12:45p' or '2:45p' or '11:45a' into 24h."""
    def parse_one(s: str) -> tuple[int, int, str] | None:
        s = s.strip().rstrip(".")
        m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*([ap])?m?$", s, re.IGNORECASE)
        if not m:
            return None
        h = int(m.group(1))
        mi = int(m.group(2)) if m.group(2) else 0
        ap = (m.group(3) or "").lower()
        return h, mi, ap

    s = parse_one(start)
    e = parse_one(end)
    if not s or not e:
        return "", ""

    sh, smin, sap = s
    eh, emin, eap = e

    # If end has am/pm but start doesn't, infer
    if not sap and eap:
        # e.g., "11:45-2:45p" -> could be 11:45 AM to 2:45 PM (crossing noon)
        # e.g., "12:45-2:45p" -> 12:45 PM to 2:45 PM
        # e.g., "7:30-9:30p" -> 7:30 PM to 9:30 PM
        # Rule: if start > end and start is 10 or 11, it's AM crossing to PM
        if eap == "p" and sh > eh and sh >= 10 and sh < 12:
            sap = "a"  # Morning crossing to afternoon
        else:
            sap = eap
    if not eap and sap:
        eap = sap
    if not sap and not eap:
        # Default: assume PM for afternoon/evening times
        sap = "p" if sh < 6 or sh >= 12 else "a"
        eap = "p"

    # Convert to 24h
    if sap == "p" and sh != 12:
        sh += 12
    elif sap == "a" and sh == 12:
        sh = 0
    if eap == "p" and eh != 12:
        eh += 12
    elif eap == "a" and eh == 12:
        eh = 0

    return f"{sh:02d}:{smin:02d}", f"{eh:02d}:{emin:02d}"


# ---------------------------------------------------------------------------
# PDF Parsing: Sports Complex weekly format
# ---------------------------------------------------------------------------
# Cells like: "17\n() 12:45p-1:45p Stick\nand Puck\n() 1:45p-2:45p Stick\nand Puck"


def parse_sports_complex_table(table: list[list[str]], url: str) -> list[dict]:
    """Parse Sports Complex weekly calendar PDF."""
    sessions = []

    # Extract month/year from URL or we'll need text
    # These PDFs are weekly, not monthly. Get dates from cells.
    # The header row has day names, data row has cells with day numbers

    for row in table:
        for cell in row:
            if not cell or not cell.strip():
                continue

            # Check if cell starts with a day number
            lines = cell.strip().split("\n")
            day_match = re.match(r"^(\d{1,2})$", lines[0].strip())
            if not day_match:
                continue

            # We need the full date. The URL or PDF text should have month info.
            # For now we'll need to get month/year from the PDF text separately
            # This is handled by the caller

            # Parse session entries: () time-time Type
            cell_text = " ".join(lines[1:])  # Skip day number
            # Pattern: () time-time Type
            pat = re.compile(
                r"\(\)\s*(\d{1,2}(?::\d{2})?[ap]?)\s*-\s*(\d{1,2}(?::\d{2})?[ap]?)\s+"
                r"(Stick\s*and\s*Puck|Drop[- ]?in(?:\s+Hockey)?|Sled\s*Hockey\s*Drop[- ]?in|Public)",
                re.IGNORECASE,
            )
            for m in pat.finditer(cell_text):
                start_raw, end_raw, type_text = m.group(1), m.group(2), m.group(3).lower()

                if "stick" in type_text and "puck" in type_text:
                    stype = "stick_and_puck"
                elif "sled" in type_text:
                    continue  # Skip sled hockey
                elif "drop" in type_text:
                    stype = "drop_in"
                elif "public" in type_text:
                    stype = "public_skate"
                else:
                    continue

                start_24, end_24 = parse_time_range_short(start_raw, end_raw)
                if start_24 and end_24:
                    sessions.append({
                        "day_num": int(day_match.group(1)),
                        "type": stype,
                        "start": start_24,
                        "end": end_24,
                    })

    return sessions


# ---------------------------------------------------------------------------
# Cottonwood Heights Parsing
# ---------------------------------------------------------------------------


def parse_cottonwood_public_pdf(pdf_bytes: bytes, url: str) -> list[dict]:
    """Parse Cottonwood public skate PDF. Format: combined cells with 'time Open/Public'."""
    sessions = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        try:
            month, year = extract_month_year(full_text, url)
        except ValueError as e:
            log(f"  Warning: {e}")
            return sessions

        for page in pdf.pages:
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue
                header = [str(c or "").strip().lower() for c in table[0]]
                if not any(d in " ".join(header) for d in ["sun", "mon", "tue"]):
                    continue

                # This PDF uses combined cells or alternating rows
                # Check format of row 1
                row1 = table[1] if len(table) > 1 else []
                all_nums = all(
                    c is None or c.strip() == "" or re.match(r"^\d{1,2}$", (c or "").strip())
                    for c in row1
                )

                if all_nums:
                    # Alternating format - but Cottonwood also has combined cells sometimes
                    # Process pairs of rows
                    i = 1
                    while i < len(table):
                        row = table[i]
                        is_nums = all(
                            c is None or c.strip() == "" or re.match(r"^\d{1,2}$", (c or "").strip())
                            for c in row
                        )
                        if is_nums and i + 1 < len(table):
                            time_row = table[i + 1]
                            for col in range(min(len(row), len(time_row))):
                                day_str = (row[col] or "").strip()
                                time_str = (time_row[col] or "").strip()
                                if not day_str or not time_str:
                                    continue
                                try:
                                    day = int(day_str)
                                    session_date = date(year, month, day)
                                except (ValueError, TypeError):
                                    continue
                                sessions.extend(
                                    parse_cottonwood_time_cell(time_str, session_date)
                                )
                            i += 2
                        else:
                            # Combined cell format
                            for cell in row:
                                if not cell:
                                    continue
                                cell = cell.strip()
                                dm = re.match(r"^(\d{1,2})\n", cell)
                                if dm:
                                    try:
                                        day = int(dm.group(1))
                                        session_date = date(year, month, day)
                                    except ValueError:
                                        continue
                                    remaining = cell[dm.end():]
                                    sessions.extend(
                                        parse_cottonwood_time_cell(remaining, session_date)
                                    )
                            i += 1
                else:
                    # Combined cell format
                    for row in table[1:]:
                        for cell in row:
                            if not cell:
                                continue
                            cell = cell.strip()
                            dm = re.match(r"^(\d{1,2})\n", cell)
                            if not dm:
                                dm = re.match(r"^(\d{1,2})$", cell.split("\n")[0].strip())
                            if dm:
                                try:
                                    day = int(dm.group(1))
                                    session_date = date(year, month, day)
                                except ValueError:
                                    continue
                                remaining = cell[dm.end():]
                                sessions.extend(
                                    parse_cottonwood_time_cell(remaining, session_date)
                                )

    return sessions


def parse_cottonwood_time_cell(text: str, session_date: date) -> list[dict]:
    """Parse a Cottonwood public skate time cell like '11:30-1:30pm Open\n7-9pm Public'."""
    sessions = []
    # Normalize
    text = re.sub(r"\s*\n\s*", " ", text)

    # Pattern: time-time[am/pm] [Open|Public]
    pat = re.compile(
        r"(\d{1,2}(?::\d{2})?)\s*-\s*(\d{1,2}(?::\d{2})?)\s*(am|pm|a|p)\s*(Open|Public)?",
        re.IGNORECASE,
    )
    for m in pat.finditer(text):
        start_raw = m.group(1)
        end_raw = m.group(2) + m.group(3)
        start_24, end_24 = parse_time_range_short(start_raw, end_raw)
        if start_24 and end_24:
            sessions.append({
                "date": session_date.isoformat(),
                "type": "public_skate",
                "start": start_24,
                "end": end_24,
            })
    return sessions


def scrape_cottonwood_stick_n_puck(ref_date: date) -> list[dict]:
    """Scrape Cottonwood Heights Stick 'n Puck from their HTML page."""
    sessions = []

    try:
        resp = fetch(COTTONWOOD_PAGES["stick_n_puck"])
    except Exception as e:
        log(f"  Warning: Failed to fetch Cottonwood S&P: {e}")
        return sessions

    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text().lower()

    # Base recurring schedule (school year, approx Sept 1 - June 1)
    recurring = {
        0: [("05:30", "06:30")],  # Monday
        1: [("11:30", "12:30"), ("12:30", "13:30")],  # Tuesday
        2: [("05:30", "06:30")],  # Wednesday
    }

    # Check for monthly cancellations
    current_month = ref_date.strftime("%B").lower()
    tuesday_cancelled = (
        "no stick" in page_text
        and "tuesday" in page_text
        and current_month in page_text
    )

    # Generate sessions for current + next month
    for month_offset in range(2):
        gen_month = ref_date.month + month_offset
        gen_year = ref_date.year
        if gen_month > 12:
            gen_month -= 12
            gen_year += 1

        day = 1
        while True:
            try:
                d = date(gen_year, gen_month, day)
            except ValueError:
                break

            weekday = d.weekday()
            if weekday in recurring:
                if weekday == 1 and tuesday_cancelled and d.month == ref_date.month:
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

    # Parse additional Saturday sessions from the page
    # Look for table with extra sessions (dates like "March 14", "March 21")
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        # Parse time from headers
        header_times = []
        for h in headers:
            tm = re.search(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)", h, re.IGNORECASE)
            header_times.append(tm)

        for row in rows[1:]:
            cells = row.find_all("td")
            for i, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                dm = re.search(
                    r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})",
                    text, re.IGNORECASE,
                )
                if dm and i < len(header_times) and header_times[i]:
                    month_num = MONTH_NAMES.index(dm.group(1).lower()) + 1
                    day_num = int(dm.group(2))
                    tm = header_times[i]
                    start_str, end_str, ampm = tm.group(1), tm.group(2), tm.group(3).lower()

                    start_h = int(start_str.split(":")[0])
                    end_h = int(end_str.split(":")[0])
                    if ampm == "pm":
                        if start_h == 12:
                            start_24 = parse_time_12h(start_str, "pm")
                        elif start_h >= 10 and end_h <= 12:
                            start_24 = parse_time_12h(start_str, "am")
                        else:
                            start_24 = parse_time_12h(start_str, "pm")
                        end_24 = parse_time_12h(end_str, "pm")
                    else:
                        start_24 = parse_time_12h(start_str, "am")
                        end_24 = parse_time_12h(end_str, "am")

                    try:
                        session_date = date(ref_date.year, month_num, day_num)
                        sessions.append({
                            "date": session_date.isoformat(),
                            "type": "stick_and_puck",
                            "start": start_24,
                            "end": end_24,
                        })
                    except ValueError:
                        pass

    return sessions


# ---------------------------------------------------------------------------
# QuickScores PDF Processing
# ---------------------------------------------------------------------------


def process_quickscores_pdf(pdf_bytes: bytes, url: str, category: str) -> list[dict]:
    """Process a QuickScores PDF and return sessions."""
    sessions = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

        # Check for CID-encoded (unreadable) PDFs
        if "(cid:" in full_text and full_text.count("(cid:") > 10:
            log("    Warning: PDF uses CID encoding, text is unreadable. Skipping.")
            return sessions

        try:
            month, year = extract_month_year(full_text, url)
        except ValueError as e:
            log(f"    Warning: {e}")
            return sessions

        for page in pdf.pages:
            for table in page.extract_tables():
                if not table or len(table) < 2:
                    continue

                # Identify if this is a calendar table
                header = [str(c or "").strip().lower() for c in table[0]]
                header_str = " ".join(header)
                if not any(d in header_str for d in ["sun", "mon", "tue", "monday", "tuesday"]):
                    continue

                # Determine table format by inspecting first data row
                first_data = table[1]
                sample_cells = [c for c in first_data if c and c.strip()]

                if not sample_cells:
                    continue

                # Check if cells combine day+sessions (e.g. "1\nSP 10:15-11:15am")
                has_combined = any(
                    re.match(r"^\d{1,2}\n", c.strip()) and len(c.strip().split("\n")) > 1
                    for c in sample_cells
                )

                # Check if all cells are just numbers (alternating row format)
                all_nums = all(
                    re.match(r"^\d{1,2}$", c.strip()) for c in sample_cells
                )

                # Check for Sports Complex format: () time Type
                has_parens = any("()" in (c or "") for c in sample_cells)

                if has_parens:
                    raw = parse_sports_complex_table(table, url)
                    # Need to resolve day numbers to actual dates
                    for s in raw:
                        try:
                            session_date = date(year, month, s["day_num"])
                            sessions.append({
                                "date": session_date.isoformat(),
                                "type": s["type"],
                                "start": s["start"],
                                "end": s["end"],
                            })
                        except ValueError:
                            pass
                elif all_nums:
                    default_type = "public_skate" if category == "public" else "stick_and_puck"
                    sessions.extend(parse_alternating_row_table(table, month, year))
                elif has_combined:
                    default_type = "public_skate" if category == "public" else "stick_and_puck"
                    sessions.extend(parse_combined_cell_table(table, month, year, default_type))

    return sessions


# ---------------------------------------------------------------------------
# Rink Scraping
# ---------------------------------------------------------------------------


def scrape_quickscores_rink(rink_id: str) -> list[dict]:
    """Scrape all PDFs for a QuickScores-based rink."""
    pdfs = discover_pdfs(rink_id)
    log(f"  Found {len(pdfs)} PDFs")

    all_sessions = []
    for pdf_info in pdfs:
        log(f"  Downloading: {pdf_info['label']} ({pdf_info['category']})")
        try:
            resp = fetch(pdf_info["url"], timeout=60)
            parsed = process_quickscores_pdf(resp.content, pdf_info["url"], pdf_info["category"])
            log(f"    Parsed {len(parsed)} sessions")
            all_sessions.extend(parsed)
        except Exception as e:
            log(f"    Error: {e}")

    return all_sessions


def scrape_cottonwood() -> list[dict]:
    """Scrape Cottonwood Heights schedules."""
    today = date.today()
    sessions = []

    # Stick & Puck from HTML
    log("  Scraping Stick & Puck (HTML)...")
    snp = scrape_cottonwood_stick_n_puck(today)
    log(f"    {len(snp)} sessions")
    sessions.extend(snp)

    # Public Skate from PDF linked on their page
    log("  Scraping Public Skate (PDF)...")
    try:
        resp = fetch(COTTONWOOD_PAGES["public_skate"])
        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" in href.lower():
                pdf_url = href if href.startswith("http") else "https://www.chparksandrecut.gov" + href
                log(f"    Downloading: {pdf_url}")
                try:
                    pdf_resp = fetch(pdf_url, timeout=60)
                    parsed = parse_cottonwood_public_pdf(pdf_resp.content, pdf_url)
                    log(f"    Parsed {len(parsed)} sessions")
                    sessions.extend(parsed)
                except Exception as e:
                    log(f"    Error: {e}")
    except Exception as e:
        log(f"  Warning: Failed to fetch Cottonwood public skate page: {e}")

    return sessions


def scrape_rink(rink_config: dict) -> dict:
    """Scrape a single rink and return its data."""
    rink_id = rink_config["id"]
    log(f"Scraping: {rink_config['name']}...")

    if rink_id == "cottonwood":
        sessions = scrape_cottonwood()
    else:
        sessions = scrape_quickscores_rink(rink_id)

    # Deduplicate and filter out impossible times (start >= end)
    seen = set()
    unique = []
    for s in sessions:
        if s["start"] >= s["end"]:
            continue  # Skip sessions where start is after end (PDF typos)
        key = (s["date"], s["type"], s["start"], s["end"])
        if key not in seen:
            seen.add(key)
            unique.append(s)

    unique.sort(key=lambda s: (s["date"], s["start"]))

    return {
        "id": rink_id,
        "name": rink_config["name"],
        "phone": rink_config["phone"],
        "address": rink_config["address"],
        "website": rink_config.get("website", ""),
        "lat": rink_config.get("lat"),
        "lng": rink_config.get("lng"),
        "sessions": unique,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    output_path = Path(__file__).parent.parent / "data" / "schedules.json"

    rinks_data = []
    for config in RINKS:
        rink_data = scrape_rink(config)
        rinks_data.append(rink_data)

    output = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "rinks": rinks_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    total = sum(len(r["sessions"]) for r in rinks_data)
    log(f"\nDone! {total} total sessions across {len(rinks_data)} rinks.")
    log(f"Output: {output_path}")


if __name__ == "__main__":
    main()
