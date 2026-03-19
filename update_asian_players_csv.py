import csv
import io
import re
from typing import Dict, List, Set, Tuple

import pdfplumber
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

ATP_URLS = [
    ("ATP", "JPN", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=JPN"),
    ("ATP", "CHN", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=CHN"),
    ("ATP", "KOR", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=KOR"),
    ("ATP", "TPE", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=TPE"),
    ("ATP", "PRK", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=PRK"),
]

WTA_COUNTRIES = {"JPN", "CHN", "KOR", "TPE", "PRK"}
WTA_PDF_URL = "https://wtafiles.wtatennis.com/pdf/rankings/Singles_Numeric.pdf"


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def split_name(full_name: str) -> Tuple[str, str, str]:
    parts = normalize(full_name).split()
    if not parts:
        return "", "", ""
    if len(parts) == 1:
        return "", parts[0], ""
    first_name = parts[0]
    last_name = parts[-1]
    first_initial = first_name[0].upper()
    return first_name, last_name, first_initial


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def extract_atp_player_names_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    names: Set[str] = set()

    selectors = [
        "td.player-cell a",
        "td.name a",
        "a.player-name",
        "td:nth-child(2) a",
    ]

    for selector in selectors:
        for a_tag in soup.select(selector):
            text = normalize(a_tag.get_text(" ", strip=True))
            if len(text.split()) >= 2:
                names.add(text)

    return sorted(names)


def fetch_wta_pdf_text() -> str:
    response = requests.get(WTA_PDF_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()

    full_text: List[str] = []
    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text:
                full_text.append(page_text)
    return "\n".join(full_text)


def extract_wta_players_from_pdf_text(pdf_text: str) -> List[Dict[str, str]]:
    """
    Estrae righe tipo:
    1200 (1203) ETO, NAOKO JPN 10 7 1
    1201 (1205) LIU, MIN CHN 10 9 1

    e produce:
    {
      "tour": "WTA",
      "country_code": "JPN",
      "first_name": "Naoko",
      "last_name": "Eto",
      "first_initial": "N",
      "full_name": "Naoko Eto",
      ...
    }
    """
    rows: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()

    # pattern abbastanza tollerante:
    # rank, poi (prev), poi NOME in maiuscolo "COGNOME, NOME/I", poi country code a 3 lettere
    pattern = re.compile(
        r"(?m)^\s*\d+(?:\s*\(\d+\))?\s+([A-Z][A-Z'`\-\. ]+,\s*[A-Z][A-Z'`\-\. ]+)\s+([A-Z]{3})\b"
    )

    for raw_name, country_code in pattern.findall(pdf_text):
        country_code = country_code.strip().upper()
        if country_code not in WTA_COUNTRIES:
            continue

        raw_name = normalize(raw_name)

        if "," not in raw_name:
            continue

        last_raw, first_raw = [normalize(x) for x in raw_name.split(",", 1)]
        if not last_raw or not first_raw:
            continue

        # title case semplice, mantenendo trattini/apostrofi ragionevolmente leggibili
        def smart_title(s: str) -> str:
            return " ".join(part.capitalize() for part in s.split())

        first_name = smart_title(first_raw)
        last_name = smart_title(last_raw)
        first_initial = first_name[0].upper()
        full_name = f"{first_name} {last_name}"

        key = ("WTA", last_name.lower(), first_initial.lower())
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "tour": "WTA",
            "country_code": country_code,
            "first_name": first_name,
            "last_name": last_name,
            "first_initial": first_initial,
            "full_name": full_name,
            "invert_name": 1,
            "exception": 1 if full_name.lower() == "naomi osaka" else 0,
        })

    return rows


def build_atp_rows() -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for tour, country_code, url in ATP_URLS:
        print(f"Fetching ATP: {url}")
        html = fetch_html(url)
        players = extract_atp_player_names_from_html(html)

        for full_name in players:
            first_name, last_name, first_initial = split_name(full_name)
            if not last_name or not first_initial:
                continue

            key = (tour, last_name.lower(), first_initial.lower())
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "tour": tour,
                "country_code": country_code,
                "first_name": first_name,
                "last_name": last_name,
                "first_initial": first_initial,
                "full_name": full_name,
                "invert_name": 1,
                "exception": 1 if full_name.lower() == "naomi osaka" else 0,
            })

    return rows


def write_csv(rows: List[Dict[str, str]], output_path: str = "asian_players.csv") -> None:
    fieldnames = [
        "tour",
        "country_code",
        "first_name",
        "last_name",
        "first_initial",
        "full_name",
        "invert_name",
        "exception",
    ]

    rows.sort(key=lambda row: (
        row["tour"],
        row["country_code"],
        row["last_name"].lower(),
        row["first_name"].lower(),
    ))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    atp_rows = build_atp_rows()

    print("Fetching WTA PDF rankings...")
    wta_pdf_text = fetch_wta_pdf_text()
    wta_rows = extract_wta_players_from_pdf_text(wta_pdf_text)

    all_rows = atp_rows + wta_rows
    write_csv(all_rows)

    print(f"Creato asian_players.csv con {len(all_rows)} righe "
          f"({len(atp_rows)} ATP, {len(wta_rows)} WTA).")


if __name__ == "__main__":
    main()
