import requests
import csv
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Tuple, Set

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

URLS = [
    # ATP
    ("ATP", "JPN", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=JPN"),
    ("ATP", "CHN", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=CHN"),
    ("ATP", "KOR", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=KOR"),
    ("ATP", "TPE", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=TPE"),
    ("ATP", "PRK", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=PRK"),
    ("ATP", "HKG", "https://www.atptour.com/en/rankings/singles?rankRange=1-3000&region=HKG"),

    # WTA
    ("WTA", "JPN", "https://www.wtatennis.com/rankings/singles?region=JPN"),
    ("WTA", "CHN", "https://www.wtatennis.com/rankings/singles?region=CHN"),
    ("WTA", "KOR", "https://www.wtatennis.com/rankings/singles?region=KOR"),
    ("WTA", "TPE", "https://www.wtatennis.com/rankings/singles?region=TPE"),
    ("WTA", "HKG", "https://www.wtatennis.com/rankings/singles?region=HKG"),
    ("WTA", "PRK", "https://www.wtatennis.com/rankings/singles?region=PRK"),
]


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def split_name(full_name: str) -> Tuple[str, str, str]:
    parts = full_name.split()
    if len(parts) == 1:
        return "", parts[0], ""
    first = parts[0]
    last = parts[-1]
    initial = first[0].upper() if first else ""
    return first, last, initial


def fetch_players_from_page(url: str) -> List[str]:
    print(f"Fetching: {url}")
    res = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    names = []

    # ATP e WTA hanno strutture leggermente diverse → usiamo entrambi i selettori
    selectors = [
        "td.player-cell a",        # ATP classico
        "td.name a",               # alternativa ATP
        "a.player-name",           # fallback
        "td:nth-child(2) a",       # fallback generico
    ]

    for sel in selectors:
        for a in soup.select(sel):
            text = normalize(a.get_text())
            if text and len(text.split()) >= 2:
                names.append(text)

    return list(set(names))


def main():
    seen: Set[Tuple[str, str]] = set()
    rows: List[Dict] = []

    for tour, country_code, url in URLS:
        players = fetch_players_from_page(url)

        for full_name in players:
            first, last, initial = split_name(full_name)

            key = (last.lower(), initial.lower())
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "tour": tour,
                "country_code": country_code,
                "first_name": first,
                "last_name": last,
                "first_initial": initial,
                "full_name": full_name,
                "invert_name": 1,
                "exception": 1 if full_name.lower() == "naomi osaka" else 0
            })

    # Scrittura CSV
    with open("asian_players.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tour",
            "country_code",
            "first_name",
            "last_name",
            "first_initial",
            "full_name",
            "invert_name",
            "exception"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Creato asian_players.csv con {len(rows)} giocatori")


if __name__ == "__main__":
    main()
