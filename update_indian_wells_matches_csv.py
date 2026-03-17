#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

DEFAULT_DRAW_PAGE = "https://www.atptour.com/en/scores/current/indian-wells/404/draws"
DEFAULT_FALLBACK_PDF = "https://www.protennislive.com/posting/{year}/{tournament_id}/mds.pdf"
DEFAULT_TOURNAMENT_ID = "404"
LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das", "van", "von", "der", "den", "la", "le"
}
EAST_ASIAN_COUNTRIES = {"CHN", "JPN", "KOR"}
ROUND_LABELS = {
    64: "1° turno",
    32: "2° turno",
    16: "3° turno",
    8: "4° turno",
    4: "Quarti di finale",
    2: "Semifinali",
    1: "Finale",
}
EAST_ASIAN_SURNAME_FIRST_COUNTRIES = {"JPN", "CHN", "KOR"}

NO_INVERSION_EXCEPTIONS_SURNAME_FIRST = {
    "OSAKA Naomi",
}
STATUS_LABELS = {
    "WC": "[WC]",
    "Q": "[Q]",
    "LL": "[LL]",
    "PR": "[PR]",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def smart_name(part: str) -> str:
    words = part.title().split()
    return " ".join(w.lower() if w.lower() in LOWERCASE_PARTICLES else w for w in words)


def initials_from_given_names(given_names: str) -> str:
    parts = [p for p in re.split(r"[\s-]+", given_names.strip()) if p]
    return " ".join(f"{smart_name(p)[0]}." for p in parts if smart_name(p))


def append_tags(base: str, seed: str = "", entry_status: str = "") -> str:
    out = base.strip()
    if seed:
        out += f" [{seed}]"
    if entry_status in STATUS_LABELS:
        out += f" {STATUS_LABELS[entry_status]}"
    return out.strip()


def format_player_display(last: str, first: str, seed: str = "", entry_status: str = "", country: str = "") -> str:
    last_fmt = smart_name(last)
    first_fmt = smart_name(first)
    initials = initials_from_given_names(first_fmt)
    if country in EAST_ASIAN_COUNTRIES:
        base = f"{last_fmt} {initials}".strip()
    else:
        base = f"{initials} {last_fmt}".strip()
    return append_tags(base, seed=seed, entry_status=entry_status)

def smart_title_token(token: str) -> str:
    token = token.strip()
    if not token:
        return token

    if "-" in token:
        return "-".join(smart_title_token(part) for part in token.split("-"))

    if "'" in token:
        return "'".join(smart_title_token(part) for part in token.split("'"))

    return token[:1].upper() + token[1:].lower()

def format_name(raw_name: str, seed: str = "", entry_status: str = "", country: str = ""):
    raw_name = (raw_name or "").replace(",", "").strip()
    country = (country or "").strip().upper()

    if not raw_name:
        return ""

    if raw_name == "Bye":
        return "bye"
    if raw_name == "Qualifier / Lucky Loser":
        return "[Q/LL]"
    if raw_name == "Qualifier":
        return "[Q]"
    if raw_name == "TBA":
        return "TBA"

    tokens = raw_name.split()

    if len(tokens) == 1:
        base_name = tokens[0].capitalize()
    else:
        surname_tokens = []
        given_tokens = []

        for i, tok in enumerate(tokens):
            if tok.isupper():
                surname_tokens.append(tok)
            else:
                given_tokens = tokens[i:]
                break

        if not surname_tokens or not given_tokens:
            surname_tokens = tokens[:-1]
            given_tokens = [tokens[-1]]

        surname = " ".join(tok.capitalize() for tok in surname_tokens)
        given_name = " ".join(tok.capitalize() for tok in given_tokens)

        first_initial = f"{given_name[0].upper()}." if given_name else ""
        full_name = f"{' '.join(surname_tokens)} {given_name}".strip()

        if full_name == "OSAKA Naomi":
            base_name = f"{first_initial} {surname}"
        elif country in {"JPN", "CHN", "KOR"}:
            base_name = f"{surname} {first_initial}"
        else:
            base_name = f"{first_initial} {surname}"

    extras = []

    if seed:
        extras.append(f"[{seed}]")

    if entry_status in {"WC", "Q", "LL", "PR"}:
        extras.append(f"[{entry_status}]")

    if extras:
        return f"{base_name} {' '.join(extras)}"

    return base_name

def is_tournament_metadata(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()

    month_words = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]

    if any(month in t for month in month_words):
        return True

    if "usd" in t:
        return True

    if "hard" in t or "clay" in t or "grass" in t:
        return True

    if "|" in t:
        return True

    return False

def discover_pdf_url(draw_page_url: str, fallback_pdf_url: str) -> str:
    resp = requests.get(draw_page_url, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "protennislive.com" in href and href.lower().endswith("mds.pdf"):
            return href
    return fallback_pdf_url


def extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return [(page.extract_text() or "") for page in reader.pages]


def clean_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line and line.strip()]


def extract_released_at(pages_text: Iterable[str]) -> str:
    for page_text in pages_text:
        lines = clean_lines(page_text)
        for i, line in enumerate(lines):
            if line == "Released" and i + 1 < len(lines):
                return lines[i + 1]
    return ""

def is_fake_player_line(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()

    if "usd" in t:
        return True
    if "hard" in t or "clay" in t or "grass" in t:
        return True
    if "|" in t:
        return True
    if "march" in t:
        return True

    return False

def parse_draw_line(line: str) -> dict | None:
    m = re.match(r"^(\d{1,3})\s+(.*)$", line)
    if not m:
        return None

    position = int(m.group(1))
    if not (1 <= position <= 128):
        return None

    rest = m.group(2).strip()

    # intercetta metadata (tipo "March — 29 March 2026 | USD ...")
    if is_tournament_metadata(rest):
        return {
            "draw_position": position,
            "seed": "",
            "entry_status": "",
            "player_name": "bye",
            "raw_name": "Bye",
            "country": "",
            "slot_type": "bye",
        }

    entry_status = ""
    seed = ""
    country = ""
    slot_type = "player"
    raw_name = ""
    display_name = ""

    if rest == "Bye":
        raw_name = rest
        display_name = "bye"
        slot_type = "bye"

    elif rest.strip() == "Qualifier / Lucky Loser":
        raw_name = rest
        display_name = "[Q/LL]"
        slot_type = "qualifier_or_lucky_loser"

    elif rest.strip() == "Qualifier":
        raw_name = rest
        display_name = "[Q]"
        slot_type = "qualifier"

    else:
        tokens = rest.split()

        if tokens and tokens[0] in {"WC", "PR", "Q", "LL"}:
            entry_status = tokens.pop(0)

        if tokens and re.fullmatch(r"\d{1,2}", tokens[0]):
            seed = tokens.pop(0)

        if tokens and re.fullmatch(r"[A-Z]{3}", tokens[-1]):
            country = tokens.pop()

        raw_name = " ".join(tokens)

        display_name = format_name(
            raw_name,
            seed=seed,
            entry_status=entry_status,
            country=country
        )

    return {
        "draw_position": position,
        "seed": seed,
        "entry_status": entry_status,
        "player_name": display_name,
        "raw_name": raw_name,
        "country": country,
        "slot_type": slot_type,
    }

    # FIX per il tuo problema specifico
    if is_tournament_metadata(display_name):
        display_name = "bye"
        slot_type = "bye"

def extract_draw_block_lines(page_text: str) -> list[str]:
    lines = clean_lines(page_text)

    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if "Main Draw Singles" in line:
            start_idx = i + 1
            break

    if start_idx is None:
        return []

    for i in range(start_idx, len(lines)):
        if line_starts_round_header(lines[i]):
            end_idx = i
            break

    if end_idx is None:
        end_idx = len(lines)

    return lines[start_idx:end_idx]


def line_starts_round_header(line: str) -> bool:
    return line.strip().startswith("Round of 128")

def split_combined_draw_line(line: str) -> list[str]:
    line = line.strip()
    if not line:
        return []

    # cerca tutti gli inizi plausibili di una posizione draw:
    # numero 1-128 seguito da spazio e testo tipo Bye / Qualifier / nome giocatore
    starts = list(
        re.finditer(r"(?<!\S)(\d{1,3})(?=\s+(?:Bye|Qualifier|[A-Z]))", line)
    )

    if len(starts) <= 1:
        return [line]

    chunks = []
    for i, match in enumerate(starts):
        start = match.start()
        end = starts[i + 1].start() if i + 1 < len(starts) else len(line)
        chunk = line[start:end].strip()
        if chunk:
            chunks.append(chunk)

    return chunks
    
def parse_draw_positions(pages_text: list[str]) -> list[dict]:
    rows: list[dict] = []
    seen_positions: set[int] = set()

    for page_text in pages_text:
        lines = extract_draw_block_lines(page_text)

        for line in lines:
            candidate_lines = split_combined_draw_line(line)

            for candidate in candidate_lines:
                parsed = parse_draw_line(candidate)
                if not parsed:
                    continue

                pos = parsed["draw_position"]
                if pos in seen_positions:
                    continue

                seen_positions.add(pos)
                rows.append(parsed)

    rows.sort(key=lambda x: x["draw_position"])

    missing = [i for i in range(1, 129) if i not in seen_positions]
    if missing:
        raise RuntimeError(
            f"Attese 128 posizioni, trovate {len(rows)}. Posizioni mancanti: {missing}"
        )

    return rows


def build_match_rows(positions: list[dict]) -> list[dict]:
    current: list[dict] = []
    for p in positions:
        current.append({"name": p["player_name"], "slot_type": p["slot_type"]})

    match_rows: list[dict] = []
    while len(current) > 1:
        round_size = len(current) // 2
        round_label = ROUND_LABELS[round_size]
        next_round: list[dict] = []

        for i in range(0, len(current), 2):
            a = current[i]
            b = current[i + 1]
            a_name = a["name"]
            b_name = b["name"]
            winner = ""

            if a_name == "bye" and b_name and b_name != "bye":
                winner = b_name
            elif b_name == "bye" and a_name and a_name != "bye":
                winner = a_name
            elif a_name == "bye" and b_name == "bye":
                winner = ""

            match_rows.append(
                {
                    "Round": round_label,
                    "Player A": a_name,
                    "Player B": b_name,
                    "Winner": winner,
                    "Participant A score": "",
                    "Participant B score": "",
                }
            )
            next_round.append({"name": winner, "slot_type": "player" if winner else "unknown"})

        current = next_round

    return match_rows


def csv_bytes(rows: list[dict]) -> bytes:
    fieldnames = [
        "Round",
        "Player A",
        "Player B",
        "Winner",
        "Participant A score",
        "Participant B score",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch_and_build_rows(draw_page_url: str, fallback_pdf_url: str) -> tuple[list[dict], dict]:
    pdf_url = discover_pdf_url(draw_page_url, fallback_pdf_url)
    pdf_resp = requests.get(pdf_url, timeout=60)
    pdf_resp.raise_for_status()
    pdf_bytes = pdf_resp.content
    pages_text = extract_pdf_text(pdf_bytes)
    released_at = extract_released_at(pages_text)
    positions = parse_draw_positions(pages_text)
    rows = build_match_rows(positions)
    meta = {
        "source_draw_page": draw_page_url,
        "source_pdf": pdf_url,
        "released_at": released_at,
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
    }
    return rows, meta


def write_csv_if_changed(output_path: Path, data: bytes) -> bool:
    if output_path.exists() and output_path.read_bytes() == data:
        return False
    output_path.write_bytes(data)
    return True


def run_once(output_path: Path, draw_page_url: str, tournament_id: str) -> bool:
    year = datetime.now().year
    fallback_pdf_url = DEFAULT_FALLBACK_PDF.format(year=year, tournament_id=tournament_id)
    rows, meta = fetch_and_build_rows(draw_page_url, fallback_pdf_url)
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)

    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    print(
        f"[{utc_now_iso()}] {status} | file={output_path} | matches={meta['matches']} | "
        f"released_at={meta['released_at'] or 'n/d'} | sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )
    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match dal draw ATP ufficiale.")
    parser.add_argument("--output", default="indian_wells_matches_format.csv", help="Percorso del file CSV da creare/aggiornare")
    parser.add_argument("--draw-page", default=DEFAULT_DRAW_PAGE, help="URL della pagina ATP del draw")
    parser.add_argument("--tournament-id", default=DEFAULT_TOURNAMENT_ID, help="ID torneo ATP, usato per il PDF fallback")
    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(output_path, args.draw_page, args.tournament_id)
        return 0

    while True:
        try:
            run_once(output_path, args.draw_page, args.tournament_id)
        except KeyboardInterrupt:
            return 130
        except Exception as exc:  # noqa: BLE001
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
