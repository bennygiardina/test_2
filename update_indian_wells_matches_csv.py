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
DEFAULT_RESULTS_PAGE = "https://www.atptour.com/en/scores/current/atp-masters-1000-indian-wells/404/results"
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
    line = line.strip()
    if not line:
        return None

    # Caso 1: formato normale "107 WC NAME ..."
    m = re.match(r"^(\d{1,3})\s+(.*)$", line)

    # Caso 2: formato compatto "107WC NAME ..." oppure "125Q NAME ..."
    if not m:
        m = re.match(r"^(\d{1,3})(WC|PR|Q|LL)\s+(.*)$", line)
        if m:
            position = int(m.group(1))
            if not (1 <= position <= 128):
                return None
            rest = f"{m.group(2)} {m.group(3)}".strip()
        else:
            return None
    else:
        position = int(m.group(1))
        if not (1 <= position <= 128):
            return None
        rest = m.group(2).strip()

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

        # gestisce "SINNER, Jannik" -> "SINNER Jannik"
        raw_name = raw_name.replace(",", "")

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

    # fallback robusto: se mancano poche posizioni, trattale come bye
    if 0 < len(missing) <= 4:
        for pos in missing:
            rows.append(
                {
                    "draw_position": pos,
                    "seed": "",
                    "entry_status": "",
                    "player_name": "bye",
                    "raw_name": "Bye",
                    "country": "",
                    "slot_type": "bye",
                }
            )

    rows.sort(key=lambda x: x["draw_position"])

    missing_after_fill = [i for i in range(1, 129) if i not in {r["draw_position"] for r in rows}]
    if missing_after_fill:
        raise RuntimeError(
            f"Attese 128 posizioni, trovate {len(rows)}. Posizioni mancanti: {missing_after_fill}"
        )

    return rows


def build_match_rows(positions: list[dict]) -> list[dict]:
    current: list[dict] = []
    for p in positions:
        current.append(
            {
                "name": p["player_name"] if p["player_name"] else "TBD",
                "slot_type": p["slot_type"],
            }
        )

    match_rows: list[dict] = []

    while len(current) > 1:
        round_size = len(current) // 2
        round_label = ROUND_LABELS[round_size]
        next_round: list[dict] = []

        for i in range(0, len(current), 2):
            a = current[i]
            b = current[i + 1]

            a_name = a["name"] if a["name"] else "TBD"
            b_name = b["name"] if b["name"] else "TBD"

            a_type = a.get("slot_type", "")
            b_type = b.get("slot_type", "")

            winner = ""
            next_name = "TBD"
            next_type = "unknown"

            if a_type == "bye" and b_name not in {"", "bye", "TBD"}:
                winner = b_name
                next_name = b_name
                next_type = "player"
            elif b_type == "bye" and a_name not in {"", "bye", "TBD"}:
                winner = a_name
                next_name = a_name
                next_type = "player"

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

            next_round.append(
                {
                    "name": next_name,
                    "slot_type": next_type,
                }
            )

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


def fetch_and_build_rows(draw_page_url: str, fallback_pdf_url: str, results_page_url: str) -> tuple[list[dict], dict]:
    pdf_url = discover_pdf_url(draw_page_url, fallback_pdf_url)
    pdf_resp = requests.get(pdf_url, timeout=60)
    pdf_resp.raise_for_status()
    pdf_bytes = pdf_resp.content

    pages_text = extract_pdf_text(pdf_bytes)
    released_at = extract_released_at(pages_text)

    positions = parse_draw_positions(pages_text)
    rows = build_match_rows(positions)

    results_html = fetch_results_page(results_page_url)
    completed_matches = parse_results_page(results_html)
    soup = BeautifulSoup(results_html, "html.parser")
    debug_text = unicodedata.normalize("NFKC", soup.get_text("\n", strip=True))
    debug_lines = [line.strip() for line in debug_text.splitlines() if line.strip()]

    print(f"[DEBUG] first Game Set lines:", flush=True)
    count = 0
    for line in debug_lines:
        if "Game Set and Match" in line or "Round of 16" in line or "Quarterfinals" in line or "Semifinals" in line or "Final -" in line:
            print(f"[DEBUG] {line}", flush=True)
            count += 1
            if count >= 20:
                break

    print(f"[DEBUG] completed_matches={len(completed_matches)}", flush=True)
    for m in completed_matches[:10]:
        print(f"[DEBUG] {m}", flush=True)

   
    rows = apply_results_to_match_rows(rows, completed_matches)
    rows = propagate_winners_through_bracket(rows)
    rows = apply_results_to_match_rows(rows, completed_matches)

    meta = {
        "source_draw_page": draw_page_url,
        "source_pdf": pdf_url,
        "source_results_page": results_page_url,
        "released_at": released_at,
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "completed_matches": len(completed_matches),
    }
    return rows, meta


def write_csv_if_changed(output_path: Path, data: bytes) -> bool:
    if output_path.exists() and output_path.read_bytes() == data:
        return False
    output_path.write_bytes(data)
    return True


def run_once(output_path: Path, draw_page_url: str, tournament_id: str, results_page_url: str) -> bool:
    year = datetime.now().year
    fallback_pdf_url = DEFAULT_FALLBACK_PDF.format(year=year, tournament_id=tournament_id)
    rows, meta = fetch_and_build_rows(draw_page_url, fallback_pdf_url, results_page_url)
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)

    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    print(
        f"[{utc_now_iso()}] {status} | file={output_path} | matches={meta['matches']} | "
        f"completed={meta['completed_matches']} | released_at={meta['released_at'] or 'n/d'} | "
        f"sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )
    return changed

def set_wins_from_scoreline(scoreline: str) -> tuple[int, int]:
    a_sets = 0
    b_sets = 0

    parts = [p.strip() for p in scoreline.split() if "-" in p]

    for part in parts:
        clean = re.sub(r"\(.*?\)", "", part)
        m = re.match(r"^(\d+)-(\d+)$", clean)
        if not m:
            continue

        a_games = int(m.group(1))
        b_games = int(m.group(2))

        if a_games > b_games:
            a_sets += 1
        elif b_games > a_games:
            b_sets += 1

    return a_sets, b_sets

def normalize_name_for_matching(name: str) -> str:
    n = (name or "").strip().lower()
    n = re.sub(r"\[[^\]]+\]", "", n)   # toglie [1], [Q], [WC], ecc.
    n = n.replace(".", "")
    n = n.replace(",", "")
    n = re.sub(r"\s+", " ", n).strip()

    if not n:
        return ""

    tokens = n.split()

    # Caso draw: "j sinner" oppure "g mpetshi perricard"
    if len(tokens) >= 2 and len(tokens[0]) == 1:
        initial = tokens[0]
        surname = " ".join(tokens[1:])
        return f"{surname} {initial}".strip()

    # Caso east-asian nel draw: "sakamoto r"
    if len(tokens) >= 2 and len(tokens[-1]) == 1:
        initial = tokens[-1]
        surname = " ".join(tokens[:-1])
        return f"{surname} {initial}".strip()

    # Caso results ATP: "jannik sinner" / "giovanni mpetshi perricard"
    initial = tokens[0][0]
    surname = " ".join(tokens[1:]) if len(tokens) > 1 else tokens[0]
    return f"{surname} {initial}".strip()

def fetch_results_page(results_page_url: str) -> str:
    resp = requests.get(results_page_url, timeout=30)
    resp.raise_for_status()
    return resp.text


def map_results_round_label(atp_round: str) -> str:
    mapping = {
        "Final": "Finale",
        "Semifinals": "Semifinali",
        "Quarterfinals": "Quarti di finale",
        "Round of 16": "4° turno",
        "Round of 32": "3° turno",
        "Round of 64": "2° turno",
        "Round of 128": "1° turno",
    }
    return mapping.get(atp_round.strip(), atp_round.strip())


import unicodedata

def parse_results_page(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    text = unicodedata.normalize("NFKC", text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    completed_matches: list[dict] = []

    round_map = {
        "Final": "Finale",
        "Semifinals": "Semifinali",
        "Quarterfinals": "Quarti di finale",
        "Round of 16": "4° turno",
        "Round of 32": "3° turno",
        "Round of 64": "2° turno",
        "Round of 128": "1° turno",
    }

    def detect_round(line: str) -> str:
        for key, value in round_map.items():
            if line.startswith(key):
                return value
        return ""

    def parse_player_line(line: str) -> tuple[str, str] | None:
        # formato ATP reale:
        #  (2)
        #  (Q)
        m = re.match(r"^【\d+†([^】]+)】(?:\s+\((\d+|Q|WC|LL|PR)\))?$", line)
        if m:
            return m.group(1).strip(), (m.group(2) or "").strip()

        # fallback plain text
        m = re.match(r"^([A-Z][A-Za-zÀ-ÿ'’.\-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'’.\-]+)+)(?:\s+\((\d+|Q|WC|LL|PR)\))?$", line)
        if m:
            return m.group(1).strip(), (m.group(2) or "").strip()

        return None

    current_round = ""

    for i, line in enumerate(lines):
        maybe_round = detect_round(line)
        if maybe_round:
            current_round = maybe_round
            continue

        if not current_round:
            continue

        # match conclusi con riga "Game Set and Match ..."
        m_score = re.match(
            r"^Game Set and Match ([^.]+)\.\s+.* wins the match\s+([0-9\-\(\)\s]+)\.\s*$",
            line
        )
        if not m_score:
            continue

        winner_name = m_score.group(1).strip()
        scoreline = re.sub(r"\s+", " ", m_score.group(2)).strip()
        a_sets, b_sets = set_wins_from_scoreline(scoreline)

        # cerca gli ultimi 2 giocatori validi prima della scoreline
        players_found: list[tuple[str, str]] = []
        for j in range(max(0, i - 25), i):
            parsed = parse_player_line(lines[j])
            if not parsed:
                continue
            if players_found and players_found[-1] == parsed:
                continue
            players_found.append(parsed)

        if len(players_found) < 2:
            continue

        player1, player1_tag = players_found[-2]
        player2, player2_tag = players_found[-1]

        completed_matches.append(
            {
                "round": current_round,
                "player_a": player1,
                "player_b": player2,
                "player_a_tag": player1_tag,
                "player_b_tag": player2_tag,
                "winner": winner_name,
                "a_sets": a_sets,
                "b_sets": b_sets,
            }
        )

    return completed_matches

def apply_results_to_match_rows(match_rows: list[dict], completed_matches: list[dict]) -> list[dict]:
    for row in match_rows:
        row_a = normalize_name_for_matching(row["Player A"])
        row_b = normalize_name_for_matching(row["Player B"])
        row_round = row["Round"]

        for result in completed_matches:
            res_a = normalize_name_for_matching(result["player_a"])
            res_b = normalize_name_for_matching(result["player_b"])
            res_w = normalize_name_for_matching(result["winner"])
            res_round = result["round"]

            if row_round != res_round:
                continue

            if {row_a, row_b} != {res_a, res_b}:
                continue

            # orientamento score
            if row_a == res_a and row_b == res_b:
                row["Participant A score"] = result["a_sets"]
                row["Participant B score"] = result["b_sets"]
            else:
                row["Participant A score"] = result["b_sets"]
                row["Participant B score"] = result["a_sets"]

            # winner = usa il nome già presente nel draw/csv
            if row_a == res_w:
                row["Winner"] = row["Player A"]
            elif row_b == res_w:
                row["Winner"] = row["Player B"]

            break

    return match_rows

def propagate_winners_through_bracket(match_rows: list[dict]) -> list[dict]:
    round_order = [
        "1° turno",
        "2° turno",
        "3° turno",
        "4° turno",
        "Quarti di finale",
        "Semifinali",
        "Finale",
    ]

    rows_by_round: dict[str, list[dict]] = {rnd: [] for rnd in round_order}
    for row in match_rows:
        rows_by_round[row["Round"]].append(row)

    for rnd in round_order:
        if rnd not in rows_by_round:
            continue

    for i in range(len(round_order) - 1):
        current_round = round_order[i]
        next_round = round_order[i + 1]

        current_rows = rows_by_round.get(current_round, [])
        next_rows = rows_by_round.get(next_round, [])

        if not current_rows or not next_rows:
            continue

        winners = []
        for row in current_rows:
            winner = (row.get("Winner") or "").strip()

            # se non c'è un winner esplicito ma c'è un bye vero, fallo avanzare
            if not winner:
                a = (row.get("Player A") or "").strip()
                b = (row.get("Player B") or "").strip()

                if a == "bye" and b not in {"", "bye", "TBD"}:
                    winner = b
                elif b == "bye" and a not in {"", "bye", "TBD"}:
                    winner = a

            winners.append(winner if winner else "TBD")

        # assegna i vincitori ai match del round successivo
        for j, next_row in enumerate(next_rows):
            a_idx = j * 2
            b_idx = j * 2 + 1

            if a_idx < len(winners):
                next_row["Player A"] = winners[a_idx]
            if b_idx < len(winners):
                next_row["Player B"] = winners[b_idx]

            # reset winner/score del round successivo, poi verranno eventualmente riempiti
            if not next_row.get("Winner"):
                next_row["Winner"] = ""
            if next_row.get("Participant A score") in (None,):
                next_row["Participant A score"] = ""
            if next_row.get("Participant B score") in (None,):
                next_row["Participant B score"] = ""

    rebuilt_rows: list[dict] = []
    for rnd in round_order:
        rebuilt_rows.extend(rows_by_round.get(rnd, []))

    return rebuilt_rows

def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match dal draw ATP ufficiale.")
    parser.add_argument("--output", default="indian_wells_matches_format.csv", help="Percorso del file CSV da creare/aggiornare")
    parser.add_argument("--draw-page", default=DEFAULT_DRAW_PAGE, help="URL della pagina ATP del draw")
    parser.add_argument("--tournament-id", default=DEFAULT_TOURNAMENT_ID, help="ID torneo ATP, usato per il PDF fallback")
    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    parser.add_argument("--results-page", default=DEFAULT_RESULTS_PAGE, help="URL della pagina ATP Results")
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(output_path, args.draw_page, args.tournament_id, args.results_page)
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
