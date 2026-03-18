#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

DEFAULT_TOURNAMENT_URL = "https://www.atptour.com/en/scores/current-challenger/asuncion/2909"
DEFAULT_TOURNAMENT_ID = "2909"

DEFAULT_DRAW_PAGE = ""
DEFAULT_RESULTS_PAGE = ""
DEFAULT_FALLBACK_PDF = "https://www.protennislive.com/posting/{year}/{tournament_id}/mds.pdf"

STATUS_LABELS = {
    "WC": "[WC]",
    "Q": "[Q]",
    "LL": "[LL]",
    "PR": "[PR]",
    "ALT": "[Alt]",
}

ROUND_HEADER_TO_LABEL = {
    "Round of 128": "1° turno",
    "Round of 96": "1° turno",
    "Round of 64": "1° turno",
    "Round of 48": "1° turno",
    "Round of 32": "1° turno",
    "Round of 24": "1° turno",
    "Round of 16": "Ottavi di finale",
    "Quarterfinals": "Quarti di finale",
    "Semifinals": "Semifinali",
    "Final": "Finale",
}

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le"
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def infer_draw_page(url: str) -> str:
    url = normalize_url(url)
    if url.endswith("/draws"):
        return url
    if url.endswith("/results"):
        return url.replace("/results", "/draws")
    return url + "/draws"


def infer_results_page(url: str) -> str:
    url = normalize_url(url)
    if url.endswith("/results"):
        return url
    if url.endswith("/draws"):
        return url.replace("/draws", "/results")
    return url + "/results"


def infer_tournament_id(url: str) -> str:
    matches = re.findall(r"/(\d+)(?:/|$)", url)
    return matches[-1] if matches else ""


def smart_name(part: str) -> str:
    words = part.title().split()
    return " ".join(w.lower() if w.lower() in LOWERCASE_PARTICLES else w for w in words)


def smart_title_token(token: str) -> str:
    token = token.strip()
    if not token:
        return token
    if "-" in token:
        return "-".join(smart_title_token(part) for part in token.split("-"))
    if "'" in token:
        return "'".join(smart_title_token(part) for part in token.split("'"))
    lower = token.lower()
    if lower in LOWERCASE_PARTICLES:
        return lower
    return token[:1].upper() + token[1:].lower()


def smart_join_tokens(tokens: list[str]) -> str:
    return " ".join(smart_title_token(tok) for tok in tokens if tok)


def get_round_label(round_size: int, initial_draw_size: int) -> str:
    if round_size == 1:
        return "Finale"
    if round_size == 2:
        return "Semifinali"
    if round_size == 4:
        return "Quarti di finale"
    if round_size == 8:
        return "Ottavi di finale"

    known = {
        128: {64: "1° turno", 32: "2° turno", 16: "3° turno"},
        96: {48: "1° turno", 32: "2° turno", 16: "3° turno"},
        64: {32: "1° turno", 16: "2° turno"},
        56: {32: "1° turno", 16: "2° turno"},
        48: {24: "1° turno", 16: "2° turno"},
        32: {16: "1° turno"},
        28: {16: "1° turno"},
        24: {12: "1° turno"},
        16: {8: "1° turno"},
    }
    return known.get(initial_draw_size, {}).get(round_size, f"Round {round_size}")


def map_atp_round_to_label(round_text: str) -> str | None:
    return ROUND_HEADER_TO_LABEL.get((round_text or "").strip())


def format_name(raw_name: str, seed: str = "", entry_status: str = "", country: str = "") -> str:
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
        base_name = smart_title_token(tokens[0])
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

        surname = smart_join_tokens(surname_tokens)
        given_name = smart_join_tokens(given_tokens)
        first_initial = f"{given_name[0].upper()}." if given_name else ""
        base_name = f"{first_initial} {surname}".strip()

    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])

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


def is_score_line(text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    return bool(re.fullmatch(r"\d{1,2}\s+\d{1,2}(?:\s+\d{1,2})*", text))


def normalize_person_name_for_matching(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def surname_from_name(name: str) -> str:
    n = normalize_person_name_for_matching(name)
    parts = n.split()
    return parts[-1] if parts else ""


def first_initial_from_name(name: str) -> str:
    n = normalize_person_name_for_matching(name)
    parts = n.split()
    if len(parts) >= 2 and parts[0]:
        return parts[0][0]
    return ""


def parse_draw_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None

    if is_score_line(line):
        return None

    m = re.match(r"^(\d{1,3})\s+(.*)$", line)
    if not m:
        m = re.match(r"^(\d{1,3})(WC|PR|Q|LL|ALT|Alt|alt)\s+(.*)$", line)
        if m:
            position = int(m.group(1))
            if position < 1:
                return None
            rest = f"{m.group(2)} {m.group(3)}".strip()
        else:
            return None
    else:
        position = int(m.group(1))
        if position < 1:
            return None
        rest = m.group(2).strip()

    if position > 128:
        return None

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

        if tokens:
            cleaned = tokens[0].replace(".", "").upper()
            if cleaned in STATUS_LABELS:
                entry_status = cleaned
                tokens.pop(0)

        if tokens and re.fullmatch(r"\d{1,2}", tokens[0]):
            seed = tokens.pop(0)

        if tokens and re.fullmatch(r"[A-Z]{3}", tokens[-1]):
            country = tokens.pop()

        raw_name = " ".join(tokens).replace(",", "").strip()
        if not raw_name:
            return None

        display_name = format_name(
            raw_name,
            seed=seed,
            entry_status=entry_status,
            country=country,
        )

        if is_tournament_metadata(display_name):
            display_name = "bye"
            slot_type = "bye"

    return {
        "draw_position": position,
        "seed": seed,
        "entry_status": entry_status,
        "player_name": display_name,
        "raw_name": raw_name,
        "country": country,
        "slot_type": slot_type,
    }


def parse_draw_positions(pages_text: list[str]) -> list[dict]:
    rows: list[dict] = []
    seen_positions: set[int] = set()
    in_main_draw = False

    stop_markers = {
        "Round of 32",
        "Round of 16",
        "Quarterfinals",
        "Semifinals",
        "Final",
        "Winner",
        "Last Direct Acceptance",
        "ATP Supervisor",
        "Released",
        "Seeded Players",
        "Alternates/Lucky Losers",
        "Withdrawals",
        "Retirements/W.O.",
    }

    for page_text in pages_text:
        lines = clean_lines(page_text)

        for line in lines:
            normalized = line.strip()

            if normalized == "Main Draw Singles":
                in_main_draw = True
                continue

            if not in_main_draw:
                continue

            if normalized in stop_markers:
                in_main_draw = False
                break

            parsed = parse_draw_line(normalized)
            if not parsed:
                continue

            pos = parsed["draw_position"]
            if pos in seen_positions:
                continue

            seen_positions.add(pos)
            rows.append(parsed)

    rows.sort(key=lambda x: x["draw_position"])

    draw_size = len(rows)
    if draw_size not in {16, 24, 28, 32, 48, 56, 64, 96, 128}:
        raise RuntimeError(f"Draw non supportato: {draw_size} posizioni")

    return rows


def sets_from_score(score_raw: str) -> tuple[int, int] | None:
    score_raw = (score_raw or "").strip()
    if not score_raw:
        return None

    tokens = re.findall(r"\d-\d(?:\(\d+\))?|\d{2}(?:\(\d+\))?", score_raw)
    if not tokens:
        return None

    w_sets = 0
    l_sets = 0

    for tok in tokens:
        tok_clean = tok.replace("-", "")
        m = re.match(r"(\d)(\d)", tok_clean)
        if not m:
            continue
        a, b = int(m.group(1)), int(m.group(2))
        if a > b:
            w_sets += 1
        elif b > a:
            l_sets += 1

    if w_sets == 0 and l_sets == 0:
        return None

    return w_sets, l_sets


def extract_json_candidates_from_html(html: str) -> list[str]:
    candidates: list[str] = []

    patterns = [
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
        r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
        r'window\.__DATA__\s*=\s*({.*?});',
    ]

    for pattern in patterns:
        for m in re.finditer(pattern, html, flags=re.DOTALL | re.IGNORECASE):
            candidates.append(m.group(1))

    return candidates


def _walk_json(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_json(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)


def _first_str(obj: dict, keys: tuple[str, ...]) -> str:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def extract_results_from_internal_json(html: str) -> list[dict]:
    results: list[dict] = []

    for candidate in extract_json_candidates_from_html(html):
        try:
            data = json.loads(candidate)
        except Exception:
            continue

        for obj in _walk_json(data):
            if not isinstance(obj, dict):
                continue

            round_text = _first_str(obj, ("round", "roundName", "Round", "matchRound"))
            winner_name = _first_str(obj, ("winnerName", "WinnerName", "winningPlayerName", "winner"))
            score_raw = _first_str(obj, ("score", "Score", "matchScore", "result"))
            player1 = _first_str(obj, ("player1Name", "Player1Name", "homePlayerName"))
            player2 = _first_str(obj, ("player2Name", "Player2Name", "awayPlayerName"))

            label = map_atp_round_to_label(round_text)
            if not label:
                continue

            if not winner_name and not score_raw and not (player1 and player2):
                continue

            results.append({
                "round": label,
                "winner_name_raw": winner_name,
                "score_raw": score_raw,
                "player1_name_raw": player1,
                "player2_name_raw": player2,
                "source": "json",
            })

    return dedupe_results(results)


def dedupe_results(results: list[dict]) -> list[dict]:
    seen = set()
    out = []

    for r in results:
        key = (
            r.get("round", ""),
            normalize_person_name_for_matching(r.get("winner_name_raw", "")),
            re.sub(r"\s+", " ", (r.get("score_raw", "") or "").strip()),
            normalize_person_name_for_matching(r.get("player1_name_raw", "")),
            normalize_person_name_for_matching(r.get("player2_name_raw", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


def extract_results_from_html_text(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    pattern = re.compile(
        r"(Round of 128|Round of 96|Round of 64|Round of 48|Round of 32|Round of 24|Round of 16|Quarterfinals|Semifinals|Final)"
        r".*?Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+"
        r"(?:\2)\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        re.IGNORECASE,
    )

    results = []
    for m in pattern.finditer(text):
        round_text = m.group(1).strip()
        winner_name = " ".join(m.group(2).split())
        score_raw = " ".join(m.group(3).split())

        label = map_atp_round_to_label(round_text)
        if not label:
            continue

        results.append({
            "round": label,
            "winner_name_raw": winner_name,
            "score_raw": score_raw,
            "player1_name_raw": "",
            "player2_name_raw": "",
            "source": "html",
        })

    return dedupe_results(results)


def fetch_results_page(results_page_url: str) -> list[dict]:
    resp = requests.get(results_page_url, timeout=30)
    resp.raise_for_status()
    html = resp.text

    json_results = extract_results_from_internal_json(html)
    if json_results:
        return json_results

    return extract_results_from_html_text(html)


def group_results_by_round(results: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for r in results:
        grouped.setdefault(r["round"], []).append(r)
    return grouped


def resolve_winner_from_results_page(player_a: str, player_b: str, winner_full_name: str) -> str:
    a_norm = normalize_person_name_for_matching(player_a)
    b_norm = normalize_person_name_for_matching(player_b)
    w_norm = normalize_person_name_for_matching(winner_full_name)

    if a_norm and a_norm == w_norm:
        return player_a
    if b_norm and b_norm == w_norm:
        return player_b

    a_surname = surname_from_name(player_a)
    b_surname = surname_from_name(player_b)
    w_surname = surname_from_name(winner_full_name)

    if w_surname == a_surname and w_surname != b_surname:
        return player_a
    if w_surname == b_surname and w_surname != a_surname:
        return player_b

    a_initial = first_initial_from_name(player_a)
    b_initial = first_initial_from_name(player_b)
    w_initial = first_initial_from_name(winner_full_name)

    if w_surname == a_surname and w_initial and w_initial == a_initial and not (w_surname == b_surname and w_initial == b_initial):
        return player_a
    if w_surname == b_surname and w_initial and w_initial == b_initial and not (w_surname == a_surname and w_initial == a_initial):
        return player_b

    return ""


def match_result_to_players(player_a: str, player_b: str, res: dict) -> bool:
    p1 = normalize_person_name_for_matching(res.get("player1_name_raw", ""))
    p2 = normalize_person_name_for_matching(res.get("player2_name_raw", ""))
    a = normalize_person_name_for_matching(player_a)
    b = normalize_person_name_for_matching(player_b)

    if p1 and p2:
        return {a, b} == {p1, p2}

    winner = res.get("winner_name_raw", "")
    return bool(resolve_winner_from_results_page(player_a, player_b, winner))


def build_match_rows_from_results_page(positions: list[dict], round_results: dict[str, list[dict]]) -> list[dict]:
    current = [{"name": p["player_name"], "slot_type": p["slot_type"]} for p in positions]
    match_rows: list[dict] = []
    initial_size = len(current)

    while len(current) > 1:
        round_size = len(current) // 2
        round_label = get_round_label(round_size, initial_size)
        next_round = []

        results_for_round = round_results.get(round_label, [])
        used = [False] * len(results_for_round)

        for i in range(0, len(current), 2):
            a_name = current[i]["name"]
            b_name = current[i + 1]["name"]

            winner = ""
            a_sets = ""
            b_sets = ""

            if a_name == "bye" and b_name and b_name != "bye":
                winner = b_name
            elif b_name == "bye" and a_name and a_name != "bye":
                winner = a_name
            elif a_name == "bye" and b_name == "bye":
                winner = ""

            if not winner:
                for idx, res in enumerate(results_for_round):
                    if used[idx]:
                        continue
                    if not match_result_to_players(a_name, b_name, res):
                        continue

                    resolved = resolve_winner_from_results_page(
                        a_name, b_name, res.get("winner_name_raw", "")
                    )
                    if not resolved:
                        continue

                    used[idx] = True
                    winner = resolved

                    sets = sets_from_score(res.get("score_raw", ""))
                    if sets:
                        w_sets, l_sets = sets
                        if winner == a_name:
                            a_sets, b_sets = str(w_sets), str(l_sets)
                        else:
                            a_sets, b_sets = str(l_sets), str(w_sets)
                    break

            match_rows.append({
                "Round": round_label,
                "Player A": a_name,
                "Player B": b_name,
                "Winner": winner,
                "Participant A score": a_sets,
                "Participant B score": b_sets,
            })

            next_round.append({
                "name": winner,
                "slot_type": "player" if winner else "unknown",
            })

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


def resolve_runtime_urls(
    tournament_url: str,
    draw_page_url: str,
    results_page_url: str,
    tournament_id: str,
    year: int,
) -> tuple[str, str, str, str]:
    tournament_url = normalize_url(tournament_url or DEFAULT_TOURNAMENT_URL)
    draw_page_url = normalize_url(draw_page_url or DEFAULT_DRAW_PAGE)
    results_page_url = normalize_url(results_page_url or DEFAULT_RESULTS_PAGE)
    tournament_id = (tournament_id or DEFAULT_TOURNAMENT_ID or "").strip()

    if tournament_url:
        if not draw_page_url:
            draw_page_url = infer_draw_page(tournament_url)
        if not results_page_url:
            results_page_url = infer_results_page(tournament_url)

    if not draw_page_url and results_page_url:
        draw_page_url = re.sub(r"/results$", "/draws", results_page_url)

    if not results_page_url and draw_page_url:
        results_page_url = infer_results_page(draw_page_url)

    if not draw_page_url:
        raise ValueError("Devi specificare --tournament-url oppure --draw-page")

    if not tournament_id:
        tournament_id = infer_tournament_id(draw_page_url)

    if not tournament_id and tournament_url:
        tournament_id = infer_tournament_id(tournament_url)

    if not tournament_id:
        raise ValueError("Impossibile ricavare tournament_id dall'URL. Passa --tournament-id")

    fallback_pdf_url = DEFAULT_FALLBACK_PDF.format(year=year, tournament_id=tournament_id)
    return draw_page_url, results_page_url, tournament_id, fallback_pdf_url


def fetch_and_build_rows(draw_page_url: str, results_page_url: str, fallback_pdf_url: str) -> tuple[list[dict], dict]:
    pdf_url = discover_pdf_url(draw_page_url, fallback_pdf_url)

    pdf_resp = requests.get(pdf_url, timeout=60)
    pdf_resp.raise_for_status()
    pdf_bytes = pdf_resp.content

    pages_text = extract_pdf_text(pdf_bytes)
    released_at = extract_released_at(pages_text)
    positions = parse_draw_positions(pages_text)

    results_list = fetch_results_page(results_page_url)
    round_results = group_results_by_round(results_list)

    rows = build_match_rows_from_results_page(positions, round_results)

    meta = {
        "source_draw_page": draw_page_url,
        "source_pdf": pdf_url,
        "source_results_page": results_page_url,
        "released_at": released_at,
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "results_found": len(results_list),
        "results_source": results_list[0]["source"] if results_list else "none",
    }
    return rows, meta


def write_csv_if_changed(output_path: Path, data: bytes) -> bool:
    if output_path.exists() and output_path.read_bytes() == data:
        return False
    output_path.write_bytes(data)
    return True


def run_once(
    output_path: Path,
    tournament_url: str,
    draw_page_url: str,
    results_page_url: str,
    tournament_id: str,
    year: int,
) -> bool:
    draw_page_url, results_page_url, tournament_id, fallback_pdf_url = resolve_runtime_urls(
        tournament_url=tournament_url,
        draw_page_url=draw_page_url,
        results_page_url=results_page_url,
        tournament_id=tournament_id,
        year=year,
    )

    rows, meta = fetch_and_build_rows(draw_page_url, results_page_url, fallback_pdf_url)
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)

    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    print(
        f"[{utc_now_iso()}] {status} | file={output_path} | matches={meta['matches']} | "
        f"results_found={meta['results_found']} | results_source={meta['results_source']} | "
        f"released_at={meta['released_at'] or 'n/d'} | sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )

    return changed


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match dal draw ATP ufficiale.")
    parser.add_argument("--output", default="matches.csv", help="Percorso del file CSV da creare/aggiornare")
    parser.add_argument(
        "--tournament-url",
        default=DEFAULT_TOURNAMENT_URL,
        help="URL base del torneo, es. .../current/miami/403 oppure .../current-challenger/asuncion/2909",
    )
    parser.add_argument(
        "--draw-page",
        default=DEFAULT_DRAW_PAGE,
        help="URL della pagina ATP del draw; se omesso viene derivato da --tournament-url",
    )
    parser.add_argument(
        "--results-page",
        default=DEFAULT_RESULTS_PAGE,
        help="URL della pagina ATP Results; se omesso viene derivato da --tournament-url o --draw-page",
    )
    parser.add_argument(
        "--tournament-id",
        default=DEFAULT_TOURNAMENT_ID,
        help="ID torneo ATP/Challenger; se omesso viene ricavato dagli URL",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Anno usato per il PDF fallback ProTennisLive",
    )
    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(
            output_path=output_path,
            tournament_url=args.tournament_url,
            draw_page_url=args.draw_page,
            results_page_url=args.results_page,
            tournament_id=args.tournament_id,
            year=args.year,
        )
        return 0

    while True:
        try:
            run_once(
                output_path=output_path,
                tournament_url=args.tournament_url,
                draw_page_url=args.draw_page,
                results_page_url=args.results_page,
                tournament_id=args.tournament_id,
                year=args.year,
            )
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
