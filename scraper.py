"""
Playwright scraper for Mitcham CC junior stats from play.cricket.com.au.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import Page, sync_playwright

logger = logging.getLogger("mitcham_scraper")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[MitchamStats] %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)


def _perf(label: str, t0: float) -> None:
    logger.info("%s: %.2fs", label, time.perf_counter() - t0)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _debug_browser_season() -> bool:
    return _env_truthy("MITCHAM_DEBUG_BROWSER")


@dataclass
class BattingRow:
    player: str
    runs: int
    balls: int
    not_out: bool


def _dismissal_not_out(dismissal: str) -> bool:
    d = (dismissal or "").lower()
    return "not out" in d


def format_batting_display(row: BattingRow) -> str:
    if row.not_out:
        return f"{row.player} – {row.runs} not out"
    return f"{row.player} – {row.runs}"


def format_bowling_display(name: str, wickets: int, runs_conceded: int) -> str:
    return f"{name} – {wickets}/{runs_conceded}"

# UUID-only club URLs 404 on play.cricket.com.au; slug + UUID is the working route for Mitcham CC.
CLUB_UUID = "624939ca-87d8-eb11-a7ad-2818780da0cc"
CLUB_URL_AS_GIVEN = f"https://play.cricket.com.au/club/{CLUB_UUID}"
CLUB_PAGE = f"https://play.cricket.com.au/club/mitcham-cricket-club/{CLUB_UUID}"

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def is_junior_fast9_or_super7_label(label: str) -> bool:
    """U10-style Fast 9's / Super 7's competitions — always junior, never senior."""
    s = label or ""
    if re.search(r"(?i)fast\s*9", s):
        return True
    if re.search(r"(?i)super\s*7", s):
        return True
    return False


def is_junior_team_label(label: str) -> bool:
    """Under-age and junior-only comps: U10–U18, Stage 1 / Stage 2, Fast 9's, Super 7's, etc."""
    s = label or ""
    if is_junior_fast9_or_super7_label(s):
        return True
    if re.search(r"(?i)\bunder\s*(10|12|14|16|18)\b", s):
        return True
    if re.search(r"(?i)(?<![0-9])u/?\s*(10|12|14|16|18)(?![0-9])", s):
        return True
    if re.search(r"(?i)\bstage\s*[12]\b", s):
        return True
    return False


TeamCategory = Literal["junior", "senior_men", "senior_women"]

# Senior women: competition/team naming (not junior U12–U18 paths).
_SENIOR_WOMEN_LABEL_SOURCES: tuple[str, ...] = (
    r"(?i)\begwc\b",
    r"(?i)\bsenior\s+women\b",
    r"(?i)\bwomen'?s?\s+(?:[a-z]\s+)?(?:grade|xi|shield|weekly|premier)\b",
    r"(?i)\b(?:womens|ladies)\b.*\b(?:xi|grade|shield|weekly|cup)\b",
    r"(?i)\bfemale\b.*\b(?:xi|grade)\b",
)
_SENIOR_WOMEN_LABEL_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(src) for src in _SENIOR_WOMEN_LABEL_SOURCES
)

# Senior men (open-age): XI, men's, premier, vets — exclude strings already junior or women's senior.
_SENIOR_MEN_LABEL_SOURCES: tuple[str, ...] = (
    r"(?i)\b(?:[1-9]|1[0-9])(?:st|nd|rd|th)\s+xi\b",
    r"(?i)\b(?:first|second|third|fourth|fifth|sixth)\s+xi\b",
    r"(?i)\b(?:over|o)\s*40\b",
    r"(?i)\bveterans?\b",
    r"(?i)\bmasters?\b",
    r"(?i)\b(?:o\s*40|ov(?:er)?\s*40)\b",
    r"(?i)\bmen'?s?\b",
    r"(?i)\bopen\s+age\b",
    r"(?i)\bpremier\b",
    r"(?i)\bcolts\b",
    r"(?i)\bsub[\s-]*district\b",
)
_SENIOR_MEN_LABEL_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(src) for src in _SENIOR_MEN_LABEL_SOURCES
)

# Broad hints for non-junior sides not caught above (legacy).
_SENIOR_BROAD_HINT_SOURCES: tuple[str, ...] = (
    r"(?i)\b(?:women|womens|women's|ladies)\b",
    r"(?i)\b(?:[1-9]|1[0-9])(?:st|nd|rd|th)\s+xi\b",
    r"(?i)\b(?:first|second|third)\s+xi\b",
)
_SENIOR_BROAD_HINT_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(src) for src in _SENIOR_BROAD_HINT_SOURCES
)


def _matches_senior_women_label(label: str) -> bool:
    s = (label or "").strip()
    if not s or is_junior_team_label(s):
        return False
    return any(rx.search(s) for rx in _SENIOR_WOMEN_LABEL_RES)


def _matches_senior_men_label(label: str) -> bool:
    s = (label or "").strip()
    if not s or is_junior_team_label(s) or _matches_senior_women_label(s):
        return False
    return any(rx.search(s) for rx in _SENIOR_MEN_LABEL_RES)


def classify_team_label(label: str) -> TeamCategory:
    """
    Single source of truth: junior vs senior men's vs senior women's team labels.
    """
    s = (label or "").strip()
    if not s:
        return "senior_men"
    if is_junior_team_label(s):
        return "junior"
    if _matches_senior_women_label(s):
        return "senior_women"
    if _matches_senior_men_label(s):
        return "senior_men"
    low = s.lower()
    if any(rx.search(s) for rx in _SENIOR_BROAD_HINT_RES):
        if re.search(r"(?i)(women|womens|women's|ladies|egwc|female|senior\s+women)", low):
            return "senior_women"
        return "senior_men"
    return "senior_men"


def is_senior_team_label(label: str) -> bool:
    """True for senior men's or senior women's sides (not juniors)."""
    return classify_team_label(label) in ("senior_men", "senior_women")


def teams_for_scope(
    all_teams: list[TeamRef],
    *,
    include_juniors: bool,
    include_seniors: bool,
) -> list[TeamRef]:
    """Deduplicate by grade_url; juniors vs seniors (men + women) from classify_team_label."""
    if not include_juniors and not include_seniors:
        return []
    seen: set[str] = set()
    out: list[TeamRef] = []
    for t in all_teams:
        cat = classify_team_label(t.label)
        take = (include_juniors and cat == "junior") or (
            include_seniors and cat in ("senior_men", "senior_women")
        )
        if not take:
            continue
        if t.grade_url in seen:
            continue
        seen.add(t.grade_url)
        out.append(t)
    return out


def _parse_first_match_date(card_text: str) -> date | None:
    m = re.search(
        r",\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+at\b",
        card_text,
    )
    if not m:
        return None
    d, mon, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
    mi = MONTHS.get(mon)
    if not mi:
        return None
    try:
        return date(y, mi, d)
    except ValueError:
        return None


def _mitcham_in_string(s: str) -> bool:
    return "mitcham" in (s or "").lower()


def is_valid_mitcham_match(
    rep: Any,
    resolved: dict[str, Any],
    card_text: str,
) -> tuple[bool, str]:
    """
    True only when the scorecard clearly involves Mitcham on exactly one side.
    discovered_team_label must NOT be treated as proof — fixture headers / card win.
    """
    fh_home = (getattr(rep, "fixture_header_home_team", None) or "").strip()
    fh_away = (getattr(rep, "fixture_header_away_team", None) or "").strip()

    if fh_home and fh_away:
        hm, ha = _mitcham_in_string(fh_home), _mitcham_in_string(fh_away)
        if not hm and not ha:
            return False, "fixture_neither_mitcham"
        if hm and ha:
            return False, "fixture_both_mitcham"
        if not (hm ^ ha):
            return False, "fixture_ambiguous"
    elif fh_home or fh_away:
        side = fh_home or fh_away
        if not _mitcham_in_string(side):
            return False, "fixture_single_non_mitcham"
    else:
        cm, co = parse_match_card_teams(card_text or "")
        if cm and co and not _mitcham_in_string(cm) and not _mitcham_in_string(co):
            return False, "card_both_sides_non_mitcham"
        rm = (resolved.get("resolved_mitcham_team") or "").strip()
        ro = (resolved.get("resolved_opponent") or "").strip()
        if rm and ro and ro not in ("—", "-"):
            if not (
                _mitcham_in_string(rm)
                and not _mitcham_in_string(ro)
                and len(rm) >= 7
            ):
                return False, "fallback_resolved_pair_invalid"
        else:
            return False, "fixture_missing_incomplete_pair"

    fm = (resolved.get("mitcham_team") or "").strip()
    fo = (resolved.get("opponent") or "").strip()
    if not fm or not _mitcham_in_string(fm):
        return False, "final_mitcham_invalid"
    if fo in ("—", "-", "") or not str(fo).strip():
        return False, "final_opponent_empty"
    if _mitcham_in_string(fo):
        return False, "final_opponent_is_mitcham"
    return True, ""


def _log_match_validation(
    url: str,
    discovered_label: str,
    rep: Any,
    resolved: dict[str, Any],
    ok: bool,
    reject_reason: str,
) -> None:
    msg = (
        f"[MatchValidation] url={url!r} "
        f"discovered_team_label={discovered_label!r} "
        f"fixture_header_home_team={getattr(rep, 'fixture_header_home_team', '')!r} "
        f"fixture_header_away_team={getattr(rep, 'fixture_header_away_team', '')!r} "
        f"resolved_mitcham_team={resolved.get('resolved_mitcham_team')!r} "
        f"resolved_opponent={resolved.get('resolved_opponent')!r} "
        f"final_mitcham_team={resolved.get('mitcham_team')!r} "
        f"final_opponent_team={resolved.get('opponent')!r} "
        f"normalized_result={resolved.get('result')!r} "
        f"is_valid_mitcham_match={ok!r} "
        f"reject_reason={reject_reason!r}"
    )
    logger.info(msg)


def match_status_from_card(card_text: str) -> str:
    u = card_text.upper()
    if "COMPLETED" in u:
        return "Completed"
    if "IN PROGRESS" in u or "LIVE" in u:
        return "In progress"
    if "ABANDONED" in u or "CANCELLED" in u:
        return "Abandoned"
    return "Other"


def outcome_from_card(card_text: str) -> str | None:
    """
    Return 'win' | 'loss' | 'draw' | None from match card text.
    Uses the first line that looks like a result (contains 'won by', 'tied', etc.).
    """
    if "COMPLETED" not in card_text.upper():
        return None
    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    result_line = None
    for ln in lines:
        low = ln.lower()
        if "won by" in low or "tied" in low or "draw" in low or "scores level" in low:
            result_line = ln
            break
    if not result_line:
        return None
    low = result_line.lower()
    if "tied" in low or "scores level" in low:
        return "draw"
    if "match drawn" in low or ("draw" in low and "won" not in low):
        return "draw"
    if "won by" not in low:
        return None
    m = re.search(r"(.+?)\s+won\s+by\b", result_line, flags=re.I)
    if not m:
        return None
    winner = m.group(1).strip()
    return "win" if _mitcham_in_string(winner) else "loss"


def _trim_team_label_side(s: str) -> str:
    s = (s or "").strip()
    s = re.split(r"[\|\-–—]\s*", s, 1)[0].strip()
    s = re.split(r"\s+at\s+", s, flags=re.I)[0].strip()
    s = re.split(r"\s+Round\s+\d", s, flags=re.I)[0].strip()
    return s


def _split_teams_from_title_blob(blob: str) -> tuple[str | None, str | None]:
    """
    Parse Mitcham side and opponent from 'A vs B' / 'A v B' title or header text.
    Returns (mitcham_team, opponent) when both sides can be identified.
    """
    t = re.sub(r"\s+", " ", (blob or "").strip())
    if len(t) < 3:
        return None, None
    for sep in (r"\s+vs\.?\s+", r"\s+v\.?\s+"):
        m = re.search(r"(.+?)" + sep + r"(.+)", t, flags=re.I)
        if not m:
            continue
        left = _trim_team_label_side(m.group(1))
        right = _trim_team_label_side(m.group(2))
        if not left or not right:
            continue
        lm, rm = _mitcham_in_string(left), _mitcham_in_string(right)
        if lm and not rm:
            return left[:120], right[:120]
        if rm and not lm:
            return right[:120], left[:120]
        if lm and rm:
            return left[:120], right[:120]
    return None, None


def _best_mitcham_opponent_pair_from_blob(blob: str) -> tuple[str | None, str | None]:
    """
    Try each | segment (title often joins many headings). Prefer a pair with exactly
    one Mitcham side so roles are unambiguous.
    """
    for seg in re.split(r"\s*\|\s*", blob or ""):
        seg = seg.strip()
        if len(seg) < 6:
            continue
        a, b = _split_teams_from_title_blob(seg)
        if not a or not b:
            continue
        la, lb = _mitcham_in_string(a), _mitcham_in_string(b)
        if la and not lb:
            return a[:120], b[:120]
        if lb and not la:
            return b[:120], a[:120]
        if la and lb:
            return a[:120], b[:120]
    return None, None


# Substrings that must never appear in a scorecard-header fallback team name or segment.
_SCORECARD_HEADER_JUNK_SUBSTRINGS: tuple[str, ...] = (
    "play cricket",
    "playcricket app",
    "fall of wickets",
    "scorecard",
    "ball by ball",
    "graphs",
    "summary",
)


def _segment_has_scorecard_junk_keyword(seg: str) -> bool:
    low = (seg or "").lower()
    return any(j in low for j in _SCORECARD_HEADER_JUNK_SUBSTRINGS)


def _segment_is_probable_venue_only(seg: str) -> bool:
    """Drop pipe segments that look like ground/venue lines without teams or scores."""
    t = re.sub(r"^\s*one\s+day\s*", "", (seg or "").strip(), flags=re.I)
    if re.search(r"\bCOMPLETED\b", t, flags=re.I):
        return False
    if re.search(r"\bwon\s+by\b", t, flags=re.I):
        return False
    if re.search(r"\d+\s*[-/]\s*\d+", t):
        return False
    if re.search(r"\bU\d{1,2}\s*\(", t, flags=re.I):
        return False
    if _mitcham_in_string(t):
        return False
    return bool(
        re.search(
            r"\b(oval|ground|park|recreation|sports\s*cent|secondary\s+college|primary\s+school)\b",
            t,
            flags=re.I,
        )
    )


def _segment_is_probable_datetime_only(seg: str) -> bool:
    """Drop segments that are mostly a date/time line, not a scorecard header."""
    t = (seg or "").strip()
    if len(t) > 120:
        return False
    if re.search(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", t):
        return True
    if re.search(
        r"\b(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\s+\d{1,2}\b",
        t,
        flags=re.I,
    ):
        return True
    if re.search(
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b",
        t,
        flags=re.I,
    ):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\s*(?:am|pm)?\b", t, flags=re.I):
        return len(t) < 60
    return False


def _discard_segment_before_scorecard_header_parse(seg: str) -> bool:
    s = (seg or "").strip()
    if len(s) < 8:
        return True
    if _segment_has_scorecard_junk_keyword(s):
        return True
    if _segment_is_probable_venue_only(s):
        return True
    if _segment_is_probable_datetime_only(s):
        return True
    return False


def _clean_segments_for_scorecard_header_fallback(blob: str) -> list[str]:
    segs = [s.strip() for s in re.split(r"\s*\|\s*", blob or "") if s.strip()]
    return [s for s in segs if not _discard_segment_before_scorecard_header_parse(s)]


def _segment_looks_like_scorecard_header_line(seg: str) -> bool:
    """[score] Team … COMPLETED [score] Team …"""
    if not re.search(r"\bCOMPLETED\b", seg, flags=re.I):
        return False
    parts = re.split(r"\bCOMPLETED\b", seg, maxsplit=1, flags=re.I)
    if len(parts) != 2:
        return False
    left, right = parts[0].strip(), parts[1].strip()
    return bool(
        re.search(r"\d+\s*[-/]\s*\d+", left)
        and re.search(r"\d+\s*[-/]\s*\d+", right)
    )


def _ordered_scorecard_header_candidate_segments(cleaned: list[str]) -> list[str]:
    preferred = [s for s in cleaned if _segment_looks_like_scorecard_header_line(s)]
    preferred.sort(key=len)
    rest = [
        s
        for s in cleaned
        if s not in preferred and re.search(r"\bCOMPLETED\b", s, flags=re.I)
    ]
    rest.sort(key=len)
    return preferred + rest


def _valid_scorecard_fallback_team_name(name: str | None) -> bool:
    if not name:
        return False
    n = name.strip()
    if len(n) < 2 or "|" in n:
        return False
    low = n.lower()
    if any(j in low for j in _SCORECARD_HEADER_JUNK_SUBSTRINGS):
        return False
    if "completed" in low:
        return False
    if re.search(r"\bwon\s+by\b", low):
        return False
    # Innings lines like 4-101 / 6-98 should not remain inside a team label.
    if re.search(r"\b\d{1,2}-\d{2,4}\b", n):
        return False
    return True


def _strip_scorecard_segment_for_team_parse(seg: str) -> str:
    """Remove trailing result clauses so COMPLETED header parsing can succeed."""
    t = re.sub(r"\s+", " ", (seg or "").strip())
    if not t:
        return t
    t = re.sub(r"(?i)\s+won\s+by\s+.*$", "", t)
    t = re.sub(r"(?i)\s+lost\s+by\s+.*$", "", t)
    return t.strip()


def _parse_teams_from_scorecard_header_blob_validated(
    blob: str,
) -> tuple[str | None, str | None]:
    hm, ho = _parse_teams_from_scorecard_header_blob(blob)
    if not _valid_scorecard_fallback_team_name(hm) or not _valid_scorecard_fallback_team_name(
        ho
    ):
        return None, None
    return hm, ho


def _side_name_from_scorecard_header_chunk(chunk: str) -> str | None:
    """
    Step A: strip leading score (e.g. 3-82, 7 / 77).
    Step B: if at least two '(' in the chunk, strip only the LAST (number) — overs;
    keep grade suffix like U14 (2) when (23) is removed.
    """
    c = re.sub(r"\s+", " ", (chunk or "").strip())
    if not c:
        return None
    c = re.sub(r"^\d+\s*[-/]\s*\d+\s+", "", c)
    c = c.strip()
    if not c:
        return None
    if c.count("(") >= 2:
        c = re.sub(r"\s*\(\d+\)\s*$", "", c)
    c = c.strip()
    if len(c) < 2:
        return None
    return c[:120]


def _parse_teams_from_scorecard_header_blob(blob: str) -> tuple[str | None, str | None]:
    """
    Fallback when no 'A vs B' in blob: scorecard-style
    [score] Team A … (overs) COMPLETED [score] Team B … (overs)
    """
    t = re.sub(r"\s+", " ", (blob or "").strip())
    if len(t) < 12 or not re.search(r"\bCOMPLETED\b", t, flags=re.I):
        return None, None
    parts = re.split(r"\bCOMPLETED\b", t, maxsplit=1, flags=re.I)
    if len(parts) != 2:
        return None, None
    left_raw, right_raw = parts[0].strip(), parts[1].strip()

    team_a = _side_name_from_scorecard_header_chunk(left_raw)
    team_b = _side_name_from_scorecard_header_chunk(right_raw)

    logger.debug(
        "[ScorecardHeaderParse] left_chunk=%r right_chunk=%r team_a=%r team_b=%r",
        left_raw,
        right_raw,
        team_a,
        team_b,
    )

    if not team_a or not team_b:
        return None, None

    la, lb = _mitcham_in_string(team_a), _mitcham_in_string(team_b)
    if la and not lb:
        logger.debug(
            "[ScorecardHeaderParse] resolved mitcham_team=%r opponent=%r",
            team_a,
            team_b,
        )
        return team_a, team_b
    if lb and not la:
        logger.debug(
            "[ScorecardHeaderParse] resolved mitcham_team=%r opponent=%r",
            team_b,
            team_a,
        )
        return team_b, team_a
    if la and lb:
        logger.debug(
            "[ScorecardHeaderParse] resolved mitcham_team=%r opponent=%r",
            team_a,
            team_b,
        )
        return team_a, team_b
    logger.debug(
        "[ScorecardHeaderParse] unresolved: neither side clearly Mitcham "
        "team_a=%r team_b=%r",
        team_a,
        team_b,
    )
    return None, None


def _opponent_from_match_title_blob(text: str) -> str | None:
    """Pick the non-Mitcham side from a title or header like 'MIT v TEM' or 'Mitcham vs Foo'."""
    m, o = _split_teams_from_title_blob(text)
    if o and not _mitcham_in_string(o):
        return o[:100]
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        return None
    for sep in (r"\s+vs\.?\s+", r"\s+v\.?\s+"):
        parts = re.split(sep, t, maxsplit=1, flags=re.I)
        if len(parts) < 2:
            continue
        for chunk in (parts[0], parts[1]):
            chunk = re.split(r"[\|\-–—]", chunk, 1)[0].strip()
            if not chunk or _mitcham_in_string(chunk):
                continue
            if len(chunk) > 2 and not re.match(r"^[\d\-/&]+$", chunk):
                return chunk[:100]
    return None


def _parse_teams_from_card_vs_line(card_text: str) -> tuple[str | None, str | None]:
    """Fixture cards sometimes include a single 'Team A vs Team B' line."""
    for ln in card_text.splitlines():
        ln = ln.strip()
        if len(ln) < 6:
            continue
        low = ln.lower()
        if " vs " not in low and not re.search(r"\s+v\s+", ln, flags=re.I):
            continue
        if "won by" in low:
            continue
        if "completed" in low and len(ln) < 50:
            continue
        return _split_teams_from_title_blob(ln)
    return None, None


def _extract_match_page_metadata(page: Page) -> dict[str, Any]:
    """Title/header team names plus lines that look like match results (won by, tie, etc.)."""
    try:
        raw = page.evaluate(
            r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const parts = [];
          parts.push(norm(document.title || ''));
          for (const sel of [
            'h1', 'h2', 'h3',
            '[class*="MatchTitle" i]', '[class*="match-title" i]',
            '[class*="fixture" i]', '[class*="match-header" i]', '[class*="team-name" i]'
          ]) {
            for (const el of document.querySelectorAll(sel)) {
              const x = norm(el.innerText || '');
              if (x && x.length < 260) parts.push(x);
            }
          }
          const blob = parts.filter(Boolean).join(' | ');
          const resultLines = [];
          const body = norm(document.body && document.body.innerText || '');
          for (const line of body.split(/\n+/)) {
            const t = norm(line);
            if (!t || t.length > 240) continue;
            const low = t.toLowerCase();
            if (low.includes('won by') || low.includes('won the') || low.includes('tied')
                || low.includes('match drawn') || low.includes('scores level')
                || low.includes('no result') || low.includes('abandoned')
                || low.includes('defeated')
                || (low.includes('beat') && low.includes('by'))) {
              resultLines.push(t);
              if (resultLines.length >= 12) break;
            }
          }
          return { blob, resultLines };
        }"""
        )
    except Exception:
        return {"blob": "", "resultLines": []}
    if not isinstance(raw, dict):
        return {"blob": "", "resultLines": []}
    blob = (raw.get("blob") or "").strip()
    lines = raw.get("resultLines") or []
    if not isinstance(lines, list):
        lines = []
    lines = [str(x).strip() for x in lines if x]
    mitch, opp = _best_mitcham_opponent_pair_from_blob(blob)
    if not mitch or not opp:
        m2, o2 = _split_teams_from_title_blob(blob)
        mitch = mitch or m2
        opp = opp or o2
    if not opp:
        opp = _opponent_from_match_title_blob(blob)
    return {
        "blob": blob,
        "resultLines": lines[:20],
        "mitcham_from_blob": mitch,
        "opponent_from_blob": opp,
    }


def _extract_fixture_header_metadata(page: Page) -> dict[str, str]:
    """
    Primary scorecard fixture header: team names (overs spans excluded) and result line.
    """
    try:
        raw = page.evaluate(
            r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const teamEls = document.querySelectorAll('div.o-play-match-card__team-name');
          const teams = [];
          for (const el of teamEls) {
            const clone = el.cloneNode(true);
            for (const ov of clone.querySelectorAll('span.o-play-match-card__team-overs')) {
              ov.remove();
            }
            teams.push(norm(clone.innerText || ''));
          }
          const rtEl = document.querySelector('span.o-play-match-card__result-text');
          const resultText = rtEl ? norm(rtEl.innerText || '') : '';
          return {
            homeTeam: teams[0] || '',
            awayTeam: teams[1] || '',
            resultText: resultText
          };
        }"""
        )
    except Exception:
        return {"homeTeam": "", "awayTeam": "", "resultText": ""}
    if not isinstance(raw, dict):
        return {"homeTeam": "", "awayTeam": "", "resultText": ""}
    return {
        "homeTeam": str(raw.get("homeTeam") or "").strip(),
        "awayTeam": str(raw.get("awayTeam") or "").strip(),
        "resultText": str(raw.get("resultText") or "").strip(),
    }


def _team_name_matches_label(winner: str, label: str | None) -> bool:
    """True if fixture result winner/loser fragment refers to this team label."""
    if not winner or not label:
        return False
    w = re.sub(r"\s+", " ", winner.strip().lower())
    l = re.sub(r"\s+", " ", label.strip().lower())
    if not w or not l:
        return False
    if l == w:
        return True
    if l in w or w in l:
        return True
    if len(l) >= 10 and (l[:22] in w or l[-18:] in w):
        return True
    if len(w) >= 10 and (w[:22] in l or w[-18:] in l):
        return True
    return False


def normalize_fixture_header_result_to_compact(
    result_text: str,
    mitcham_team: str | None,
    opponent_team: str | None,
    oc: str | None,
) -> str | None:
    """
    Map span.o-play-match-card__result-text to Match Results / Facebook compact result.
    """
    t = re.sub(r"\s+", " ", (result_text or "").strip())
    if not t:
        return None
    tl = t.lower()
    if tl in ("completed", "complete", "—", "-"):
        return None
    if "no result" in tl or ("abandoned" in tl and "won" not in tl):
        return "No result"
    if re.search(r"\b(tied|scores level)\b", tl):
        return "Tie"
    if "match drawn" in tl or (
        "draw" in tl and "won" not in tl and "won by" not in tl
    ):
        return "Draw"

    m = re.search(
        r"(?is)^(.+)\s+won\s+by\s+(\d+)\s+(runs?|wickets?)\s*\.?\s*$",
        t,
    )
    if m:
        winner = m.group(1).strip()
        margin = _normalize_runs_wickets_margin(m.group(2), m.group(3))
        wm = _team_name_matches_label(winner, mitcham_team)
        wo = _team_name_matches_label(winner, opponent_team)
        if wm and not wo:
            return f"Won by {margin}"
        if wo and not wm:
            return f"Lost by {margin}"
        if _mitcham_in_string(winner):
            return f"Won by {margin}"
        return f"Lost by {margin}"

    m2 = re.search(r"(?is)^(.+)\s+won\s*\.?\s*$", t)
    if m2:
        winner = m2.group(1).strip()
        wm = _team_name_matches_label(winner, mitcham_team)
        wo = _team_name_matches_label(winner, opponent_team)
        if wm and not wo:
            return "Won"
        if wo and not wm:
            return "Lost"
        if _mitcham_in_string(winner):
            return "Won"
        return "Lost"

    m3 = re.search(
        r"(?is)^(.+)\s+lost\s+by\s+(\d+)\s+(runs?|wickets?)\s*\.?\s*$",
        t,
    )
    if m3:
        loser = m3.group(1).strip()
        margin = _normalize_runs_wickets_margin(m3.group(2), m3.group(3))
        lm = _team_name_matches_label(loser, mitcham_team)
        lo = _team_name_matches_label(loser, opponent_team)
        if lm and not lo:
            return f"Lost by {margin}"
        if lo and not lm:
            return f"Won by {margin}"
        if _mitcham_in_string(loser):
            return f"Lost by {margin}"
        return f"Won by {margin}"

    if oc == "win":
        return "Won"
    if oc == "loss":
        return "Lost"
    if oc == "draw":
        return "Draw"
    return None


def parse_match_card_teams(card_text: str) -> tuple[str, str]:
    """Best-effort Mitcham XI name and opponent from the grade match card."""
    vm, vo = _parse_teams_from_card_vs_line(card_text)
    if vm and vo:
        return vm, vo
    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    mitcham_candidates = [
        ln
        for ln in lines
        if _mitcham_in_string(ln)
        and "won by" not in ln.lower()
        and not re.match(r"^[\d\-/&]+$", ln)
    ]
    mitcham_team = mitcham_candidates[-1] if mitcham_candidates else "Mitcham"
    for ln in lines:
        low = ln.lower()
        if _mitcham_in_string(ln):
            continue
        if "completed" in low or "progress" in low or "live" in low:
            continue
        if "won by" in low or "tied" in low or "draw" in low:
            continue
        if re.match(r"^[\d\-/&]+(\s|$)", ln) and len(ln) < 18:
            continue
        if re.search(r",\s*\d{1,2}\s+\w+\s+\d{4}\s+at\b", ln):
            continue
        if "round" in low and len(ln) < 30:
            continue
        if len(ln) > 3:
            return mitcham_team, ln
    return mitcham_team, "—"


def result_display_from_card(card_text: str, outcome: str | None, status: str) -> str:
    if status == "In progress":
        return "In progress"
    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    if outcome == "draw":
        for ln in lines:
            if "tied" in ln.lower() or "draw" in ln.lower():
                return ln[:120]
        return "Draw / tie"
    if outcome == "win":
        for ln in lines:
            if "won by" in ln.lower() and _mitcham_in_string(ln):
                return ln[:120]
        return "Win"
    if outcome == "loss":
        for ln in lines:
            if "won by" in ln.lower() and not _mitcham_in_string(ln):
                return f"Loss — {ln[:100]}"
        return "Loss"
    if status == "Completed":
        return "Completed (result not parsed)"
    return "—"


def _normalize_runs_wickets_margin(n: str, unit: str) -> str:
    """Canonical '20 runs' / '3 wickets' from regex groups (singular or plural)."""
    u = unit.lower()
    if u.startswith("run"):
        return f"{n} runs"
    return f"{n} wickets"


def _extract_won_by_margin_phrase(s: str) -> str | None:
    """Only the margin part, e.g. '49 runs' — never trailing team names or scores."""
    m = re.search(
        r"\bwon\s+by\s+(\d+)\s+(runs?|wickets?)\b", s, flags=re.I
    )
    if not m:
        return None
    return _normalize_runs_wickets_margin(m.group(1), m.group(2))


def _extract_lost_by_margin_phrase(s: str) -> str | None:
    m = re.search(
        r"\blost\s+by\s+(\d+)\s+(runs?|wickets?)\b", s, flags=re.I
    )
    if not m:
        return None
    return _normalize_runs_wickets_margin(m.group(1), m.group(2))


def _winner_text_before_strict_won_by(s: str) -> str | None:
    s = re.sub(r"\s+", " ", (s or "").strip())
    m = re.search(r"\bwon\s+by\s+\d+\s+(runs?|wickets?)\b", s, flags=re.I)
    if not m:
        return None
    before = s[: m.start()].strip()
    before = before.split("|")[-1].strip()
    return before or None


def _mitcham_won_from_winner_fragment(
    winner: str | None,
    mitcham_team: str | None,
    opponent: str | None,
) -> bool | None:
    """True if Mitcham won; False if opponent won; None if unclear."""
    if not winner:
        return None
    wlow = winner.lower()
    if _mitcham_in_string(winner):
        return True
    if mitcham_team:
        mt = mitcham_team.strip().lower()
        if mt and (mt in wlow or wlow in mt or (len(mt) > 10 and mt[:14] in wlow)):
            return True
    if opponent:
        ol = opponent.strip().lower()
        if ol and len(ol) > 4 and ol in wlow:
            return False
    return None


def _compact_result_from_line(
    raw: str,
    oc: str | None,
    mitcham_team: str | None,
    opponent: str | None,
) -> str | None:
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if not s:
        return None
    low = s.lower()
    if "tied" in low or "scores level" in low:
        return "Tie"
    if "match drawn" in low:
        return "Draw"
    if "no result" in low or ("abandoned" in low and "won" not in low):
        return "No result"
    if "draw" in low and "won" not in low and "won by" not in low:
        return "Draw"

    margin = _extract_won_by_margin_phrase(s)
    if not margin:
        lm = _extract_lost_by_margin_phrase(s)
        if lm:
            return f"Lost by {lm}"
        return None
    winner = _winner_text_before_strict_won_by(s)
    mitcham_won = _mitcham_won_from_winner_fragment(winner, mitcham_team, opponent)

    if oc == "draw":
        return "Tie" if "tie" in low else "Draw"
    if oc == "win":
        return f"Won by {margin}"
    if oc == "loss":
        return f"Lost by {margin}"
    if mitcham_won is True:
        return f"Won by {margin}"
    if mitcham_won is False:
        return f"Lost by {margin}"
    if winner and _mitcham_in_string(winner):
        return f"Won by {margin}"
    if winner:
        return f"Lost by {margin}"
    return None


def _infer_result_from_broad_blob(
    card_text: str,
    page_result_lines: list[str],
    oc: str | None,
    mitcham_team: str | None,
    opponent: str | None,
) -> str | None:
    """When line-by-line parsing missed 'won by', scan full card + result lines."""
    blob = re.sub(
        r"\s+",
        " ",
        (card_text or "") + " " + " ".join(page_result_lines or []),
    )
    if not blob.strip():
        return None
    m = re.search(
        r"\bwon\s+by\s+(\d+)\s+(runs?|wickets?)\b", blob, flags=re.I
    )
    if not m:
        lm = re.search(
            r"\blost\s+by\s+(\d+)\s+(runs?|wickets?)\b", blob, flags=re.I
        )
        if not lm:
            return None
        margin = _normalize_runs_wickets_margin(lm.group(1), lm.group(2))
        return f"Lost by {margin}"
    margin = _normalize_runs_wickets_margin(m.group(1), m.group(2))
    idx = m.start()
    before = blob[max(0, idx - 220) : idx]
    synthetic = (before + f" won by {margin}").strip()
    out = _compact_result_from_line(
        synthetic, oc, mitcham_team, opponent
    )
    if out:
        return out
    if oc == "win":
        return f"Won by {margin}"
    if oc == "loss":
        return f"Lost by {margin}"
    return f"Won by {margin}"


def normalize_match_result_display(
    status: str,
    oc: str | None,
    card_text: str,
    page_result_lines: list[str],
    mitcham_team: str | None,
    opponent: str | None,
) -> str:
    """Compact outcome for Match Results (no team names in this column)."""
    if status == "In progress":
        return "In progress"
    if status in ("Abandoned",):
        return "No result"
    if status == "Other":
        return "—"

    candidates: list[str] = []
    full_join = "\n".join([card_text or "", *(page_result_lines or [])])
    if full_join.strip():
        candidates.append(re.sub(r"\s+", " ", full_join)[:4000])
    for ln in (card_text or "").splitlines():
        t = ln.strip()
        if not t:
            continue
        low = t.lower()
        if "won by" in low or "lost by" in low:
            candidates.append(t)
        if (
            "won by" in low
            or "tied" in low
            or "scores level" in low
            or "match drawn" in low
            or ("draw" in low and "won by" not in low)
        ):
            candidates.append(t)
    candidates.extend(page_result_lines or [])

    scored: list[tuple[int, str]] = []
    for t in candidates:
        strict = bool(
            re.search(r"\bwon\s+by\s+\d+\s+(runs?|wickets?)\b", t, flags=re.I)
        )
        scored.append((0 if strict else 1, t))
    scored.sort(key=lambda x: x[0])
    for _, raw in scored:
        compact = _compact_result_from_line(raw, oc, mitcham_team, opponent)
        if compact:
            return compact

    if oc == "draw":
        return "Tie" if "tie" in (card_text or "").lower() else "Draw"
    blob = " ".join((card_text or "").splitlines())
    for chunk in [blob, *list(page_result_lines or [])]:
        margin = _extract_won_by_margin_phrase(chunk)
        if margin:
            if oc == "win":
                return f"Won by {margin}"
            if oc == "loss":
                return f"Lost by {margin}"
            break
    if oc == "win":
        return "Won"
    if oc == "loss":
        return "Lost"
    if status == "Completed":
        inferred = _infer_result_from_broad_blob(
            card_text,
            page_result_lines,
            oc,
            mitcham_team,
            opponent,
        )
        if inferred:
            return inferred
        return "Completed"
    return "—"


def _raw_result_snippet_for_log(card_text: str, page_lines: list[str]) -> str:
    for ln in (card_text or "").splitlines():
        low = ln.lower()
        if "won by" in low or "tied" in low or "draw" in low:
            return ln.strip()[:220]
    if page_lines:
        return (page_lines[0] or "")[:220]
    return ""


_TRAILING_TEAM_PARENS_RE = re.compile(r"^(.*)\s*\(([^)]*)\)\s*$")


def _paren_content_is_overs_or_progress(inner: str) -> bool:
    """True for (30.2), (20), (50); False for squad markers (1)–(9)."""
    inner = (inner or "").strip()
    if not inner:
        return False
    if re.fullmatch(r"\d+\.\d+", inner):
        return True
    if re.fullmatch(r"\d+", inner):
        n = int(inner)
        if 1 <= n <= 9:
            return False
        return True
    return False


def _paren_content_is_competition_metadata(inner: str) -> bool:
    """(80 Overs, 12 Players), (Split Innings - …), etc. — not squad (1)/(2)."""
    low = (inner or "").lower()
    if not low:
        return False
    if "overs" in low or "player" in low or "split innings" in low:
        return True
    if re.search(r"\b\d+\s*overs\b", low):
        return True
    return False


def _strip_leading_scoreboard_prefix(s: str) -> str:
    """Remove leading totals like '138 ', '133 & 0-0 ', '& 0-0 ', '249 & 0-0 '."""
    t = s.strip()
    if not t:
        return t
    for _ in range(12):
        orig = t
        m = re.match(r"^&\s*[\d.]+\s*-\s*[\d.]+\s+", t)
        if m:
            t = t[m.end() :].lstrip()
            continue
        m = re.match(r"^\d{1,4}\s*&\s*[\d.]+\s*-\s*[\d.]+\s+", t)
        if m:
            t = t[m.end() :].lstrip()
            continue
        m = re.match(r"^\d{2,4}\s+(?=[A-Za-z&])", t)
        if m:
            t = t[m.end() :].lstrip()
            continue
        if t == orig:
            break
    return t


def _strip_trailing_overs_loop(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    while True:
        m = _TRAILING_TEAM_PARENS_RE.match(s)
        if not m:
            break
        body, inner = m.group(1).strip(), m.group(2)
        if not _paren_content_is_overs_or_progress(inner):
            break
        s = body
    return s


def _strip_trailing_competition_metadata_parens(s: str) -> str:
    """Remove trailing (80 Overs, 12 Players) etc. repeatedly."""
    t = (s or "").strip()
    if not t:
        return t
    for _ in range(8):
        m = _TRAILING_TEAM_PARENS_RE.match(t)
        if not m:
            break
        inner = (m.group(2) or "").strip()
        if not _paren_content_is_competition_metadata(inner):
            break
        t = m.group(1).strip()
    return t


def _strip_mitcham_duplicate_grade_segment(s: str) -> str:
    """'Mitcham U14 (2) U14 - 6 (SEDA)' -> 'Mitcham U14 (2)'."""
    return re.sub(
        r"\s+U\d{1,2}\s*-\s*\d+\s*\([^)]+\)\s*$",
        "",
        (s or "").strip(),
        flags=re.I,
    ).strip()


def _strip_mitcham_numbered_shield_suffix(s: str) -> str:
    """'… 1. Compare & Connect … Shield' -> strip from ' 1.' onwards."""
    return re.sub(r"\s+\d+\.\s+.*$", "", (s or "").strip()).strip()


def _strip_mitcham_space_grade_block(s: str) -> str:
    """'… B Grade (80 Overs, 12 Players)' at end."""
    t = re.sub(
        r"\s+[A-Z]\s+Grade\s*\([^)]*(?:Overs|Players|Split)[^)]*\)\s*$",
        "",
        (s or "").strip(),
        flags=re.I,
    )
    return t.strip()


def _dedupe_mitcham_fast9_super7_lines(s: str) -> str:
    """Remove duplicated Fast 9's / Super 7's competition tail on Mitcham lines."""
    t = (s or "").strip()
    m = re.match(r"(?i)^(.+?-\s*Fast\s*9'?s)\s+Fast\s*9'?s\s+.*$", t)
    if m:
        return m.group(1).strip()
    m = re.match(r"(?i)^(.+?-\s*Super\s*7'?s\s+\S+)\s+Super\s*7'?s\s+.*$", t)
    if m:
        return m.group(1).strip()
    return t


def _strip_trailing_overs_and_competition_loop(s: str) -> str:
    """Alternate overs/progress and competition-metadata parens until stable."""
    t = (s or "").strip()
    for _ in range(12):
        prev = t
        t = _strip_trailing_overs_loop(t)
        t = _strip_trailing_competition_metadata_parens(t)
        if t == prev:
            break
    return t


def _strip_amp_score_fragments(s: str) -> str:
    """Remove trailing/leading '& 4', '& 2-81' fragments leaked from score rows."""
    t = re.sub(r"\s+", " ", (s or "").strip())
    if not t or t in ("—", "-"):
        return t
    for _ in range(8):
        prev = t
        t = re.sub(r"\s*&\s*\d+(?:-\d+)?\s*$", "", t).strip()
        t = re.sub(r"^\s*&\s*\d+(?:-\d+)?\s*", "", t).strip()
        if t == prev:
            break
    return t


def _strip_senior_mitcham_competition_suffix(s: str) -> str:
    """
    Strip shield / EGWC / grade tails after senior XI names only (not junior U14/U16).
    """
    t = re.sub(r"\s+", " ", (s or "").strip())
    if not t:
        return t
    if re.search(r"(?i)\bU\s*/?\s*(10|12|14|16|18)\b", t) and not re.search(
        r"(?i)\b(?:1st|2nd|3rd|4th|5th)\s+XI\b",
        t,
    ):
        return t
    if not re.search(r"(?i)\b(?:1st|2nd|3rd|4th|5th)\s+XI\b", t) and not re.search(
        r"(?i)\bmitcham\s+women\b",
        t,
    ):
        return t
    t = re.sub(
        r"(?i)\s+\d+\.\s+Compare\s*&\s*Con(?:n)?ect\s+.+?(?:Shield|shield)\s*$",
        "",
        t,
    )
    t = re.sub(
        r"(?i)\s+\d+\.\s+.+?(?:McIntosh|mcintosh)\s+Shield\s*$",
        "",
        t,
    )
    t = re.sub(r"(?i)\s+EGWC\s+Senior\s+Women\s+.+$", "", t)
    t = re.sub(r"(?i)\s+EGWC\s+.+$", "", t)
    t = re.sub(
        r"(?i)\s+\b[A-Z]\s+Grade\s*\([^)]*(?:Overs|Players|Split|Weekly)[^)]*\)\s*$",
        "",
        t,
    )
    t = re.sub(r"(?i)\s+B\s+Grade\s*\(80\s+Overs[^)]*\)\s*$", "", t)
    t = re.sub(r"(?i)\s+D\s+Grade\s*\(64\s+Overs[^)]*\)\s*$", "", t)
    t = re.sub(r"(?i)\s+H\s+Grade\s*\(Split\s+Innings[^)]*\)\s*$", "", t)
    return t.strip()


def _strip_leaked_match_result_crap_from_team_name(s: str) -> str:
    """Remove result text, vs tails, or leaked 'won …' fragments from a team label."""
    t = re.sub(r"\s+", " ", (s or "").strip())
    if not t or t in ("—", "-"):
        return t
    t = re.sub(r"(?i)\s+won\s+by\s+.*$", "", t)
    t = re.sub(r"(?i)\s+lost\s+by\s+.*$", "", t)
    t = re.sub(r"(?i)\s+vs\.?\s+.*$", "", t)
    t = re.sub(r"(?i)\s+v\.?\s+.*$", "", t)
    t = re.sub(r"(?i)\s+—\s*(completed|abandoned|no result).*$", "", t)
    if _mitcham_in_string(t) and re.search(r"(?i)\s+won\s+", t):
        t = re.split(r"(?i)\s+won\s+", t, maxsplit=1)[0].strip()
    if len(t) > 70 and re.search(r"(?i)\b(?:1st|2nd|3rd|4th|5th)\s+XI\b", t):
        m = re.match(r"(?i)^(.+?\b(?:1st|2nd|3rd|4th|5th)\s+XI)\b", t)
        if m and len(m.group(1)) + 10 < len(t):
            t = m.group(1).strip()
    return t


def _orient_mitcham_opponent_pair(
    hm: str, ho: str
) -> tuple[str | None, str | None]:
    la, lb = _mitcham_in_string(hm), _mitcham_in_string(ho)
    if la and not lb:
        return hm, ho
    if lb and not la:
        return ho, hm
    if la and lb:
        return hm, ho
    return None, None


def _scorecard_pair_quality(m: str, o: str) -> int:
    """Higher = cleaner Mitcham vs opponent pair (for picking best header fallback)."""
    m, o = (m or "").strip(), (o or "").strip()
    if not m or not o or o in ("—", "-"):
        return -10_000
    m = _strip_amp_score_fragments(m)
    o = _strip_amp_score_fragments(o)
    score = 0
    lm, lo = _mitcham_in_string(m), _mitcham_in_string(o)
    if lm and not lo:
        score += 500
    elif lm and lo:
        score += 120
    else:
        score += 40
    for x in (m, o):
        xl = x.lower()
        if re.search(r"\s&\s*\d", x):
            score -= 300
        if re.search(r"(?i)\b(won|lost)\s+by\b", xl):
            score -= 400
        if "completed" in xl and len(xl) < 80:
            score -= 100
        if "|" in x:
            score -= 50
    score += min(len(m) + len(o), 160) // 3
    return score


def _best_validated_scorecard_pair_from_segments(
    ordered: list[str],
) -> tuple[str | None, str | None]:
    """Scan all header segments; return the highest-quality Mitcham + opponent pair."""
    best: tuple[str | None, str | None] = (None, None)
    best_q = -10_000
    seen: set[tuple[str, str]] = set()
    for seg in ordered:
        for candidate in (seg, _strip_scorecard_segment_for_team_parse(seg)):
            if len(candidate) < 12:
                continue
            hm, ho = _parse_teams_from_scorecard_header_blob_validated(candidate)
            if not hm or not ho:
                continue
            pair = _orient_mitcham_opponent_pair(hm, ho)
            pm, po = pair[0], pair[1]
            if not pm or not po:
                continue
            key = (pm, po)
            if key in seen:
                continue
            seen.add(key)
            q = _scorecard_pair_quality(pm, po)
            if q > best_q:
                best_q = q
                best = (pm, po)
    return best


def _fallback_opponent_from_scorecard_segments(
    ordered: list[str],
) -> str | None:
    """If opponent is still blank, pick first valid non-Mitcham label from segments."""
    for seg in ordered:
        for chunk in re.split(r"\s*\|\s*", seg):
            chunk = _trim_team_label_side(chunk.strip())
            chunk = _strip_leading_scoreboard_prefix(chunk)
            chunk = _strip_amp_score_fragments(chunk)
            if not chunk:
                continue
            if not _valid_scorecard_fallback_team_name(chunk):
                continue
            if not _mitcham_in_string(chunk):
                return chunk[:120]
    return None


def _mitcham_team_is_weak(mitch: str | None) -> bool:
    t = (mitch or "").strip()
    if not t:
        return True
    tl = t.lower()
    if tl in ("mitcham", "mitcham cc"):
        return True
    if re.search(r"\s&\s*\d", t):
        return True
    if re.search(r"(?i)\b(won|lost)\s+by\b", t):
        return True
    if "|" in t:
        return True
    if len(tl) < 14 and tl.startswith("mitcham") and "u" not in tl and "xi" not in tl:
        return True
    if len(t) > 100:
        return True
    return False


def finalize_team_display_name(name: str, *, role: str = "opponent") -> str:
    """
    Final cleanup for Match Results / Facebook: scores, overs, competition junk, Mitcham-only tails.
    role: 'mitcham' | 'opponent'
    """
    s = (name or "").strip()
    if not s or s in ("—", "-"):
        return s
    s = _strip_amp_score_fragments(s)
    s = _strip_leading_scoreboard_prefix(s)
    if role == "mitcham":
        s = _strip_mitcham_duplicate_grade_segment(s)
        s = _strip_mitcham_numbered_shield_suffix(s)
        s = _strip_mitcham_space_grade_block(s)
        s = _dedupe_mitcham_fast9_super7_lines(s)
    s = _strip_trailing_overs_and_competition_loop(s)
    if role == "mitcham":
        s = _strip_mitcham_numbered_shield_suffix(s)
        s = _strip_mitcham_space_grade_block(s)
        s = _dedupe_mitcham_fast9_super7_lines(s)
        s = _strip_trailing_overs_and_competition_loop(s)
        s = _strip_senior_mitcham_competition_suffix(s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _strip_amp_score_fragments(s)
    return s


def clean_team_name_for_display(name: str) -> str:
    """Backward-compatible alias: opponent-style cleanup."""
    return finalize_team_display_name(name, role="opponent")


def _context_indicates_womens_senior_competition(
    team_category: str,
    discovered_label: str,
    match_url: str,
    card: str,
    blob: str,
) -> bool:
    """True when the fixture is a women's senior comp (EGWC etc.), not junior girls."""
    if team_category == "senior_women":
        return True
    u = (match_url or "").lower().replace("_", "-")
    if "egwc" in u or "senior-women" in u or "seniorwomen" in u:
        return True
    hay = f"{discovered_label} {card} {blob}".lower()
    if "egwc" in hay and "women" in hay:
        return True
    if re.search(r"senior\s+women", hay):
        return True
    if re.search(r"(?i)women'?s?\s+[abc]\s+grade", hay):
        return True
    return False


def format_mitcham_team_for_match_display(
    cleaned_mitcham: str,
    *,
    team_category: str,
    discovered_label: str,
    match_url: str,
    card: str,
    blob: str,
) -> str:
    """
    Final Mitcham column: prefix women's senior sides as 'Mitcham Women - …' when needed.
    """
    base = (cleaned_mitcham or "").strip()
    if not base:
        base = "Mitcham"
    if not _context_indicates_womens_senior_competition(
        team_category, discovered_label, match_url, card, blob
    ):
        return base
    if re.match(r"(?i)^mitcham\s+women(\s+[-–]\s+|\s+)", base):
        return base
    if re.search(r"(?i)\bmitcham\s+women\b", base):
        return base
    return f"Mitcham Women - {base}"


def _resolve_match_row_fields(
    item: dict[str, Any],
    rep: Any,
) -> dict[str, Any]:
    """Combine fixture card, grade list context, and scorecard page metadata."""
    card = item["card"]
    status = item["status"]
    oc = item["oc"]
    card_m, card_o = parse_match_card_teams(card)

    blob = (getattr(rep, "raw_match_team_blob", None) or "").strip()
    vs_m, vs_o = _best_mitcham_opponent_pair_from_blob(blob)
    pair_m, pair_o = vs_m, vs_o
    scorecard_candidates = _clean_segments_for_scorecard_header_fallback(blob)
    scorecard_ordered = _ordered_scorecard_header_candidate_segments(scorecard_candidates)
    chosen_header_segment = ""
    header_parsed_pair_log = ""
    scorecard_header_pair = ""
    if pair_m is None and pair_o is None:
        for seg in scorecard_ordered:
            hm, ho = _parse_teams_from_scorecard_header_blob_validated(seg)
            if hm and ho:
                pair_m, pair_o = hm, ho
                chosen_header_segment = seg
                scorecard_header_pair = f"({hm!r}, {ho!r})"
                header_parsed_pair_log = scorecard_header_pair
                break
        if not header_parsed_pair_log and scorecard_ordered:
            header_parsed_pair_log = "(None, None)"

    disc = (item.get("discovered_team_label") or "").strip()
    mp = (getattr(rep, "mitcham_team_from_page", None) or "").strip()
    ms = (item.get("mitcham_side") or "").strip()

    fh_home = (getattr(rep, "fixture_header_home_team", None) or "").strip()
    fh_away = (getattr(rep, "fixture_header_away_team", None) or "").strip()
    fh_result = (getattr(rep, "fixture_header_result_text", None) or "").strip()
    fixture_teams_primary = False
    mitch: str | None = None
    opp: str | None = None
    if fh_home and fh_away:
        h, a = fh_home, fh_away
        hm_f, ha_f = _mitcham_in_string(h), _mitcham_in_string(a)
        if hm_f and not ha_f:
            mitch, opp, fixture_teams_primary = h, a, True
        elif ha_f and not hm_f:
            mitch, opp, fixture_teams_primary = a, h, True

    if not fixture_teams_primary:
        disc_mitcham_ok = bool(disc and _mitcham_in_string(disc))

        if disc and _mitcham_in_string(disc):
            mitch = disc
        elif mp and _mitcham_in_string(mp):
            mitch = mp
        else:
            rest = [
                x.strip()
                for x in (card_m, pair_m, ms)
                if x and _mitcham_in_string(str(x))
            ]
            mitch = max(rest, key=len) if rest else None

        if not mitch or len(mitch) <= len("Mitcham") + 1:
            lengthen_sources = (
                (card_m, mp, disc, ms)
                if disc_mitcham_ok
                else (pair_m, card_m, mp, disc, ms)
            )
            for x in lengthen_sources:
                if (
                    x
                    and _mitcham_in_string(str(x))
                    and len(str(x).strip()) > len(mitch or "")
                ):
                    mitch = str(x).strip()

        if not mitch:
            mitch = "Mitcham"

        if pair_o and not _mitcham_in_string(pair_o):
            opp = pair_o.strip()
        rp = getattr(rep, "opponent_from_scorecard", None)
        if (not opp or opp in ("—", "-")) and rp:
            rs = str(rp).strip()
            if rs and not _mitcham_in_string(rs):
                opp = rs
        if not opp or opp in ("—", "-"):
            for c in (card_o, item.get("opponent")):
                if (
                    c
                    and str(c).strip() not in ("—", "-", "")
                    and not _mitcham_in_string(str(c))
                ):
                    opp = str(c).strip()
                    break
        if not opp or opp in ("—", "-"):
            for seg in re.split(r"\s*\|\s*", blob):
                o = _opponent_from_match_title_blob(seg.strip())
                if o:
                    opp = o
                    break

        if not opp or opp in ("—", "-"):
            opp = "—"
        else:
            opp = str(opp).strip()
    else:
        mitch = str(mitch).strip()
        opp = str(opp).strip()

    mitch = _strip_amp_score_fragments(mitch)
    if opp not in ("—", "-"):
        opp = _strip_amp_score_fragments(opp)

    cur_o = opp if opp not in ("—", "-") else ""
    cur_q = _scorecard_pair_quality(mitch, cur_o) if cur_o else -5000
    if scorecard_ordered and not fixture_teams_primary:
        bm, bo = _best_validated_scorecard_pair_from_segments(scorecard_ordered)
        if bm and bo and _scorecard_pair_quality(bm, bo) > cur_q:
            mitch, opp = bm, bo
    if (not opp or opp in ("—", "-")) and not fixture_teams_primary:
        fo = _fallback_opponent_from_scorecard_segments(scorecard_ordered)
        if fo:
            opp = fo

    mitch = _strip_leaked_match_result_crap_from_team_name(mitch)
    if opp not in ("—", "-"):
        opp = _strip_leaked_match_result_crap_from_team_name(opp)

    resolved_mitcham_team = mitch
    resolved_opponent = opp
    cleaned_mitcham_team = finalize_team_display_name(mitch, role="mitcham")
    cleaned_opponent = (
        finalize_team_display_name(opp, role="opponent")
        if opp not in ("—", "-")
        else "—"
    )

    weak_mitcham_detected = _mitcham_team_is_weak(
        cleaned_mitcham_team
    ) or cleaned_opponent in ("—", "-")
    if weak_mitcham_detected and scorecard_ordered and not fixture_teams_primary:
        bm2, bo2 = _best_validated_scorecard_pair_from_segments(scorecard_ordered)
        if bm2 and bo2:
            q_new = _scorecard_pair_quality(bm2, bo2)
            q_old = _scorecard_pair_quality(
                cleaned_mitcham_team,
                cleaned_opponent if cleaned_opponent not in ("—", "-") else "",
            )
            if q_new > q_old:
                mitch = _strip_amp_score_fragments(
                    _strip_leaked_match_result_crap_from_team_name(bm2)
                )
                opp = _strip_amp_score_fragments(
                    _strip_leaked_match_result_crap_from_team_name(bo2)
                )
                resolved_mitcham_team = mitch
                resolved_opponent = opp
                cleaned_mitcham_team = finalize_team_display_name(mitch, role="mitcham")
                cleaned_opponent = (
                    finalize_team_display_name(opp, role="opponent")
                    if opp not in ("—", "-")
                    else "—"
                )
                weak_mitcham_detected = _mitcham_team_is_weak(
                    cleaned_mitcham_team
                ) or cleaned_opponent in ("—", "-")

    if disc:
        team_cat: TeamCategory = classify_team_label(disc)
    else:
        tc_raw = item.get("team_category")
        if tc_raw in ("junior", "senior_men", "senior_women"):
            team_cat = tc_raw  # type: ignore[assignment]
        elif tc_raw == "senior":
            team_cat = "senior_men"
        else:
            team_cat = "junior"

    junior_fast9_super7 = is_junior_fast9_or_super7_label(disc)

    final_mitcham_team = format_mitcham_team_for_match_display(
        cleaned_mitcham_team,
        team_category=team_cat,
        discovered_label=disc,
        match_url=str(item.get("match_url") or ""),
        card=card,
        blob=blob,
    )
    final_mitcham_team = finalize_team_display_name(final_mitcham_team, role="mitcham")
    final_opponent_team = cleaned_opponent

    page_lines = list(getattr(rep, "scorecard_result_lines", None) or [])
    result_from_fixture = None
    if fh_result and status == "Completed":
        result_from_fixture = normalize_fixture_header_result_to_compact(
            fh_result,
            final_mitcham_team,
            final_opponent_team if final_opponent_team not in ("—", "-") else None,
            oc,
        )
    if result_from_fixture:
        result = result_from_fixture
    else:
        result = normalize_match_result_display(
            status,
            oc,
            card,
            page_lines,
            mitch if mitch != "—" else None,
            opp if opp != "—" else None,
        )
        if result == "Completed" and status == "Completed":
            inferred2 = _infer_result_from_broad_blob(
                card,
                page_lines,
                oc,
                final_mitcham_team,
                final_opponent_team if final_opponent_team not in ("—", "-") else None,
            )
            if inferred2 and inferred2 != "Completed":
                result = inferred2
    raw_log = _raw_result_snippet_for_log(card, page_lines)
    parsed_pair = f"({vs_m!r}, {vs_o!r})"
    return {
        "mitcham_team": final_mitcham_team,
        "opponent": final_opponent_team,
        "team_category": team_cat,
        "resolved_mitcham_team": resolved_mitcham_team,
        "resolved_opponent": resolved_opponent,
        "cleaned_mitcham_team": cleaned_mitcham_team,
        "cleaned_opponent": cleaned_opponent,
        "final_mitcham_team": final_mitcham_team,
        "final_opponent": final_opponent_team,
        "junior_fast9_super7": junior_fast9_super7,
        "result": result,
        "raw_teams_log": (
            f"card=({card_m!r},{card_o!r}); "
            f"discovered_team_label={disc!r}; "
            f"mitcham_team_from_page={mp!r}; "
            f"parsed_pair={parsed_pair}; "
            f"page_blob={blob[:400]!r}"
        ),
        "raw_result_log": raw_log,
        "discovered_team_label": disc,
        "mitcham_team_from_page": mp,
        "raw_match_team_blob": blob,
        "parsed_pair": parsed_pair,
        "scorecard_header_pair": scorecard_header_pair,
        "scorecard_candidate_segments": scorecard_candidates,
        "scorecard_chosen_header_segment": chosen_header_segment,
        "scorecard_header_parsed_pair": header_parsed_pair_log,
        "weak_mitcham_detected": weak_mitcham_detected,
        "fixture_header_home_team": fh_home,
        "fixture_header_away_team": fh_away,
        "fixture_header_result_text": fh_result,
    }


def _facebook_mitcham_field_usable(mt: str) -> bool:
    t = (mt or "").strip()
    if not t:
        return False
    if len(t) > 200:
        return False
    low = t.lower()
    if "won by" in low or "lost by" in low:
        return False
    if " vs " in t and len(t) > 120:
        return False
    return True


def _facebook_opponent_field_usable(opp: str) -> bool:
    o = (opp or "").strip()
    if not o or o in ("—", "-"):
        return False
    if o.lower() == "opposition":
        return False
    return True


def _facebook_mitcham_won_result(result: str) -> bool | None:
    low = (result or "").lower().strip()
    if low.startswith("won by") or low == "won":
        return True
    if low.startswith("lost by") or low == "lost":
        return False
    return None


def _facebook_row_summary_line(
    row: dict[str, Any],
) -> tuple[str | None, str | None]:
    """Build one Facebook line from mitcham_team, opponent, result, status only."""
    mt = str(row.get("mitcham_team") or "").strip()
    opp = str(row.get("opponent") or "").strip()
    res = str(row.get("result") or "").strip()
    status = str(row.get("status") or "").strip()

    if not _facebook_mitcham_field_usable(mt):
        return None, "mitcham_unusable"

    ou = _facebook_opponent_field_usable(opp)
    won = _facebook_mitcham_won_result(res)
    res_l = res.lower()

    def res_is_vague() -> bool:
        return not res or res in ("—", "-") or res_l == "completed"

    if won is True:
        if ou:
            return f"{mt} d. {opp}", None
        if not res_is_vague():
            return f"{mt} — {res}", None
        return None, "win_no_opponent"

    if won is False:
        if ou:
            return f"{opp} d. {mt}", None
        if not res_is_vague():
            return f"{mt} — {res}", None
        return None, "loss_no_opponent"

    if "tie" in res_l or (
        status == "Completed" and ("draw" in res_l or "tie" in res_l)
    ):
        if not ou:
            return None, "tie_no_opponent"
        return f"{mt} vs {opp} — {res or 'Draw'}", None

    if status == "In progress":
        if ou:
            return f"{mt} vs {opp} — In progress", None
        return f"{mt} — In progress", None

    if not res_is_vague():
        if ou:
            return f"{mt} vs {opp} — {res}", None
        return f"{mt} — {res}", None

    if status == "Completed":
        if ou:
            return f"{mt} vs {opp}", None
        return None, "completed_bare"

    return None, "insufficient_fields"


def _log_match_results_row(
    url: str,
    resolved: dict[str, Any],
    status: str,
) -> None:
    opp = resolved.get("opponent") or "—"
    if opp == "—":
        pair_note = " opponent_unresolved"
    else:
        pair_note = ""
    cands = resolved.get("scorecard_candidate_segments") or []
    fb_line, fb_skip = _facebook_row_summary_line(resolved)
    msg = (
        f"[MatchResultsRow] url={url} "
        f"fixture_header_home_team={resolved.get('fixture_header_home_team')!r} "
        f"fixture_header_away_team={resolved.get('fixture_header_away_team')!r} "
        f"fixture_header_result_text={resolved.get('fixture_header_result_text')!r} "
        f"discovered_team_label={resolved.get('discovered_team_label')!r} "
        f"mitcham_team_from_page={resolved.get('mitcham_team_from_page')!r} "
        f"raw_match_team_blob={resolved.get('raw_match_team_blob')!r} "
        f"parsed_pair={resolved.get('parsed_pair')!r} "
        f"scorecard_candidate_segments={cands!r} "
        f"scorecard_chosen_header_segment={resolved.get('scorecard_chosen_header_segment')!r} "
        f"scorecard_header_parsed_pair={resolved.get('scorecard_header_parsed_pair')!r} "
        f"scorecard_header_fallback={resolved.get('scorecard_header_pair')!r} "
        f"team_category={resolved.get('team_category')!r} "
        f"junior_fast9_super7={resolved.get('junior_fast9_super7')!r} "
        f"weak_mitcham_detected={resolved.get('weak_mitcham_detected')!r} "
        f"resolved_mitcham_team={resolved.get('resolved_mitcham_team')!r} "
        f"resolved_opponent={resolved.get('resolved_opponent')!r} "
        f"cleaned_mitcham_team={resolved.get('cleaned_mitcham_team')!r} "
        f"cleaned_opponent={resolved.get('cleaned_opponent')!r} "
        f"final_mitcham_team={resolved.get('final_mitcham_team')!r} "
        f"final_opponent_team={resolved.get('final_opponent')!r} "
        f"raw_result={resolved.get('raw_result_log')!r} "
        f"normalized_result={resolved.get('result')!r} status={status!r}"
        f"{pair_note} "
        f"facebook_line={fb_line!r} facebook_skip_reason={fb_skip!r}"
    )
    logger.info(msg)
    try:
        tlog = Path(__file__).resolve().parent / "match_results_team_names.log"
        with tlog.open("a", encoding="utf-8") as fh:
            fh.write(
                f"url={url} "
                f"fixture_header_home_team={resolved.get('fixture_header_home_team')!r} "
                f"fixture_header_away_team={resolved.get('fixture_header_away_team')!r} "
                f"fixture_header_result_text={resolved.get('fixture_header_result_text')!r} "
                f"discovered_team_label={resolved.get('discovered_team_label')!r} "
                f"team_category={resolved.get('team_category')!r} "
                f"junior_fast9_super7={resolved.get('junior_fast9_super7')!r} "
                f"weak_mitcham_detected={resolved.get('weak_mitcham_detected')!r} "
                f"resolved_mitcham_team={resolved.get('resolved_mitcham_team')!r} "
                f"resolved_opponent={resolved.get('resolved_opponent')!r} "
                f"cleaned_mitcham_team={resolved.get('cleaned_mitcham_team')!r} "
                f"cleaned_opponent={resolved.get('cleaned_opponent')!r} "
                f"final_mitcham_team={resolved.get('final_mitcham_team')!r} "
                f"final_opponent={resolved.get('final_opponent')!r}\n"
            )
    except OSError:
        pass
    if _env_truthy("MITCHAM_MATCH_RESULTS_DEBUG"):
        try:
            logf = Path(__file__).resolve().parent / "match_results_metadata.log"
            with logf.open("a", encoding="utf-8") as fh:
                fh.write(msg + "\n")
        except Exception:
            pass


def default_season_choices(today: date | None = None) -> list[str]:
    """Approximate PlayCricket 'Summer YYYY/YY' labels for current and previous season."""
    t = today or date.today()
    y, m = t.year, t.month
    if m >= 9:
        cur_start = y
    else:
        cur_start = y - 1
    def label(start: int) -> str:
        return f"Summer {start}/{str(start + 1)[2:]}"
    return [label(cur_start), label(cur_start - 1)]


def build_summary_sentence(
    wins: int,
    losses: int,
    draws: int,
    in_progress: int,
    completed: int,
    *,
    scope: str = "junior",
) -> str:
    finished = wins + losses + draws
    if finished > 0:
        rate = wins / finished
        if rate >= 0.55:
            tone = "a good"
        elif rate >= 0.35:
            tone = "a decent"
        else:
            tone = "a challenging"
    else:
        tone = "an"
    if scope == "senior":
        who = "Mitcham senior sides had"
    elif scope == "both":
        who = "Mitcham teams (juniors and seniors) had"
    else:
        who = "Mitcham juniors had"
    return (
        f"{who} {tone} outing in the selected period with results showing "
        f"{wins} wins, {losses} losses, {draws} draws/ties and {in_progress} games in progress."
    )


def _col_index(headers: list[str], *needles: str) -> int | None:
    for i, h in enumerate(headers):
        hl = h.lower()
        if all(n.lower() in hl for n in needles):
            return i
    for i, h in enumerate(headers):
        hl = h.lower()
        if any(n.lower() in hl for n in needles):
            return i
    return None


def _parse_int_runs(s: str) -> int | None:
    s = (s or "").strip().replace("*", "")
    if s == "" or s == "-":
        return None
    if re.fullmatch(r"\d+", s):
        return int(s)
    return None


def parse_batting_table(rows: list[list[str]]) -> list[BattingRow]:
    """Batting rows from scorecard table; dismissal column sets not_out."""
    if not rows:
        return []
    for start in range(min(10, len(rows))):
        header = rows[start]
        ir = _col_index(header, "runs")
        ib = _col_index(header, "balls")
        if ir is None or ib is None:
            continue
        out: list[BattingRow] = []
        for cells in rows[start + 1 :]:
            if len(cells) <= max(ir, ib):
                continue
            name = (cells[0] or "").strip()
            low = name.lower()
            if not name or low == "batting":
                continue
            if low == "extras" or low.startswith("total"):
                continue
            if "did not bat" in low:
                continue
            dismissal = (cells[1] if len(cells) > 1 else "") or ""
            rn = _parse_int_runs(cells[ir])
            bl = _parse_int_runs(cells[ib])
            if rn is None and bl is not None:
                rn = 0
            if rn is None or bl is None:
                continue
            clean_name = name.split("\n")[0].strip()
            if not clean_name:
                continue
            not_out = _dismissal_not_out(dismissal)
            out.append(BattingRow(player=clean_name, runs=rn, balls=bl, not_out=not_out))
        if out:
            return out
    return []


def _safe_int(s: str) -> int | None:
    t = (s or "").strip()
    if not t or t == "-":
        return None
    try:
        return int(t)
    except ValueError:
        return None


def _normalize_header_cell(h: str) -> str:
    return re.sub(r"\s+", " ", (h or "").strip()).lower()


def _is_wides_header(h: str) -> bool:
    """True for Wides / Wd — must not be treated as the Wickets column."""
    hl = _normalize_header_cell(h)
    if "wicket" in hl:
        return False
    if "wide" in hl:
        return True
    return hl.strip() in ("wd", "wds")


def _find_bowling_player_column(header: list[str]) -> int:
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if hl in ("bowler", "player", "name"):
            return i
        if "bowler" in hl:
            return i
        if hl == "player" or (hl.startswith("player") and len(hl) < 24):
            return i
    return 0


def _find_runs_conceded_column(header: list[str]) -> int | None:
    """
    Runs Conceded (full label) or short 'R' — not Dot Balls, Wides, Maidens, Overs.
    """
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "dot ball" in hl or hl.strip() in ("db",):
            continue
        if "runs conceded" in hl or "runs conc" in hl:
            return i
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "dot ball" in hl or "maiden" in hl or _is_wides_header(h):
            continue
        if "over" in hl and "bowl" in hl:
            continue
        if hl.strip() == "r":
            return i
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "dot ball" in hl or "maiden" in hl or _is_wides_header(h):
            continue
        if hl in ("runs", "r's", "rs"):
            return i
    return None


def _find_wickets_column(header: list[str]) -> int | None:
    """
    Wickets (full label) or 'W' — not Wides / Wd / Maidens.
    """
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "maiden" in hl:
            continue
        if _is_wides_header(h):
            continue
        if "wicket" in hl:
            return i
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "maiden" in hl or _is_wides_header(h):
            continue
        if hl.strip() == "w":
            return i
    for i, h in enumerate(header):
        hl = _normalize_header_cell(h)
        if "maiden" in hl or _is_wides_header(h):
            continue
        if hl in ("wkts", "wk", "wkt"):
            return i
    return None


def _bowling_column_indices(header: list[str]) -> tuple[int, int, int] | None:
    """
    Map bowling header row to (player_name_idx, wickets_idx, runs_conceded_idx).
    Uses header text only (Play Cricket: Overs O, Maidens M, Runs Conceded R, Wickets W, …).
    """
    if not header:
        return None
    player_i = _find_bowling_player_column(header)
    r_i = _find_runs_conceded_column(header)
    w_i = _find_wickets_column(header)
    if r_i is None or w_i is None or r_i == w_i:
        return None
    return (player_i, w_i, r_i)


def _bowling_stat_token(s: str) -> bool:
    """True if cell looks like a bowling stat (integer or decimal)."""
    t = (s or "").strip()
    if not t or t == "-":
        return False
    return bool(re.match(r"^-?\d+(\.\d+)?$", t))


def _normalize_bowling_data_row(cells: list[str], header: list[str]) -> list[str]:
    """
    Play Cricket sometimes duplicates the first bowling metric (overs) after the name.
    Align data rows to len(header) before column mapping.
    """
    if len(cells) < 3 or len(header) < 2:
        return list(cells)
    expected = len(header)
    a = (cells[1] or "").strip()
    b = (cells[2] or "").strip()
    if a != b or not _bowling_stat_token(a):
        return list(cells)
    # Avoid O=0, M=0: two zeros are different columns, not duplicate overs.
    if a in ("0", "0.0", "0.00"):
        return list(cells)
    fixed = [cells[0]] + cells[2:]
    while len(fixed) < expected:
        fixed.append("")
    return fixed[:expected]


def _normalize_wickets_runs_tuple(
    t: tuple[str, int, int],
) -> tuple[str, int, int]:
    """
    Correct rare column swaps where runs and wickets are reversed (e.g. 9/1 vs 1/9).
    """
    name, w, rc = t
    if (w >= 9 and rc <= 1 and w > rc) or (w >= 8 and rc == 0 and w > rc):
        return (name, rc, w)
    return t


def _parse_bowling_row_mapped(
    cells: list[str],
    player_i: int,
    w_i: int,
    r_i: int,
) -> tuple[str, int, int] | None:
    """One bowling row using header-mapped columns; skip malformed rows."""
    need = max(player_i, w_i, r_i) + 1
    if len(cells) < need:
        return None
    name = (cells[player_i] or "").strip().split("\n")[0].strip()
    low = name.lower()
    if not name or low in ("bowling", "bowler", "extras"):
        return None
    if low.startswith("total") or low == "total":
        return None
    wkts = _safe_int(cells[w_i])
    runs = _safe_int(cells[r_i])
    if wkts is None:
        wkts = 0
    if runs is None:
        runs = 0
    if wkts < 0 or wkts > 12 or runs < 0 or runs > 999:
        return None
    return _normalize_wickets_runs_tuple((name, wkts, runs))


def _parse_bowling_table_with_meta(
    rows: list[list[str]],
) -> tuple[
    list[tuple[str, int, int]],
    list[str] | None,
    tuple[int, int, int] | None,
    list[list[str]],
    list[list[str]],
]:
    """
    Parse bowling table; return (rows, header, indices, first raw data rows,
    normalized rows for best header, up to 20 for logging).
    Canonical tuple: (player_name, wickets, runs_conceded).
    """
    if not rows:
        return [], None, None, [], []
    best: list[tuple[str, int, int]] = []
    best_header: list[str] | None = None
    best_idx: tuple[int, int, int] | None = None
    best_raw_data: list[list[str]] = []
    best_norm_data: list[list[str]] = []
    for start in range(min(10, len(rows))):
        header = rows[start]
        idx = _bowling_column_indices(header)
        if idx is None:
            continue
        pi, wi, ri = idx
        out: list[tuple[str, int, int]] = []
        for cells in rows[start + 1 :]:
            try:
                norm = _normalize_bowling_data_row(cells, header)
                parsed = _parse_bowling_row_mapped(norm, pi, wi, ri)
                if parsed:
                    out.append(parsed)
            except Exception:
                continue
        if len(out) > len(best):
            best = out
            best_header = header
            best_idx = idx
            best_raw_data = rows[start + 1 : start + 21]
            best_norm_data = [
                _normalize_bowling_data_row(c, header)
                for c in best_raw_data
            ]
    return best, best_header, best_idx, best_raw_data, best_norm_data


def parse_bowling_table(rows: list[list[str]]) -> list[tuple[str, int, int]]:
    """Return (player_name, wickets, runs_conceded) using header column mapping."""
    parsed, _, _, _, _ = _parse_bowling_table_with_meta(rows)
    return parsed


def _scrape_tables(page: Page) -> list[list[list[str]]]:
    return page.evaluate(
        """() => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          return [...document.querySelectorAll('table')].map((t) =>
            [...t.querySelectorAll('tr')].map((tr) =>
              [...tr.querySelectorAll('td, th')].map((c) => norm(c.innerText))
            )
          );
        }"""
    )


def _wait_for_scorecard_content(page: Page, timeout_ms: int = 25_000) -> bool:
    """Wait for scorecard body: batting/bowling text, table/grid rows, or innings UI."""
    deadline = time.perf_counter() + timeout_ms / 1000.0
    while time.perf_counter() < deadline:
        ok = page.evaluate(
            """() => {
          const t = (document.body && document.body.innerText) || '';
          const low = t.toLowerCase();
          const hasBat = low.includes('batting') || low.includes('batsman');
          const hasBowl = low.includes('bowling');
          const hasInn = low.includes('innings') || low.includes('fall of');
          const nTr = document.querySelectorAll('table tr').length;
          const nRow = document.querySelectorAll('[role="row"]').length;
          const nTab = document.querySelectorAll('[role="tab"]').length;
          const nTbl = document.querySelectorAll(
            'table, [role="table"], [role="grid"]'
          ).length;
          if (nTbl > 0 && (nTr + nRow) > 2) return true;
          if (hasBat && (nTr + nRow) > 1) return true;
          if (hasBat && hasBowl) return true;
          if (hasInn && (nTr + nRow) > 0) return true;
          if (nTab > 2 && (hasBat || hasBowl)) return true;
          return false;
        }"""
        )
        if ok:
            return True
        page.wait_for_timeout(220)
    return False


def _scorecard_dom_probe(page: Page) -> dict[str, Any]:
    """Snapshot of visible controls and scorecard-related DOM (for debugging)."""
    return page.evaluate(
        r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const vis = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 1 && r.height > 1 && r.bottom > -400 && r.top < window.innerHeight + 600;
          };
          const btnTexts = [];
          for (const el of document.querySelectorAll('button, [role="button"]')) {
            if (!vis(el)) continue;
            const t = norm(el.innerText);
            if (t && t.length < 200) btnTexts.push(t);
            if (btnTexts.length >= 20) break;
          }
          const tabTexts = [];
          for (const el of document.querySelectorAll('[role="tab"], [role="radio"]')) {
            if (!vis(el)) continue;
            const t = norm(el.innerText);
            if (t && t.length < 200) tabTexts.push(t);
            if (tabTexts.length >= 20) break;
          }
          const nearHeadings = [];
          const scoreEl = [...document.querySelectorAll('h1,h2,h3,h4,*')].find((e) => {
            const x = norm(e.innerText).toLowerCase();
            return x.includes('scorecard') && x.length < 120;
          });
          const root = scoreEl && scoreEl.parentElement ? scoreEl.parentElement : document.body;
          for (const el of root.querySelectorAll('h1,h2,h3,h4,h5,h6,[role="heading"],strong')) {
            if (!vis(el)) continue;
            const t = norm(el.innerText);
            if (t && t.length > 3 && t.length < 220) nearHeadings.push(t);
            if (nearHeadings.length >= 20) break;
          }
          const kw = ['batting', 'bowling', 'scorecard', 'innings', 'fall of wickets', 'overs'];
          const kwCounts = {};
          for (const k of kw) {
            let n = 0;
            const kl = k.toLowerCase();
            for (const el of document.querySelectorAll('*')) {
              try {
                if (!vis(el)) continue;
                const it = (el.innerText || '').toLowerCase();
                if (it.includes(kl)) n++;
              } catch (e) {}
            }
            kwCounts[k] = n;
          }
          let nTables = 0;
          let nRows = 0;
          for (const el of document.querySelectorAll('table, [role="table"], [role="grid"]')) {
            if (vis(el)) nTables++;
          }
          for (const el of document.querySelectorAll('table tr, [role="row"]')) {
            if (vis(el)) nRows++;
          }
          return {
            pageUrl: location.href || '',
            pageTitle: document.title || '',
            buttonTexts: btnTexts,
            tabTexts: tabTexts,
            nearScorecardHeadings: nearHeadings,
            nVisibleTables: nTables,
            nVisibleRows: nRows,
            keywordElementCounts: kwCounts,
          };
        }"""
    )


def _log_scorecard_dom_probe(match_url: str, probe: dict[str, Any]) -> None:
    purl = probe.get("pageUrl") or match_url
    title = probe.get("pageTitle") or ""
    bt = probe.get("buttonTexts") or []
    tt = probe.get("tabTexts") or []
    hd = probe.get("nearScorecardHeadings") or []
    kwc = probe.get("keywordElementCounts") or {}
    msg = (
        f"DOM_PROBE url={purl} | title={title!r} | "
        f"buttons={bt!r} | tabs={tt!r} | headings_near_scorecard={hd!r} | "
        f"n_tables={probe.get('nVisibleTables')} | n_rows={probe.get('nVisibleRows')} | "
        f"kw_counts={kwc!r}"
    )
    _scorecard_extract_log(msg)


def _snapshot_scorecard_dom_when_empty(
    page: Page, match_url: str, rep: ScorecardExtractReport
) -> None:
    """Log truncated HTML/text around scorecard when extraction found nothing."""
    try:
        snap = page.evaluate(
            r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const pick = [...document.querySelectorAll('main, article, [class*="scorecard" i], [class*="Scorecard"], [class*="match" i], section')].find(
            (e) => {
              const t = (e.innerText || '').toLowerCase();
              return t.includes('batting') || t.includes('bowling') || t.includes('innings');
            }
          );
          const el = pick || document.body;
          const text = norm(el.innerText || '').slice(0, 8000);
          const html = (el.innerHTML || '').slice(0, 12000);
          return { text, html };
        }"""
        )
    except Exception as e:
        _scorecard_extract_log(f"SNAPSHOT_ERR url={match_url} err={e!r}")
        return
    if not isinstance(snap, dict):
        return
    tx = (snap.get("text") or "")[:4000]
    hx = (snap.get("html") or "")[:6000]
    _scorecard_extract_log(
        f"SCORECARD_SNAPSHOT url={match_url} text_preview={tx!r} html_preview={hx!r}"
    )
    if _env_truthy("MITCHAM_SCORECARD_DEBUG"):
        try:
            logf = Path(__file__).resolve().parent / "scorecard_dom_snapshot.log"
            with logf.open("a", encoding="utf-8") as fh:
                fh.write(f"\n--- {match_url} ---\n{snap.get('text', '')}\n")
        except Exception:
            pass


def _scrape_scorecard_row_matrices(page: Page) -> list[list[list[str]]]:
    """
    Tables and ARIA table/grid structures (Play Cricket often uses role=grid).
    Rows below the fold must still be collected: do not filter by viewport position.
    """
    raw: Any = page.evaluate(
        """() => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const rootOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 1 && r.height > 1;
          };
          const rowOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 0 && r.height > 0;
          };
          const out = [];
          const seen = new Set();
          const roots = [];
          for (const sel of ['table', '[role="table"]', '[role="grid"]']) {
            for (const el of document.querySelectorAll(sel)) {
              if (rootOk(el)) roots.push(el);
            }
          }
          for (const root of roots) {
            if (seen.has(root)) continue;
            seen.add(root);
            const rows = [];
            for (const tr of root.querySelectorAll('tr, [role="row"]')) {
              if (!rowOk(tr)) continue;
              const cells = [...tr.querySelectorAll(
                'td, th, [role="cell"], [role="gridcell"]'
              )].map((c) => norm(c.innerText));
              if (cells.length && cells.some((x) => x)) rows.push(cells);
            }
            if (rows.length) out.push(rows);
          }
          return out;
        }"""
    )
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, list) and x]


def _scrape_div_like_row_matrices(page: Page) -> list[list[list[str]]]:
    """
    Fallback: grid/flex rows inside scorecard-like containers (no <table>).
    """
    raw: Any = page.evaluate(
        """() => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const hostOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 2 && r.height > 2;
          };
          const rowOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 0 && r.height > 0;
          };
          const kidOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 0 && r.height > 0;
          };
          const hosts = [];
          for (const el of document.querySelectorAll(
            '[class*="scorecard" i], [class*="Scorecard"], [class*="match-detail" i], [class*="MatchScore"], [data-testid*="scorecard" i]'
          )) {
            if (hostOk(el)) hosts.push(el);
          }
          if (!hosts.length) hosts.push(document.body);
          const out = [];
          for (const host of hosts) {
            for (const row of host.querySelectorAll(
              '[class*="Row" i][class*="grid" i], [class*="grid-row" i], [data-testid*="row" i]'
            )) {
              if (!rowOk(row)) continue;
              const kids = [...row.children].filter(kidOk);
              if (kids.length < 4) continue;
              const cells = kids.map((c) => norm(c.innerText));
              if (!cells.some((x) => x)) continue;
              const joined = cells.join(' ').toLowerCase();
              if (!joined.includes('batting') && !joined.includes('bowling') && !/\\d/.test(joined)) continue;
              out.push(cells);
            }
          }
          if (out.length >= 3) return [out];
          return [];
        }"""
    )
    if not isinstance(raw, list) or not raw:
        return []
    return [x for x in raw if isinstance(x, list) and x]


def _scroll_scorecard_tables_into_view(page: Page) -> None:
    """Nudge off-screen table rows into layout so cells can be read reliably."""
    try:
        page.evaluate(
            """() => {
          let n = 0;
          for (const el of document.querySelectorAll(
            'table tr, [role="grid"] tr, [role="table"] [role="row"]'
          )) {
            if (n++ > 500) break;
            try { el.scrollIntoView({ block: 'nearest' }); } catch (e) {}
          }
        }"""
        )
        page.wait_for_timeout(120)
    except Exception:
        pass


def _scrape_bowling_tables_all_rows(page: Page) -> list[list[list[str]]]:
    """
    Extra pass: tables whose early rows look like bowling, without viewport filtering.
    Catches edge cases where the primary scrape still misses rows.
    """
    raw: Any = page.evaluate(
        """() => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const rowOk = (el) => {
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
          };
          const out = [];
          for (const root of document.querySelectorAll('table, [role="table"], [role="grid"]')) {
            const st = window.getComputedStyle(root);
            if (st.visibility === 'hidden' || st.display === 'none') continue;
            const rows = [];
            for (const tr of root.querySelectorAll('tr, [role="row"]')) {
              if (!rowOk(tr)) continue;
              const cells = [...tr.querySelectorAll(
                'td, th, [role="cell"], [role="gridcell"]'
              )].map((c) => norm(c.innerText));
              if (cells.length && cells.some((x) => x)) rows.push(cells);
            }
            if (rows.length < 3) continue;
            const blob = rows.slice(0, 8).map((r) => r.join(' ')).join(' ').toLowerCase();
            if (!blob.includes('bowling')) continue;
            if (!blob.includes('wicket')) continue;
            if (!blob.includes('runs') && !blob.includes('conc')) continue;
            out.push(rows);
          }
          return out;
        }"""
    )
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, list) and x]


def _all_scorecard_matrices(page: Page) -> list[list[list[str]]]:
    _scroll_scorecard_tables_into_view(page)
    mats = _scrape_scorecard_row_matrices(page)
    if not mats:
        mats.extend(_scrape_div_like_row_matrices(page))
    else:
        extra = _scrape_div_like_row_matrices(page)
        for m in extra:
            if m not in mats:
                mats.append(m)
    for m in _scrape_bowling_tables_all_rows(page):
        if m not in mats:
            mats.append(m)
    return mats


def _scorecard_heading_sections(page: Page) -> list[dict[str, Any]]:
    """Batting/bowling blocks anchored by nearby headings + innings context."""
    raw: Any = page.evaluate(
        r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const vis = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 2 && r.height > 2;
          };
          const rowOk = (el) => {
            const r = el.getBoundingClientRect();
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') return false;
            return r.width > 0 && r.height > 0;
          };
          const inningsHint = (el) => {
            let p = el;
            for (let i = 0; i < 14 && p; i++) {
              const t = norm(p.innerText || '');
              if (/\d+(st|nd|rd|th)\s+\S+/i.test(t) && t.length < 180) return t.slice(0, 160);
              p = p.parentElement;
            }
            return '';
          };
          const sections = [];
          const heads = document.querySelectorAll(
            'h1, h2, h3, h4, h5, h6, [role="heading"], strong, [class*="Heading"], [class*="heading"], [class*="title"]'
          );
          for (const h of heads) {
            if (!vis(h)) continue;
            const txt = norm(h.innerText);
            if (!txt || txt.length > 220) continue;
            const low = txt.toLowerCase();
            if (!low.includes('batting') && !low.includes('bowling')) continue;
            let table = null;
            let el = h.nextElementSibling;
            for (let i = 0; i < 18 && !table; i++) {
              if (!el) break;
              if (el.matches && el.matches('table, [role="table"], [role="grid"]')) {
                table = el;
                break;
              }
              const inner = el.querySelector && el.querySelector('table, [role="table"], [role="grid"]');
              if (inner) {
                table = inner;
                break;
              }
              el = el.nextElementSibling;
            }
            if (!table) continue;
            const rows = [];
            for (const tr of table.querySelectorAll('tr, [role="row"]')) {
              if (!rowOk(tr)) continue;
              const cells = [...tr.querySelectorAll(
                'td, th, [role="cell"], [role="gridcell"]'
              )].map((c) => norm(c.innerText));
              if (cells.some((x) => x)) rows.push(cells);
            }
            if (!rows.length) continue;
            let sectionContext = '';
            let p = h.parentElement;
            for (let i = 0; i < 6 && p; i++) {
              const t = norm(p.innerText || '').slice(0, 400);
              if (t.length > 30) {
                sectionContext = t;
                break;
              }
              p = p.parentElement;
            }
            sections.push({
              heading: txt,
              sectionContext: sectionContext,
              inningsHint: inningsHint(h),
              kind: low.includes('bowling') ? 'bowling' : 'batting',
              rows: rows,
            });
          }
          return sections;
        }"""
    )
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


def _dedupe_batting_rows(rows: list[BattingRow]) -> list[BattingRow]:
    seen: set[tuple[str, int, int, bool]] = set()
    out: list[BattingRow] = []
    for r in rows:
        k = (r.player.lower(), r.runs, r.balls, r.not_out)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _dedupe_bowling_rows(
    rows: list[tuple[str, int, int]],
) -> list[tuple[str, int, int]]:
    seen: set[tuple[str, int, int]] = set()
    out: list[tuple[str, int, int]] = []
    for t in rows:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _extend_batting_from_matrices(
    matrices: list[list[list[str]]], acc: list[BattingRow]
) -> int:
    n = 0
    for tbl in matrices:
        if not tbl:
            continue
        if "batting" not in _table_text_blob(tbl, 16):
            continue
        hit = False
        for off in range(min(10, len(tbl))):
            sub = tbl[off:]
            rows = parse_batting_table(sub)
            if rows:
                acc.extend(rows)
                hit = True
                break
        if hit:
            n += 1
    return n


def _extend_bowling_from_matrices(
    matrices: list[list[list[str]]], acc: list[tuple[str, int, int]]
) -> tuple[int, dict[str, Any] | None]:
    """
    Append all bowling rows from each bowling table (best slice per table).
    Returns (tables_hit, debug) where debug describes the largest single-table parse.
    """
    n = 0
    dbg_best: dict[str, Any] | None = None
    for tbl in matrices:
        if not tbl:
            continue
        if "bowling" not in _table_text_blob(tbl, 18):
            continue
        best: list[tuple[str, int, int]] = []
        best_header: list[str] | None = None
        best_idx: tuple[int, int, int] | None = None
        best_raw: list[list[str]] = []
        best_norm: list[list[str]] = []
        for off in range(min(10, len(tbl))):
            sub = tbl[off:]
            rows, hdr, ix, raw, norm = _parse_bowling_table_with_meta(sub)
            if len(rows) > len(best):
                best = rows
                best_header = hdr
                best_idx = ix
                best_raw = raw
                best_norm = norm
        if best:
            acc.extend(best)
            n += 1
            if best_header is not None and best_idx is not None:
                cand = {
                    "n_rows": len(best),
                    "header": best_header,
                    "indices": best_idx,
                    "raw_first20": best_raw[:20],
                    "norm_first20": best_norm[:20],
                    "expected_cols": len(best_header),
                }
                if dbg_best is None or cand["n_rows"] > dbg_best["n_rows"]:
                    dbg_best = cand
    return n, dbg_best


def _parse_flat_scorecard_by_headings(
    page: Page,
    all_bat: list[BattingRow],
    all_bowl: list[tuple[str, int, int]],
) -> tuple[int, int]:
    """When innings chips are absent: use heading + inningsHint to classify sections."""
    secs = _scorecard_heading_sections(page)
    bat_tables = 0
    bowl_tables = 0
    bowling_secs = [
        s for s in secs if (s.get("kind") or "").lower() == "bowling"
    ]
    for sec in secs:
        kind = (sec.get("kind") or "").lower()
        rows = sec.get("rows") or []
        if not rows or not isinstance(rows, list):
            continue
        hint = (
            (sec.get("inningsHint") or "")
            + " "
            + (sec.get("heading") or "")
            + " "
            + (sec.get("sectionContext") or "")
        )
        if kind == "batting":
            if _innings_is_mitcham(hint) or _innings_is_mitcham(
                (sec.get("sectionContext") or "")[:200]
            ):
                br = parse_batting_table(rows)
                if br:
                    all_bat.extend(br)
                    bat_tables += 1
        elif kind == "bowling":
            # Mitcham bowling: opponent batted this innings — hint is not Mitcham's batting innings.
            h = hint.strip()
            if h and _innings_is_mitcham(h):
                continue
            if not h and len(bowling_secs) != 1:
                continue
            bw = parse_bowling_table(rows)
            if bw:
                all_bowl.extend(bw)
                bowl_tables += 1
    return bat_tables, bowl_tables


def _parse_scorecard_full_page_matrices_mitcham_batting(
    page: Page,
    all_bat: list[BattingRow],
) -> int:
    """
    Last resort: batting matrices whose text blob suggests Mitcham's innings.
    (Bowling is handled via heading-based sections — avoid guessing from grids alone.)
    """
    matrices = _all_scorecard_matrices(page)
    bat_n = 0
    for tbl in matrices:
        blob = _table_text_blob(tbl, min(24, len(tbl)))
        low = blob.lower()
        if "batting" not in low:
            continue
        if not (
            "mitcham" in low
            or re.search(r"\d+\s*(st|nd|rd|th)\s+mit\b", low)
        ):
            continue
        hit = False
        for off in range(min(10, len(tbl))):
            sub = tbl[off:]
            br = parse_batting_table(sub)
            if br:
                all_bat.extend(br)
                hit = True
                break
        if hit:
            bat_n += 1
    return bat_n


def _batting_table_fingerprint(tables: list[list[list[str]]]) -> str:
    """Cheap signature of batting table data rows for wait-for-update (header may not be row 0)."""
    for tbl in tables:
        if not tbl or "batting" not in _table_text_blob(tbl, 12):
            continue
        header_off: int | None = None
        for off in range(min(8, len(tbl))):
            ir = _col_index(tbl[off], "runs")
            ib = _col_index(tbl[off], "balls")
            if ir is not None and ib is not None:
                header_off = off
                break
        if header_off is None:
            continue
        parts: list[str] = []
        for row in tbl[header_off + 1 : header_off + 6]:
            if not row or not row[0]:
                continue
            low = (row[0] or "").strip().lower()
            if low in ("extras", "batting") or low.startswith("total"):
                continue
            parts.append((row[0] or "")[:40])
        return "|".join(parts)
    return ""


def _discover_innings_toggle_labels(page: Page) -> list[str]:
    """
    Find innings selector labels, e.g. '1st MIT 6-154', '1st BUL 9-166', '2nd ROW'.
    Skips parents that concatenate multiple innings into one string.
    """
    raw: list[str] = page.evaluate(
        r"""() => {
          const re = /^\d+(st|nd|rd|th)\s+\S+/i;
          const seen = new Set();
          const out = [];
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          for (const el of document.querySelectorAll(
            'div, button, a, span, [role="tab"], [role="radio"]'
          )) {
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') continue;
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1) continue;
            if (r.bottom < -200 || r.top > innerHeight + 600) continue;
            const t = norm(el.innerText || el.getAttribute('aria-label') || '');
            if (!re.test(t) || t.length > 100) continue;
            const ordinals = t.match(/\d+(st|nd|rd|th)/gi) || [];
            if (ordinals.length !== 1) continue;
            if (/^(Play|Login|Menu|Cancel|Scorecard|Ball by ball)/i.test(t)) continue;
            if (!seen.has(t)) {
              seen.add(t);
              out.push(t);
            }
          }
          return out;
        }"""
    )
    # Prefer longer, specific labels (e.g. '1st MIT 6-154') over bare '1st MIT' when both exist
    out: list[str] = []
    for t in raw:
        if not re.match(
            r"(?i)^\d+(?:st|nd|rd|th)\s+\S+(\s+[\d\-/&]+)?\s*$",
            t,
        ):
            continue
        if t not in out:
            out.append(t)

    out = _dedupe_prefer_longer_innings_labels(out)

    def sort_key(lab: str) -> tuple[int, str]:
        m = re.match(r"(?i)^(\d+)(?:st|nd|rd|th)\s+", lab)
        if not m:
            return (999, lab)
        return (int(m.group(1)), lab)

    out.sort(key=sort_key)
    return out


def _discover_innings_toggle_labels_broad(page: Page) -> list[str]:
    """More permissive innings chip detection (aria-label, visible layout, score-in-label)."""
    raw: list[str] = page.evaluate(
        r"""() => {
          const seen = new Set();
          const out = [];
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          for (const el of document.querySelectorAll(
            'button, a, [role="tab"], [role="radio"], label, div, span'
          )) {
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') continue;
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1) continue;
            if (r.bottom < -200 || r.top > innerHeight + 600) continue;
            const t = norm(el.innerText || el.getAttribute('aria-label') || '');
            if (t.length < 4 || t.length > 120) continue;
            if (!/\d+(st|nd|rd|th)\s+\S/i.test(t)) continue;
            if (/^(Play|Login|Menu|Cancel|Scorecard|Ball by ball|Close)/i.test(t)) continue;
            if (!seen.has(t)) {
              seen.add(t);
              out.push(t);
            }
          }
          return out;
        }"""
    )
    out: list[str] = []
    for t in raw:
        if not re.match(
            r"(?i)^\d+(?:st|nd|rd|th)\s+\S+(\s+[\d\-/&]+)?\s*$",
            t,
        ):
            continue
        if t not in out:
            out.append(t)
    out = _dedupe_prefer_longer_innings_labels(out)

    def sort_key(lab: str) -> tuple[int, str]:
        m = re.match(r"(?i)^(\d+)(?:st|nd|rd|th)\s+", lab)
        if not m:
            return (999, lab)
        return (int(m.group(1)), lab)

    out.sort(key=sort_key)
    return out


def _discover_innings_score_chips(page: Page) -> list[str]:
    """
    Chip-style innings selectors without ordinals, e.g. MIT 5-120, WAR 6-110.
    Patterns: TEAM wkts-runs or TEAM wkts/runs (2–4 letter team codes).
    """
    raw: list[str] = page.evaluate(
        r"""() => {
          const seen = new Set();
          const out = [];
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const reChip = /^[A-Z]{2,4}\s+\d+\s*[-/]\s*\d+$/i;
          for (const el of document.querySelectorAll(
            'button, a, [role="tab"], [role="radio"], label, div, span'
          )) {
            const st = window.getComputedStyle(el);
            if (st.visibility === 'hidden' || st.display === 'none') continue;
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1) continue;
            if (r.bottom < -200 || r.top > innerHeight + 600) continue;
            const t = norm(el.innerText || el.getAttribute('aria-label') || '');
            if (t.length < 4 || t.length > 40) continue;
            if (!reChip.test(t)) continue;
            if (/^(Play|Login|Menu|Cancel|Scorecard|Ball by ball|Close)/i.test(t)) continue;
            if (!seen.has(t)) {
              seen.add(t);
              out.push(t);
            }
          }
          return out;
        }"""
    )
    out: list[str] = []
    for t in raw:
        if not _SCORE_CHIP_LABEL_RE.match(t.strip()):
            continue
        if t not in out:
            out.append(t)
    out.sort(key=lambda lab: lab.upper())
    return out


def _merge_innings_toggle_labels(*lists: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for lst in lists:
        for t in lst:
            t = t.strip()
            if not t or t in seen:
                continue
            seen.add(t)
            out.append(t)
    out = _dedupe_prefer_longer_innings_labels(out)

    def sort_key(lab: str) -> tuple[int, str]:
        m = re.match(r"(?i)^(\d+)(?:st|nd|rd|th)\s+", lab)
        if not m:
            return (999, lab)
        return (int(m.group(1)), lab)

    out.sort(key=sort_key)
    return out


def _dedupe_prefer_longer_innings_labels(labels: list[str]) -> list[str]:
    """If both '1st MIT' and '1st MIT 6-154' exist, keep the longer label for clicking."""
    drop: set[str] = set()
    for a in labels:
        for b in labels:
            if a != b and b.startswith(a) and len(b) > len(a):
                drop.add(a)
                break
    return [lab for lab in labels if lab not in drop]


def _innings_team_token(label: str) -> str | None:
    m = re.match(r"(?i)^\d+(?:st|nd|rd|th)\s+(\S+)", (label or "").strip())
    if not m:
        return None
    return re.sub(r"[^\w/]", "", m.group(1)).upper()


_SCORE_CHIP_LABEL_RE = re.compile(
    r"^(?i)(?P<code>[A-Z]{2,4})\s+(?P<a>\d+)\s*[-/]\s*(?P<b>\d+)\s*$"
)


def _score_chip_team_code(label: str) -> str | None:
    """Team abbreviation from chip labels like MIT 5-120 or WAR 6/110."""
    m = _SCORE_CHIP_LABEL_RE.match((label or "").strip())
    if not m:
        return None
    return m.group("code").upper()


def _mittokens_include_mitcham(tok: str | None) -> bool:
    if not tok:
        return False
    if tok == "MIT":
        return True
    for part in tok.replace("\\", "/").split("/"):
        if part == "MIT":
            return True
    return False


def _innings_is_mitcham(label: str) -> bool:
    """Mitcham batting innings: ordinal labels, or score chips (e.g. MIT 5-120)."""
    s = (label or "").strip()
    if not s:
        return False
    low = s.lower()
    chip_code = _score_chip_team_code(s)
    if chip_code is not None:
        if "MIT" in chip_code:
            return True
        return _mittokens_include_mitcham(chip_code)
    tok = _innings_team_token(s)
    if _mittokens_include_mitcham(tok):
        return True
    if "mitcham" in low:
        return True
    return False


_SEASON_LABEL_LINE_RE = re.compile(r"^Summer\s+\d{4}/\d{2}")


def _ordered_dedupe_season_labels(labels: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in labels:
        t = re.sub(r"\s+", " ", (raw or "").strip())
        if not t or t in seen:
            continue
        if not _SEASON_LABEL_LINE_RE.match(t):
            continue
        seen.add(t)
        out.append(t)
    return out


def discover_season_labels_from_page(page: Page) -> list[str]:
    """Read season labels from the Play Cricket club Teams tab dropdown (simple path)."""
    t0 = time.perf_counter()
    page.goto(f"{CLUB_PAGE}?tab=teams", wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(900)
    wrap = page.locator(".o-dropdown__select-wrapper").filter(
        has_text=re.compile(r"Summer\s+20")
    ).first
    wrap.wait_for(state="visible", timeout=15_000)
    _open_club_season_dropdown(page)
    page.wait_for_timeout(220)
    raw: list[Any] = page.evaluate(
        r"""() => {
          const out = [];
          const re = /^Summer\s+\d{4}\/\d{2}/;
          for (const el of document.querySelectorAll(
            '[role="option"], button, a, li, span, div'
          )) {
            const t = (el.innerText || '').replace(/\s+/g, ' ').trim();
            if (re.test(t) && t.length < 80) out.push(t);
          }
          return out;
        }"""
    )
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    page.wait_for_timeout(200)

    def season_sort_key(s: str) -> int:
        m = re.search(r"Summer\s+(\d{4})/", s)
        return -int(m.group(1)) if m else 0

    deduped = _ordered_dedupe_season_labels([str(x) for x in (raw or [])])
    labels = sorted(deduped, key=season_sort_key)
    logger.info("Season discovery: count=%d labels=%s", len(labels), labels)
    _perf("season label discovery", t0)
    return labels


def discover_all_season_labels(headless: bool | None = None) -> list[str]:
    """Standalone browser session: all seasons (for Streamlit cache / first load)."""
    debug = _debug_browser_season()
    if debug:
        headless = False
    elif headless is None:
        headless = True
    slow_mo = 250 if debug else 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=slow_mo)
        page = browser.new_page()
        try:
            return discover_season_labels_from_page(page)
        finally:
            browser.close()


def _click_innings_toggle(page: Page, label: str) -> None:
    """
    Innings chips are often implemented as <label> + <div> sharing the same innerText;
    get_by_text(exact=True) can time out. Prefer clicking a visible label/control.
    """
    before = _batting_table_fingerprint(_all_scorecard_matrices(page))
    clicked = page.evaluate(
        """([want]) => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const order = (el) => (el.tagName === 'LABEL' ? 0 : el.tagName === 'BUTTON' ? 1 : 2);
          const candidates = [];
          for (const el of document.querySelectorAll(
            'label, button, a, [role="tab"], [role="radio"], div'
          )) {
            if (norm(el.innerText) !== want) continue;
            const r = el.getBoundingClientRect();
            if (r.width < 2 || r.height < 2) continue;
            candidates.push(el);
          }
          candidates.sort((a, b) => order(a) - order(b));
          if (!candidates.length) return false;
          candidates[0].click();
          return true;
        }""",
        [label],
    )
    if not clicked:
        page.locator("label").filter(
            has_text=re.compile("^" + re.escape(label) + "$")
        ).first.click(timeout=8000)
    page.wait_for_timeout(220)
    try:
        page.wait_for_function(
            """() => {
          const t = (document.body && document.body.innerText) || '';
          const low = t.toLowerCase();
          const n = document.querySelectorAll('table tr, [role="row"]').length;
          return n > 1 || low.includes('batting') || low.includes('bowling');
        }""",
            timeout=12_000,
        )
    except Exception:
        page.wait_for_timeout(400)
    for _ in range(14):
        page.wait_for_timeout(70)
        after = _batting_table_fingerprint(_all_scorecard_matrices(page))
        if after and after != before:
            return
        if not before and after:
            return
    page.wait_for_timeout(250)


def _scorecard_extract_log(msg: str) -> None:
    logger.info("[ScorecardExtract] %s", msg)
    if not _env_truthy("MITCHAM_SCORECARD_DEBUG"):
        return
    try:
        logf = Path(__file__).resolve().parent / "scorecard_extraction.log"
        with logf.open("a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except Exception:
        pass


def _table_text_blob(tbl: list[list[str]], max_rows: int = 12) -> str:
    return " ".join((c or "").lower() for row in tbl[:max_rows] for c in row)


@dataclass
class ScorecardExtractReport:
    match_url: str
    scorecard_reached: bool
    scorecard_tab_clicked: bool = False
    toggles: list[str] = field(default_factory=list)
    innings_chips_detected: list[str] = field(default_factory=list)
    mitcham_innings_chips: list[str] = field(default_factory=list)
    opposition_innings_chips: list[str] = field(default_factory=list)
    mitcham_batting_tabs_used: list[str] = field(default_factory=list)
    opposition_innings_for_bowling: list[str] = field(default_factory=list)
    opponent_from_scorecard: str | None = None
    mitcham_team_from_page: str | None = None
    fixture_header_home_team: str = ""
    fixture_header_away_team: str = ""
    fixture_header_result_text: str = ""
    raw_match_team_blob: str = ""
    scorecard_result_lines: list[str] = field(default_factory=list)
    batting_tables_parsed: int = 0
    bowling_tables_parsed: int = 0
    batting_rows: int = 0
    bowling_rows: int = 0
    note: str = ""
    bowling_header_row: list[str] | None = None
    bowling_column_indices: tuple[int, int, int] | None = None
    bowling_expected_columns: int | None = None
    bowling_raw_rows_first20: list[list[str]] = field(default_factory=list)
    bowling_norm_rows_first20: list[list[str]] = field(default_factory=list)


def _log_scorecard_report(rep: ScorecardExtractReport) -> None:
    _scorecard_extract_log(
        " | ".join(
            [
                f"url={rep.match_url}",
                f"page_opened={rep.scorecard_reached}",
                f"scorecard_tab={rep.scorecard_tab_clicked}",
                f"innings_toggles={rep.toggles!r}",
                f"mitcham_batting_tabs={rep.mitcham_batting_tabs_used!r}",
                f"opposition_batting_tabs={rep.opposition_innings_for_bowling!r}",
                f"batting_tables={rep.batting_tables_parsed}",
                f"bowling_tables={rep.bowling_tables_parsed}",
                f"batting_rows={rep.batting_rows}",
                f"bowling_rows={rep.bowling_rows}",
            ]
            + ([f"note={rep.note}"] if rep.note else [])
        )
    )


def scrape_match_scorecard(
    page: Page,
    match_url: str,
    match_date: date | None = None,
) -> tuple[list[BattingRow], list[tuple[str, int, int]], ScorecardExtractReport]:
    """
    Mitcham batting from Mitcham-labelled innings; Mitcham bowling from opposition
    innings. Falls back to heading-based and full-page matrix parsing when chips
    are absent or tables use role=grid / non-table layouts.
    """
    rep = ScorecardExtractReport(match_url=match_url, scorecard_reached=False)
    all_bat: list[BattingRow] = []
    all_bowl: list[tuple[str, int, int]] = []
    bat_tables_total = 0
    bowl_tables_total = 0

    try:
        page.goto(match_url, wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_timeout(500)
        rep.scorecard_reached = True
    except Exception as e:
        rep.note = f"goto_failed:{e!r}"
        return all_bat, all_bowl, rep

    try:
        page.get_by_text("Scorecard", exact=True).first.click(timeout=4000)
        page.wait_for_timeout(200)
        rep.scorecard_tab_clicked = True
    except Exception:
        pass

    ready = _wait_for_scorecard_content(page)
    if not ready:
        rep.note = (rep.note + " scorecard_content_wait_timeout;").strip()
    page.wait_for_timeout(260)

    try:
        fh_meta = _extract_fixture_header_metadata(page)
        rep.fixture_header_home_team = fh_meta.get("homeTeam") or ""
        rep.fixture_header_away_team = fh_meta.get("awayTeam") or ""
        rep.fixture_header_result_text = fh_meta.get("resultText") or ""
    except Exception:
        pass

    try:
        probe = _scorecard_dom_probe(page)
        _log_scorecard_dom_probe(match_url, probe)
    except Exception as e:
        rep.note = (rep.note + f" dom_probe_err:{e!r};").strip()

    toggles = _merge_innings_toggle_labels(
        _discover_innings_toggle_labels(page),
        _discover_innings_toggle_labels_broad(page),
        _discover_innings_score_chips(page),
    )
    if not toggles:
        toggles = _fallback_short_innings_labels(page)
    rep.toggles = list(toggles)

    mitcham_tabs = [t for t in toggles if _innings_is_mitcham(t)]
    opp_tabs = [t for t in toggles if not _innings_is_mitcham(t)]
    rep.innings_chips_detected = list(toggles)
    rep.mitcham_innings_chips = list(mitcham_tabs)
    rep.opposition_innings_chips = list(opp_tabs)

    if toggles:
        for lab in mitcham_tabs:
            try:
                _click_innings_toggle(page, lab)
                rep.mitcham_batting_tabs_used.append(lab)
                matrices = _all_scorecard_matrices(page)
                bat_tables_total += _extend_batting_from_matrices(matrices, all_bat)
            except Exception as e:
                rep.note = (rep.note + f" bat_tab_err({lab}):{e!r};").strip()

        bowling_dbg_best: dict[str, Any] | None = None
        for lab in opp_tabs:
            try:
                _click_innings_toggle(page, lab)
                rep.opposition_innings_for_bowling.append(lab)
                matrices = _all_scorecard_matrices(page)
                hit, bdbg = _extend_bowling_from_matrices(matrices, all_bowl)
                bowl_tables_total += hit
                if bdbg is not None and (
                    bowling_dbg_best is None
                    or bdbg["n_rows"] > bowling_dbg_best["n_rows"]
                ):
                    bowling_dbg_best = bdbg
            except Exception as e:
                rep.note = (rep.note + f" bowl_tab_err({lab}):{e!r};").strip()
        if bowling_dbg_best is not None:
            rep.bowling_header_row = bowling_dbg_best["header"]
            rep.bowling_column_indices = bowling_dbg_best["indices"]
            rep.bowling_expected_columns = bowling_dbg_best.get("expected_cols")
            rep.bowling_raw_rows_first20 = bowling_dbg_best.get(
                "raw_first20", []
            )
            rep.bowling_norm_rows_first20 = bowling_dbg_best.get(
                "norm_first20", []
            )

    if (not toggles) or (not all_bat and not all_bowl):
        hb, hbw = _parse_flat_scorecard_by_headings(page, all_bat, all_bowl)
        bat_tables_total += hb
        bowl_tables_total += hbw

    if not all_bat:
        bat_tables_total += _parse_scorecard_full_page_matrices_mitcham_batting(
            page, all_bat
        )

    all_bat[:] = _dedupe_batting_rows(all_bat)
    all_bowl[:] = _dedupe_bowling_rows(all_bowl)

    rep.batting_tables_parsed = bat_tables_total
    rep.bowling_tables_parsed = bowl_tables_total
    rep.batting_rows = len(all_bat)
    rep.bowling_rows = len(all_bowl)

    if (
        not rep.toggles
        and rep.batting_tables_parsed == 0
        and rep.bowling_tables_parsed == 0
    ):
        try:
            _snapshot_scorecard_dom_when_empty(page, match_url, rep)
        except Exception:
            pass

    try:
        meta = _extract_match_page_metadata(page)
        rep.raw_match_team_blob = (meta.get("blob") or "").strip()
        rep.scorecard_result_lines = list(meta.get("resultLines") or [])
        rep.mitcham_team_from_page = meta.get("mitcham_from_blob")
        rep.opponent_from_scorecard = meta.get("opponent_from_blob")
    except Exception:
        pass

    return all_bat, all_bowl, rep


def _fallback_short_innings_labels(page: Page) -> list[str]:
    """If score-in-label toggles are absent, use legacy short labels (e.g. 1st MIT)."""
    loc = page.get_by_text(re.compile(r"^\d+(st|nd|rd|th)\s+[A-Z0-9/]+$"))
    n = loc.count()
    labels: list[str] = []
    for i in range(n):
        t = loc.nth(i).inner_text().strip()
        if t and t not in labels:
            labels.append(t)

    def sort_key(lab: str) -> tuple[int, str]:
        m = re.match(r"(?i)^(\d+)(st|nd|rd|th)\s+(\S+)", lab)
        if not m:
            return (0, lab)
        try:
            inn = int(m.group(1))
        except ValueError:
            inn = 0
        return (inn, lab)

    labels.sort(key=sort_key)
    return labels


@dataclass
class TeamRef:
    label: str
    grade_url: str
    team_id: str


def _top_batting_highlights(rows: list[BattingRow], n: int = 2) -> str:
    if not rows:
        return "—"
    rows_sorted = sorted(rows, key=lambda r: (-r.runs, r.balls))
    return "; ".join(format_batting_display(r) for r in rows_sorted[:n])


def _top_bowling_highlights(bowls: list[tuple[str, int, int]], n: int = 2) -> str:
    if not bowls:
        return "—"
    s = sorted(bowls, key=lambda x: (-x[1], x[2]))
    return "; ".join(format_bowling_display(a, b, c) for a, b, c in s[:n])


def _better_bowl(
    current: tuple[str, int, int] | None, name: str, w: int, r: int
) -> tuple[str, int, int] | None:
    if current is None:
        return (name, w, r)
    _, aw, ar = current
    if w > aw or (w == aw and r < ar):
        return (name, w, r)
    return current


def _open_club_season_dropdown(page: Page) -> None:
    """
    Open the PlayCricket club season control. Prefer the real trigger button so the
    options panel becomes visible (avoids clicking duplicate hidden option text).
    """
    wrap = page.locator(".o-dropdown__select-wrapper").filter(
        has_text=re.compile(r"Summer\s+20")
    ).first
    trig = page.locator("#organisation-seasons").first
    try:
        if trig.count() > 0:
            trig.click(timeout=12_000)
        else:
            wrap.locator("button.o-dropdown__trigger").first.click(timeout=12_000)
    except Exception:
        wrap.click(timeout=12_000)
    page.wait_for_timeout(280)
    try:
        page.wait_for_function(
            """() => {
          const b = document.querySelector('#organisation-seasons');
          if (b && b.getAttribute('aria-expanded') === 'true') return true;
          const p = document.querySelector('#organisation-seasons-options');
          if (!p) return false;
          const st = window.getComputedStyle(p);
          return st.display !== 'none' && st.visibility !== 'hidden';
        }""",
            timeout=8_000,
        )
    except Exception:
        page.wait_for_timeout(400)


def _click_season_option_via_js(page: Page, season_label: str) -> bool:
    """Click first visible element in the options panel whose text equals season_label."""
    return bool(
        page.evaluate(
            """(want) => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const roots = document.querySelectorAll(
            '#organisation-seasons-options, #organisation-seasons-options-list, .o-dropdown__options-wrapper'
          );
          for (const root of roots) {
            for (const el of root.querySelectorAll(
              '[role="option"], button, a, li, span, div, label'
            )) {
              if (norm(el.innerText) !== want) continue;
              const r = el.getBoundingClientRect();
              const st = window.getComputedStyle(el);
              if (st.visibility === 'hidden' || st.display === 'none') continue;
              if (r.width < 2 || r.height < 2) continue;
              el.click();
              return true;
            }
          }
          return false;
        }""",
            season_label,
        )
    )


def _click_season_in_open_dropdown(page: Page, season_label: str) -> None:
    """
    Click the season row inside the open dropdown only (avoids strict-mode duplicate
    matches from page-wide get_by_text, which often resolves to a hidden list span).
    """
    logger.info("[MitchamStats] _select_season: requested=%r", season_label)
    attempts: list[tuple[str, Any]] = [
        ("#organisation-seasons-options", page.locator("#organisation-seasons-options")),
        ("#organisation-seasons-options-list", page.locator("#organisation-seasons-options-list")),
        (".o-dropdown__options-wrapper", page.locator(".o-dropdown__options-wrapper").first),
        ('[role="listbox"]', page.locator('[role="listbox"]').first),
        (
            "[data-radix-popper-content-wrapper]",
            page.locator("[data-radix-popper-content-wrapper]").first,
        ),
        (".o-dropdown__menu", page.locator(".o-dropdown__menu").first),
    ]
    for desc, container in attempts:
        try:
            container.wait_for(state="visible", timeout=5_000)
        except Exception:
            continue
        opt = container.get_by_text(season_label, exact=True)
        try:
            if opt.count() == 0:
                continue
            o0 = opt.first
            try:
                o0.scroll_into_view_if_needed(timeout=3_000)
            except Exception:
                pass
            o0.click(timeout=10_000)
        except Exception:
            continue
        logger.info(
            "[MitchamStats] _select_season: clicked ok container=%r for %r",
            desc,
            season_label,
        )
        return
    if _click_season_option_via_js(page, season_label):
        logger.info(
            "[MitchamStats] _select_season: clicked ok container=%r for %r",
            "js_visible_option",
            season_label,
        )
        return
    raise TimeoutError(
        f"Could not select season {season_label!r}: dropdown option not visible or not found"
    )


def _select_season(page: Page, season_label: str) -> None:
    """Open club Teams tab and choose season from PlayCricket dropdown (controls team list)."""
    page.goto(f"{CLUB_PAGE}?tab=teams", wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(900)
    wrap = page.locator(".o-dropdown__select-wrapper").filter(
        has_text=re.compile(r"Summer\s+20")
    ).first
    try:
        cur = re.sub(r"\s+", " ", wrap.inner_text(timeout=8000).strip())
    except Exception:
        cur = ""
    if season_label in cur:
        return
    _open_club_season_dropdown(page)
    _click_season_in_open_dropdown(page, season_label)
    page.wait_for_timeout(2000)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=60_000)
    except Exception:
        pass


def discover_teams_from_page(page: Page) -> list[TeamRef]:
    """Read team links from the current club Teams tab (after season is selected)."""
    page.wait_for_timeout(350)
    raw_rows: list[dict[str, str]] = page.evaluate(
        r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const rows = [];
          for (const a of document.querySelectorAll('a[href*="/grade/"]')) {
            const href = a.getAttribute('href') || '';
            if (!href.includes('teamId=') && !href.includes('teamid=')) continue;
            rows.push({ href, label: norm(a.innerText) });
          }
          return rows;
        }"""
    )
    if not isinstance(raw_rows, list):
        raw_rows = []
    seen: set[tuple[str, str]] = set()
    teams: list[TeamRef] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        href = (row.get("href") or "").strip()
        label = re.sub(r"\s+", " ", (row.get("label") or "").strip())
        if not href:
            continue
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        tid_list = qs.get("teamId") or qs.get("teamid")
        if not tid_list:
            continue
        team_id = tid_list[0]
        base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        key = (base, team_id)
        if key in seen:
            continue
        seen.add(key)
        grade_url = f"{base}?teamId={team_id}&tab=matches"
        teams.append(TeamRef(label=label, grade_url=grade_url, team_id=team_id))
    return teams


def list_matches_for_team(page: Page, team: TeamRef) -> list[tuple[str, str]]:
    """Return (match_url, card_inner_text) for completed and in-progress listings."""
    page.goto(team.grade_url, wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(650)
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass
    page.wait_for_timeout(450)
    # Snapshot links in one evaluate — avoids count()+nth(i) timeouts when the DOM
    # updates or count() overshoots visible / stable nodes.
    raw_rows: list[dict[str, str]] = page.evaluate(
        r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const rows = [];
          for (const a of document.querySelectorAll('a[href*="/match/"]')) {
            let href = a.getAttribute('href') || '';
            if (!href || href.includes('livestreams=true')) continue;
            const i = href.indexOf('?');
            if (i >= 0) href = href.slice(0, i);
            rows.push({ href, text: norm(a.innerText) });
          }
          return rows;
        }"""
    )
    if not isinstance(raw_rows, list):
        raw_rows = []
    out: list[tuple[str, str]] = []
    seen_url: set[str] = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        href = (row.get("href") or "").strip()
        text = (row.get("text") or "").strip()
        if "livestreams=true" in href:
            continue
        m = re.search(r"/match/([a-f0-9-]+)/", href, flags=re.I)
        if not m:
            continue
        uid = m.group(1)
        clean = f"https://play.cricket.com.au/match/{uid}/"
        if clean in seen_url:
            continue
        seen_url.add(clean)
        upper = text.upper()
        if not any(k in upper for k in ("COMPLETED", "IN PROGRESS", "LIVE")):
            continue
        out.append((href, text))
    return out


def _log_bowling_match_extraction(
    rep: ScorecardExtractReport,
    bowls: list[tuple[str, int, int]],
    min_wickets: int,
) -> None:
    """Console/file debug for one match's bowling parse (completed matches)."""
    idx = rep.bowling_column_indices
    pi = wi = ri = None
    if idx is not None:
        pi, wi, ri = idx
    msg = (
        f"[BowlingExtraction] url={rep.match_url} "
        f"innings_chips_detected={rep.innings_chips_detected!r} "
        f"mitcham_innings_selected={rep.mitcham_batting_tabs_used!r} "
        f"opposition_innings_selected={rep.opposition_innings_for_bowling!r} "
        f"mitcham_chips={rep.mitcham_innings_chips!r} "
        f"opposition_chips={rep.opposition_innings_chips!r} "
        f"bowling_header={rep.bowling_header_row!r} "
        f"expected_bowling_columns={rep.bowling_expected_columns!r} "
        f"player_name_i={pi} wickets_i={wi} runs_conceded_i={ri} "
        f"raw_rows_first20={rep.bowling_raw_rows_first20!r} "
        f"norm_rows_first20={rep.bowling_norm_rows_first20!r} "
        f"parsed_first20={bowls[:20]!r} total_bowling_rows={len(bowls)} "
        f"min_wickets={min_wickets}"
    )
    logger.info(msg)
    if _env_truthy("MITCHAM_BOWLING_EXTRACTION_DEBUG"):
        try:
            logf = Path(__file__).resolve().parent / "bowling_extraction.log"
            with logf.open("a", encoding="utf-8") as fh:
                fh.write(msg + "\n")
        except Exception:
            pass


def _log_fetch_scope(payload: dict[str, Any]) -> None:
    msg = " | ".join(f"{k}={v!r}" for k, v in payload.items())
    logger.info("[FetchScope] %s", msg)
    try:
        logf = Path(__file__).resolve().parent / "mitcham_fetch_scope.log"
        with logf.open("a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except OSError:
        pass


def run_report(
    season_label: str,
    date_from: date,
    date_to: date,
    min_runs: int = 20,
    min_wickets: int = 1,
    headless: bool = True,
    *,
    include_juniors: bool = True,
    include_seniors: bool = False,
    teams_cache: dict[str, list[TeamRef]] | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if not include_juniors and not include_seniors:
        raise ValueError("Select at least one of juniors or seniors.")

    d_from = min(date_from, date_to)
    d_to = max(date_from, date_to)
    fetch_scope_key = (
        f"{season_label}|{d_from.isoformat()}|{d_to.isoformat()}"
        f"|{int(include_juniors)}|{int(include_seniors)}"
    )
    logger.info(
        "[FetchScopeReset] season=%r date_from=%r date_to=%r juniors_checked=%s "
        "seniors_checked=%s fetch_scope_key=%r accumulators=batting_highlights,"
        "bowling_highlights,match_rows,all_bowling_agg (fresh per run)",
        season_label,
        d_from.isoformat(),
        d_to.isoformat(),
        include_juniors,
        include_seniors,
        fetch_scope_key,
    )
    batting_highlights: list[dict[str, Any]] = []
    bowling_highlights: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    wins = losses = draws = in_progress = 0
    best_bowl: tuple[str, int, int] | None = None
    agg_bat_rows = 0
    agg_bowl_rows = 0
    agg_bat_pass_min = 0
    agg_bowl_pass_min = 0
    all_bowling_agg: list[tuple[str, int, int, str, str]] = []

    def prog(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)

    cache = teams_cache if teams_cache is not None else {}
    cache_key = season_label
    t_total = time.perf_counter()
    selected_teams: list[TeamRef] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            t0 = time.perf_counter()
            if cache_key in cache:
                all_teams = cache[cache_key]
                prog("Using cached club teams for this season…")
                logger.info(
                    "Team list: cache hit key=%r (%d teams)",
                    cache_key,
                    len(all_teams),
                )
            else:
                prog("Selecting season and loading club teams…")
                _select_season(page, season_label)
                all_teams = discover_teams_from_page(page)
                cache[cache_key] = all_teams
                logger.info(
                    "Team list: cached %d teams for season key=%r",
                    len(all_teams),
                    cache_key,
                )
            _perf("team discovery", t0)

            n_jr_all = sum(1 for t in all_teams if classify_team_label(t.label) == "junior")
            n_srm_all = sum(
                1 for t in all_teams if classify_team_label(t.label) == "senior_men"
            )
            n_srw_all = sum(
                1 for t in all_teams if classify_team_label(t.label) == "senior_women"
            )

            selected_teams = teams_for_scope(
                all_teams,
                include_juniors=include_juniors,
                include_seniors=include_seniors,
            )
            n_jr_sel = sum(1 for t in selected_teams if classify_team_label(t.label) == "junior")
            n_srm_sel = sum(
                1 for t in selected_teams if classify_team_label(t.label) == "senior_men"
            )
            n_srw_sel = sum(
                1 for t in selected_teams if classify_team_label(t.label) == "senior_women"
            )

            _log_fetch_scope(
                {
                    "season": season_label,
                    "cache_key": cache_key,
                    "juniors_checked": include_juniors,
                    "seniors_checked": include_seniors,
                    "junior_teams_found": n_jr_all,
                    "senior_men_teams_found": n_srm_all,
                    "senior_women_teams_found": n_srw_all,
                    "junior_teams_selected": n_jr_sel,
                    "senior_men_teams_selected": n_srm_sel,
                    "senior_women_teams_selected": n_srw_sel,
                    "all_teams_cached": len(all_teams),
                }
            )

            by_url: dict[str, dict[str, Any]] = {}
            per_team_raw_matches: dict[str, int] = {}
            t1 = time.perf_counter()
            prog(f"Scanning fixtures ({len(selected_teams)} teams)…")
            for attempt in range(3):
                for team in selected_teams:
                    tc = classify_team_label(team.label)
                    pairs = list_matches_for_team(page, team)
                    if attempt == 0:
                        per_team_raw_matches[team.label] = len(pairs)
                        logger.info(
                            "[TeamFixtureDiscovery] current_team_label=%r "
                            "current_team_category=%r raw_matches_found=%d",
                            team.label,
                            tc,
                            len(pairs),
                        )
                    for match_url, card in pairs:
                        md = _parse_first_match_date(card)
                        if md is None or md < d_from or md > d_to:
                            continue
                        if match_url in by_url:
                            continue
                        status = match_status_from_card(card)
                        oc = outcome_from_card(card)
                        mitcham_side, opponent = parse_match_card_teams(card)
                        by_url[match_url] = {
                            "match_url": match_url,
                            "card": card,
                            "md": md,
                            "status": status,
                            "oc": oc,
                            "mitcham_side": mitcham_side,
                            "opponent": opponent,
                            "discovered_team_label": team.label,
                            "team_category": tc,
                        }
                logger.info(
                    "match_count_attempt_%d=%d",
                    attempt + 1,
                    len(by_url),
                )
                if attempt < 2:
                    page.wait_for_timeout(650)
            logger.info("final_match_count=%d", len(by_url))
            _perf("match discovery (date-filtered)", t1)

            ordered = sorted(
                by_url.values(),
                key=lambda x: (x["md"] or date.min, x["match_url"]),
            )
            n_m = len(ordered)
            n_matches_junior = sum(
                1 for x in ordered if x.get("team_category") == "junior"
            )
            n_matches_senior_men = sum(
                1 for x in ordered if x.get("team_category") == "senior_men"
            )
            n_matches_senior_women = sum(
                1 for x in ordered if x.get("team_category") == "senior_women"
            )
            _log_fetch_scope(
                {
                    "season": season_label,
                    "cache_key": cache_key,
                    "total_matches_fetched": n_m,
                    "final_match_count": n_m,
                    "match_discovery_attempts": 3,
                    "matches_junior": n_matches_junior,
                    "matches_senior_men": n_matches_senior_men,
                    "matches_senior_women": n_matches_senior_women,
                }
            )
            prog(f"Loading {n_m} scorecards…")
            t2 = time.perf_counter()
            team_accepted: dict[str, int] = defaultdict(int)
            team_rejected: dict[str, int] = defaultdict(int)
            for i, item in enumerate(ordered):
                match_url = item["match_url"]
                md: date = item["md"]
                status = item["status"]
                oc = item["oc"]
                disc_lab = item.get("discovered_team_label") or ""

                bats, bowls, _rep = scrape_match_scorecard(page, match_url, md)
                resolved = _resolve_match_row_fields(item, _rep)
                ok, rej = is_valid_mitcham_match(
                    _rep, resolved, item.get("card") or ""
                )
                _log_match_validation(
                    match_url, disc_lab, _rep, resolved, ok, rej
                )
                if not ok:
                    team_rejected[disc_lab] += 1
                    if n_m and (i + 1) % max(1, n_m // 8) == 0:
                        prog(f"Scorecards {i + 1}/{n_m}…")
                    continue

                team_accepted[disc_lab] += 1

                if status == "Completed":
                    if oc == "win":
                        wins += 1
                    elif oc == "loss":
                        losses += 1
                    elif oc == "draw":
                        draws += 1
                elif status == "In progress":
                    in_progress += 1

                if status == "Completed":
                    _log_scorecard_report(_rep)
                    logger.info(
                        "[ScorecardInningsChips] detected=%r mitcham=%r opposition=%r",
                        _rep.innings_chips_detected,
                        _rep.mitcham_innings_chips,
                        _rep.opposition_innings_chips,
                    )
                    _log_bowling_match_extraction(_rep, bowls, min_wickets)
                agg_bat_rows += len(bats)
                agg_bowl_rows += len(bowls)
                agg_bat_pass_min += sum(1 for br in bats if br.runs >= min_runs)
                ds = md.isoformat() if md else ""
                for br in bats:
                    if br.runs >= min_runs:
                        batting_highlights.append(
                            {
                                "date": ds,
                                "player": br.player,
                                "runs": br.runs,
                                "balls": br.balls,
                                "not_out": br.not_out,
                                "formatted": format_batting_display(br),
                                "match_url": match_url,
                            }
                        )
                for name, wkts, runs_con in bowls:
                    all_bowling_agg.append(
                        (name, wkts, runs_con, ds, match_url)
                    )

                match_rows.append(
                    {
                        "date": ds,
                        "mitcham_team": resolved["mitcham_team"],
                        "opponent": resolved["opponent"],
                        "status": status,
                        "result": resolved["result"],
                        "match_url": match_url,
                    }
                )
                _log_match_results_row(match_url, resolved, status)
                if n_m and (i + 1) % max(1, n_m // 8) == 0:
                    prog(f"Scorecards {i + 1}/{n_m}…")

            for team in selected_teams:
                lab = team.label
                logger.info(
                    "[TeamFixtureDiscovery] summary current_team_label=%r "
                    "raw_matches_found=%d accepted_matches=%d "
                    "rejected_non_mitcham_matches=%d",
                    lab,
                    per_team_raw_matches.get(lab, 0),
                    team_accepted.get(lab, 0),
                    team_rejected.get(lab, 0),
                )
            agg_bowl_pass_min = sum(1 for t in all_bowling_agg if t[1] >= min_wickets)
            _perf("scorecard parsing (total)", t2)
            logger.info(
                "[ScorecardAggregate] total_batting_rows=%d total_bowling_rows=%d "
                "batting_rows_runs_ge_%d=%d bowling_rows_wickets_ge_%d=%d",
                agg_bat_rows,
                agg_bowl_rows,
                min_runs,
                agg_bat_pass_min,
                min_wickets,
                agg_bowl_pass_min,
            )
            _scorecard_extract_log(
                "AGGREGATE | "
                f"total_batting_rows={agg_bat_rows} total_bowling_rows={agg_bowl_rows} "
                f"batting_pass_min_runs_{min_runs}={agg_bat_pass_min} "
                f"bowling_pass_min_wickets_{min_wickets}={agg_bowl_pass_min}"
            )
        finally:
            browser.close()

    bowling_highlights = [
        {
            "date": t[3],
            "player": t[0],
            "wickets": t[1],
            "runs_conceded": t[2],
            "formatted": format_bowling_display(t[0], t[1], t[2]),
            "match_url": t[4],
        }
        for t in all_bowling_agg
        if t[1] >= min_wickets
    ]
    for t in all_bowling_agg:
        if t[1] >= min_wickets:
            best_bowl = _better_bowl(best_bowl, t[0], t[1], t[2])
    _filt20 = [x for x in all_bowling_agg if x[1] >= min_wickets][:20]
    _disp20 = [
        format_bowling_display(name, w, r)
        for name, w, r, _ds, _u in _filt20
    ]
    logger.info(
        "[BowlingAggregate] total_bowling_rows=%d min_wickets=%s "
        "first20_agg=%r first20_after_filter=%r display_strings=%r",
        len(all_bowling_agg),
        min_wickets,
        all_bowling_agg[:20],
        _filt20,
        _disp20,
    )
    if _env_truthy("MITCHAM_BOWLING_EXTRACTION_DEBUG"):
        try:
            logf = Path(__file__).resolve().parent / "bowling_extraction.log"
            with logf.open("a", encoding="utf-8") as fh:
                fh.write(
                    f"[BowlingAggregate] total={len(all_bowling_agg)} "
                    f"filtered={agg_bowl_pass_min} min_wk={min_wickets}\n"
                )
        except Exception:
            pass

    _perf("total fetch", t_total)

    batting_highlights.sort(
        key=lambda r: (-r["runs"], r["balls"], r["player"].lower())
    )
    bowling_highlights.sort(
        key=lambda r: (-r["wickets"], r["runs_conceded"], r["player"].lower())
    )
    match_rows.sort(key=lambda r: (r["date"] or "", r["mitcham_team"]))

    completed_finished = wins + losses + draws
    if include_juniors and include_seniors:
        scope = "both"
    elif include_seniors:
        scope = "senior"
    else:
        scope = "junior"
    summary_sentence = build_summary_sentence(
        wins,
        losses,
        draws,
        in_progress,
        completed_finished,
        scope=scope,
    )

    junior_only_selected = [
        t for t in selected_teams if classify_team_label(t.label) == "junior"
    ]
    senior_men_selected = [
        t for t in selected_teams if classify_team_label(t.label) == "senior_men"
    ]
    senior_women_selected = [
        t for t in selected_teams if classify_team_label(t.label) == "senior_women"
    ]
    senior_only_selected = senior_men_selected + senior_women_selected

    return {
        "fetch_scope_key": fetch_scope_key,
        "season": season_label,
        "club_url_requested": CLUB_URL_AS_GIVEN,
        "club_page": CLUB_PAGE,
        "date_from": d_from.isoformat(),
        "date_to": d_to.isoformat(),
        "min_runs": min_runs,
        "min_wickets": min_wickets,
        "include_juniors": include_juniors,
        "include_seniors": include_seniors,
        "scope": scope,
        "junior_teams": [
            {"label": t.label, "url": t.grade_url} for t in junior_only_selected
        ],
        "senior_teams": [
            {"label": t.label, "url": t.grade_url} for t in senior_only_selected
        ],
        "senior_men_teams": [
            {"label": t.label, "url": t.grade_url} for t in senior_men_selected
        ],
        "senior_women_teams": [
            {"label": t.label, "url": t.grade_url} for t in senior_women_selected
        ],
        "selected_teams": [
            {"label": t.label, "url": t.grade_url} for t in selected_teams
        ],
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "in_progress": in_progress,
        "summary_sentence": summary_sentence,
        "matches_in_range": len(match_rows),
        "batting_highlights": batting_highlights,
        "bowling_highlights": bowling_highlights,
        "best_bowl": best_bowl,
        "match_rows": match_rows,
    }


_FACEBOOK_JUNK_FRAGMENTS: tuple[str, ...] = (
    "play cricket",
    "playcricket app",
    "fall of wickets",
    "scorecard",
    "ball by ball",
    "graphs",
    "summary",
)


def _facebook_strip_junk_text(s: str) -> str:
    t = re.sub(r"\s+", " ", (s or "").strip())
    if not t or t in ("—", "-"):
        return ""
    low = t.lower()
    if any(j in low for j in _FACEBOOK_JUNK_FRAGMENTS):
        return ""
    if "|" in t:
        parts = [p.strip() for p in t.split("|") if p.strip()]
        for p in parts:
            pl = p.lower()
            if not any(j in pl for j in _FACEBOOK_JUNK_FRAGMENTS) and len(p) >= 3:
                t = p
                break
    return t[:80].strip()


def _facebook_compact_grade_label(mitcham_team: str) -> str:
    """
    'Mitcham U14 (2) U14 - 6 (SEDA)' -> 'U14 (2)'; fallback 'U14' from first grade token.
    """
    s = re.sub(r"\s+", " ", (mitcham_team or "").strip())
    if not s:
        return "Mitcham"
    m = re.search(
        r"(?i)\bU\s*/?\s*(10|12|14|16|18)\s*\((\d+)\)",
        s,
    )
    if m:
        return f"U{m.group(1)} ({m.group(2)})"
    m = re.search(r"(?i)\bU\s*/?\s*(10|12|14|16|18)\b", s)
    if m:
        return f"U{m.group(1)}"
    if _mitcham_in_string(s):
        return "Mitcham"
    return s[:24] if len(s) > 24 else s


def _facebook_short_opponent(opponent: str) -> str:
    o = _facebook_strip_junk_text(opponent)
    if not o:
        return "Opposition"
    o = re.split(r"\s+vs\.?\s+|\s+v\.?\s+", o, maxsplit=1, flags=re.I)[0].strip()
    return o[:56].rstrip(" -–|") if o else "Opposition"


def _facebook_match_result_line(row: dict[str, Any]) -> str | None:
    """Facebook line from resolved row fields only; None if skipped."""
    line, _ = _facebook_row_summary_line(row)
    return line


def _facebook_batting_lines(bh: list[dict[str, Any]], limit: int = 30) -> list[str]:
    rows = sorted(
        bh,
        key=lambda r: (-int(r.get("runs") or 0), str(r.get("player") or "").lower()),
    )
    out: list[str] = []
    for r in rows[:limit]:
        name = str(r.get("player") or "").strip()
        runs = int(r.get("runs") or 0)
        if not name:
            continue
        suffix = " no" if r.get("not_out") else ""
        out.append(f"{name} {runs}{suffix}")
    return out


def _facebook_bowling_combined_lines(bo: list[dict[str, Any]], limit: int = 30) -> list[str]:
    """Same player, multiple spells: 'Name 3/22 & 2/18'. Sort by best spell: wickets desc, runs asc."""
    by_player: dict[str, list[tuple[int, int]]] = {}
    for r in bo:
        name = str(r.get("player") or "").strip()
        if not name:
            continue
        w = int(r.get("wickets") or 0)
        rc = int(r.get("runs_conceded") or 0)
        by_player.setdefault(name, []).append((w, rc))
    ranked: list[tuple[int, int, str, str]] = []
    for name, spells in by_player.items():
        spells_sorted = sorted(spells, key=lambda t: (-t[0], t[1]))
        fig = " & ".join(f"{w}/{r}" for w, r in spells_sorted)
        best_w, best_r = spells_sorted[0]
        ranked.append((best_w, best_r, name, f"{name} {fig}"))
    ranked.sort(key=lambda x: (-x[0], x[1], x[2].lower()))
    return [x[3] for x in ranked[:limit]]


def _facebook_wrap_title(data: dict[str, Any]) -> str:
    j = data.get("include_juniors", True)
    s = data.get("include_seniors", False)
    if j and s:
        return "Mitcham Cricket Club — Junior & senior stats"
    if s and not j:
        return "Mitcham Cricket Club — Senior stats"
    return "Mitcham Cricket Club — Junior stats"


def facebook_summary(data: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(_facebook_wrap_title(data))
    lines.append("")
    intro = (data.get("summary_sentence") or "").strip()
    if intro:
        lines.append(intro)
    lines.append("")
    lines.append("Here are the results:")
    lines.append("")

    match_rows = list(data.get("match_rows") or [])
    if match_rows:
        any_fb = False
        for row in match_rows[:40]:
            line, skip = _facebook_row_summary_line(row)
            if line:
                lines.append(line)
                any_fb = True
            else:
                logger.info(
                    "[FacebookSummary] skipped row url=%r reason=%r",
                    row.get("match_url"),
                    skip,
                )
        if not any_fb:
            lines.append("—")
        if len(match_rows) > 40:
            lines.append(f"… and {len(match_rows) - 40} more matches")
    else:
        lines.append("—")

    bh = list(data.get("batting_highlights") or [])
    bo_rows = list(data.get("bowling_highlights") or [])

    lines.append("")
    lines.append("Best with the ball:")
    bbowl = _facebook_bowling_combined_lines(bo_rows)
    if bbowl:
        lines.append(", ".join(bbowl))
    else:
        lines.append("—")

    lines.append("")
    lines.append("Best with the bat:")
    bbat = _facebook_batting_lines(bh)
    if bbat:
        lines.append(", ".join(bbat))
    else:
        lines.append("—")

    return "\n".join(lines)
