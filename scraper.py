"""
Playwright scraper for Mitcham CC junior stats from play.cricket.com.au.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections import defaultdict
from itertools import groupby
from dataclasses import dataclass, field
from datetime import date, timedelta
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
    # Perf logging is emitted as structured [Perf] lines in run_report.
    return


def _mitcham_team_key(r: dict[str, Any]) -> str:
    t = (r.get("mitcham_team") or "").strip()
    return t if t else "Mitcham"


def group_highlights_by_mitcham_team(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Split highlight rows into [{mitcham_team, entries}, ...].
    Rows must already be sorted so all rows for a team are consecutive.
    """
    if not rows:
        return []
    out: list[dict[str, Any]] = []
    for team, grp in groupby(rows, key=_mitcham_team_key):
        out.append({"mitcham_team": team, "entries": list(grp)})
    return out


def _flatten_grouped_highlight_entries(
    grouped: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """None if grouped is None (caller should fall back to flat lists)."""
    if grouped is None:
        return None
    out: list[dict[str, Any]] = []
    for g in grouped:
        out.extend(g.get("entries") or [])
    return out


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


#
# Debug env flags removed (keep scraper deterministic / low-overhead).
#


def is_match_in_progress(status_text: str, raw_result_text: str) -> bool:
    """
    True for live / multi-day / partial scorecard states. Never True for Completed.
    """
    st = (status_text or "").strip()
    if not st:
        return False
    sl = st.lower()
    if sl == "completed":
        return False
    if sl in ("abandoned", "cancelled"):
        return False
    combined = f"{st} {raw_result_text or ''}".lower()
    markers = (
        "stumps",
        "tea",
        "lunch",
        "day 1",
        "day 2",
        "day 3",
        "in progress",
        "live",
        "yet to resume",
        "scheduled continuation",
    )
    if any(m in combined for m in markers):
        return True
    return True


def normalize_card_status_for_ui(card_status: str) -> str:
    """Match Results / metrics: Completed and Abandoned preserved; else In Progress."""
    cs = (card_status or "").strip()
    if cs == "Completed":
        return "Completed"
    if cs == "Abandoned":
        return "Abandoned"
    return "In Progress"


def _debug_browser_season() -> bool:
    return False


@dataclass
class BattingRow:
    player: str
    runs: int
    balls: int
    not_out: bool
    side_owner: str = field(default="unknown")
    source_method: str = field(default="chip")
    source_confidence: str = field(default="medium")


@dataclass
class BowlingRow:
    player: str
    wickets: int
    runs_conceded: int
    side_owner: str = field(default="unknown")
    source_method: str = field(default="chip")
    source_confidence: str = field(default="medium")


def _tag_batting_slice(
    rows: list[BattingRow],
    start: int,
    *,
    side_owner: str,
    source_method: str,
    source_confidence: str,
) -> None:
    for r in rows[start:]:
        r.side_owner = side_owner
        r.source_method = source_method
        r.source_confidence = source_confidence


def _tag_bowling_slice(
    rows: list[BowlingRow],
    start: int,
    *,
    side_owner: str,
    source_method: str,
    source_confidence: str,
) -> None:
    for r in rows[start:]:
        r.side_owner = side_owner
        r.source_method = source_method
        r.source_confidence = source_confidence


def _fixture_both_sides_mitcham_named(rep: ScorecardExtractReport) -> bool:
    fh_h = (rep.fixture_header_home_team or "").strip()
    fh_a = (rep.fixture_header_away_team or "").strip()
    if not fh_h or not fh_a:
        return False
    return _mitcham_in_string(fh_h) and _mitcham_in_string(fh_a)


def _opponent_distinct_team_tokens_vs_mitcham(
    resolved: dict[str, Any],
) -> set[str]:
    """Tokens in opponent label but not in Mitcham team (e.g. yellow vs black)."""
    mt = _team_tokens_for_matching(resolved.get("mitcham_team") or "")
    ot = _team_tokens_for_matching(resolved.get("opponent") or "")
    return {t for t in ot if t not in mt and len(t) >= 3}


def _innings_label_matches_resolved_mitcham_side(
    lab: str,
    resolved: dict[str, Any] | None,
    rep: ScorecardExtractReport,
) -> bool:
    """
    Mitcham batting innings label/chip/heading: when both fixture sides are Mitcham-named,
    require a distinct Mitcham-side token (e.g. black) in the label.
    """
    if not _innings_is_mitcham(lab):
        return False
    if not resolved:
        return True
    if not _fixture_both_sides_mitcham_named(rep):
        return True
    distinct = _mitcham_distinct_team_tokens_vs_opponent(resolved)
    lab_l = (lab or "").lower()
    if not distinct:
        return False
    return any(len(t) >= 3 and t in lab_l for t in distinct)


def _innings_label_matches_resolved_opposition_bowling_tab(
    lab: str,
    resolved: dict[str, Any] | None,
    rep: ScorecardExtractReport,
) -> bool:
    """
    Innings where the opposition batted (Mitcham bowling card). Excludes our Mitcham
    batting innings; for same-club fixtures uses distinct opponent tokens (e.g. yellow).
    """
    if not resolved:
        return not _innings_is_mitcham(lab)
    if not _fixture_both_sides_mitcham_named(rep):
        return not _innings_is_mitcham(lab)
    if _innings_label_matches_resolved_mitcham_side(lab, resolved, rep):
        return False
    o_dist = _opponent_distinct_team_tokens_vs_mitcham(resolved)
    lab_l = (lab or "").lower()
    if o_dist and any(t in lab_l for t in o_dist):
        return True
    return not _innings_is_mitcham(lab)


def _log_highlight_guard_rail(
    *,
    url: str,
    mitcham_team: str,
    player_name: str,
    side_owner: str,
    source_method: str,
    reason: str,
) -> None:
    logger.debug(
        "[HighlightGuardRail] url=%s mitcham_team=%r player=%r side_owner=%s method=%s reason=%s",
        url,
        mitcham_team,
        player_name,
        side_owner,
        source_method,
        reason,
    )


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


def _extract_scheduled_dates_from_fixture_text(*parts: str) -> list[date]:
    """Distinct dates found in fixture card / scorecard metadata text."""
    blob = "\n".join((p or "") for p in parts)
    seen: set[date] = set()
    for m in re.finditer(
        r",\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\s+at\b",
        blob,
        flags=re.I,
    ):
        d, mon, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        mi = MONTHS.get(mon)
        if not mi:
            continue
        try:
            seen.add(date(y, mi, d))
        except ValueError:
            pass
    for m in re.finditer(
        r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})\b",
        blob,
    ):
        d, mon, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        mi = MONTHS.get(mon)
        if not mi:
            continue
        try:
            seen.add(date(y, mi, d))
        except ValueError:
            continue
    return sorted(seen)


def fixture_overlaps_selected_window(
    item: dict[str, Any],
    raw_fixture_text: str,
    selected_date_from: date,
    selected_date_to: date,
) -> tuple[bool, dict[str, Any]]:
    """
    True if the fixture's primary date or any parsed scheduled date falls inside
    [selected_date_from, selected_date_to]. Used at discovery time before metadata.

    When the card lists no extra day but indicates 2-day or split innings, a
    synthetic second day (primary + 7 days) is added for overlap (week-2 discovery).
    """
    debug: dict[str, Any] = {
        "primary_match_date": None,
        "scheduled_dates_detected": [],
        "overlap_reason": None,
        "team_category": item.get("team_category"),
        "multi_day_type_detected": None,
        "used_fallback_second_day": False,
        "fallback_second_day": None,
    }
    md = item.get("md")
    if isinstance(md, date):
        debug["primary_match_date"] = md.isoformat()

    mdt = _detect_multi_day_type(raw_fixture_text)
    debug["multi_day_type_detected"] = mdt

    parsed = _extract_scheduled_dates_from_fixture_text(raw_fixture_text)
    extra_from_text: set[date] = set(parsed)
    if isinstance(md, date):
        extra_from_text.discard(md)

    seen: set[date] = set()
    if isinstance(md, date):
        seen.add(md)
    seen.update(parsed)

    fallback_d: date | None = None
    if (
        isinstance(md, date)
        and not extra_from_text
        and mdt in ("2_day", "split_innings")
    ):
        fallback_d = md + timedelta(days=7)
        seen.add(fallback_d)
        debug["used_fallback_second_day"] = True
        debug["fallback_second_day"] = fallback_d.isoformat()

    candidate_dates = sorted(seen)
    debug["scheduled_dates_detected"] = [d.isoformat() for d in candidate_dates]

    if not candidate_dates:
        debug["overlap_reason"] = "no_overlap"
        return False, debug

    in_range_dates = [
        d for d in candidate_dates if selected_date_from <= d <= selected_date_to
    ]
    if not in_range_dates:
        debug["overlap_reason"] = "no_overlap"
        return False, debug

    primary_in = isinstance(md, date) and selected_date_from <= md <= selected_date_to
    if primary_in:
        debug["overlap_reason"] = "primary_date_in_range"
        return True, debug

    for d in in_range_dates:
        if d in extra_from_text:
            debug["overlap_reason"] = "scheduled_date_in_range"
            return True, debug
    if fallback_d is not None and fallback_d in in_range_dates:
        debug["overlap_reason"] = "fallback_second_day_in_range"
        return True, debug
    debug["overlap_reason"] = "scheduled_date_in_range"
    return True, debug


def _log_fixture_window_overlap(
    match_url: str,
    selected_date_from: date,
    selected_date_to: date,
    *,
    team_category: Any,
    multi_day_type_detected: str | None,
    used_fallback_second_day: bool,
    fallback_second_day: str | None,
    primary_match_date: str | None,
    scheduled_dates_detected: list[str],
    overlap_result: bool,
    overlap_reason: str | None,
) -> None:
    # Removed (verbose).
    return


def _detect_multi_day_type(blob: str) -> str | None:
    low = (blob or "").lower()
    if "split innings" in low or "split-innings" in low or "splitinnings" in low:
        return "split_innings"
    if (
        re.search(r"\b2\s*[- ]?\s*day\b", low)
        or "two day" in low
        or "two-day" in low
        or "2-day" in low
    ):
        return "2_day"
    return None


def is_partial_window_for_match(
    item: dict[str, Any],
    rep: Any,
    selected_date_from: date,
    selected_date_to: date,
) -> tuple[bool, dict[str, Any]]:
    """
    True when the user's date range only covers the first segment of a multi-day
    / split fixture — use partial display and earliest-innings stats.
    """
    debug: dict[str, Any] = {
        "scheduled_dates_detected": [],
        "multi_day_type_detected": None,
        "partial_window_reason": None,
    }
    card = item.get("card") or ""
    blob = getattr(rep, "raw_match_team_blob", None) or ""
    page_lines = list(getattr(rep, "scorecard_result_lines", None) or [])
    fh_res = getattr(rep, "fixture_header_result_text", None) or ""
    combined = "\n".join([card, blob, fh_res, "\n".join(page_lines)])

    dates = _extract_scheduled_dates_from_fixture_text(
        card, blob, fh_res, "\n".join(page_lines)
    )
    md = item.get("md")
    if isinstance(md, date):
        dates = sorted(set(dates) | {md})
    debug["scheduled_dates_detected"] = [d.isoformat() for d in dates]

    mdt = _detect_multi_day_type(combined)
    debug["multi_day_type_detected"] = mdt

    tc = item.get("team_category")
    status = (item.get("status") or "").strip()

    if len(dates) >= 2 and max(dates) > selected_date_to:
        debug["partial_window_reason"] = "later_scheduled_date_beyond_window"
        return True, debug

    if (
        mdt
        and tc == "senior_men"
        and isinstance(md, date)
        and status == "Completed"
        and selected_date_from <= md <= selected_date_to
        and (selected_date_to - md).days <= 4
        and selected_date_to < md + timedelta(days=7)
    ):
        if len(dates) <= 1:
            debug["partial_window_reason"] = "multi_day_format_narrow_window_first_block"
            return True, debug

    return False, debug


def _log_partial_window_decision(
    match_url: str,
    match_date: date | None,
    selected_date_from: date,
    selected_date_to: date,
    raw_card_status: str,
    display_status: str,
    *,
    partial_window_match: bool,
    scheduled_dates_detected: list[str],
    multi_day_type_detected: str | None,
    partial_window_reason: str | None,
) -> None:
    return


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
    # Removed (verbose / debug-only).
    return


def match_status_from_card(card_text: str) -> str:
    u = card_text.upper()
    low = card_text.lower()
    if "COMPLETED" in u:
        return "Completed"
    if "ABANDONED" in u or "CANCELLED" in u:
        return "Abandoned"
    prog_markers = (
        "stumps",
        "tea",
        "lunch",
        "day 1",
        "day 2",
        "day 3",
        "in progress",
        "live",
        "yet to resume",
        "scheduled continuation",
    )
    if any(m in low for m in prog_markers):
        return "In progress"
    if "IN PROGRESS" in u or "LIVE" in u:
        return "In progress"
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

    fh_home = (getattr(rep, "fixture_header_home_team", None) or "").strip()
    fh_away = (getattr(rep, "fixture_header_away_team", None) or "").strip()
    dom_fixture_xor = bool(
        fh_home
        and fh_away
        and (
            _mitcham_in_string(fh_home) ^ _mitcham_in_string(fh_away)
        )
    )

    blob = (getattr(rep, "raw_match_team_blob", None) or "").strip()
    if dom_fixture_xor:
        vs_m, vs_o = None, None
        pair_m, pair_o = None, None
        scorecard_candidates: list[str] = []
        scorecard_ordered: list[str] = []
        chosen_header_segment = ""
        header_parsed_pair_log = "(skipped_blob_segments_dom_fixture_ok)"
        scorecard_header_pair = ""
    else:
        vs_m, vs_o = _best_mitcham_opponent_pair_from_blob(blob)
        pair_m, pair_o = vs_m, vs_o
        scorecard_candidates = _clean_segments_for_scorecard_header_fallback(blob)
        scorecard_ordered = _ordered_scorecard_header_candidate_segments(
            scorecard_candidates
        )
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
    raw_status_result_blob = "\n".join(
        [card, fh_result or "", "\n".join(page_lines)]
    )
    if status == "Completed":
        result_from_fixture = None
        if fh_result:
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
    elif status == "Abandoned":
        result = normalize_match_result_display(
            status,
            oc,
            card,
            page_lines,
            mitch if mitch != "—" else None,
            opp if opp != "—" else None,
        )
    else:
        result = "In Progress"
    display_status = normalize_card_status_for_ui(status)
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
        "display_status": display_status,
        "raw_status_result_blob": raw_status_result_blob[:2000],
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
    status = str(
        row.get("display_status") or row.get("status") or ""
    ).strip()

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

    if status.lower() == "in progress" or res_l == "in progress":
        if ou:
            return f"{mt} vs {opp} — In Progress", None
        return f"{mt} — In Progress", None

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
    return
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
        f"normalized_result={resolved.get('result')!r} status={status!r} "
        f"display_status={resolved.get('display_status')!r} "
        f"partial_window_match={resolved.get('partial_window_match')!r} "
        f"scheduled_dates_detected={resolved.get('scheduled_dates_detected')!r} "
        f"multi_day_type_detected={resolved.get('multi_day_type_detected')!r} "
        f"partial_window_reason={resolved.get('partial_window_reason')!r} "
        f"window_partial_for_range={resolved.get('window_partial_for_range')!r}"
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
    # Debug-only file log removed.


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
        who = "Mitcham Senior sides had"
    elif scope == "both":
        who = "Mitcham teams (Juniors and Seniors) had"
    else:
        who = "Mitcham Juniors had"
    return (
        f"{who} {tone} outing in the selected period with results showing "
        f"{wins} wins,   {losses} losses,   {draws} draws/ties  and   {in_progress} games in progress."
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


def _row_looks_like_batting_stats_header(cells: list[str]) -> bool:
    """Another Batsman/R/B header row mid-table (e.g. second innings in same grid)."""
    if not cells:
        return False
    ir = _col_index(cells, "runs")
    ib = _col_index(cells, "balls")
    return ir is not None and ib is not None


def parse_batting_table(rows: list[list[str]]) -> list[BattingRow]:
    """Batting rows from scorecard table; dismissal column sets not_out.

    Handles split-innings / multiple batting blocks: scans the full table for every
    Runs+Balls header row and ends each block at extras/total/bowling/fall-of/next header.
    """
    if not rows:
        return []
    max_scan = min(len(rows), 500)
    all_out: list[BattingRow] = []
    for start in range(max_scan):
        header = rows[start]
        ir = _col_index(header, "runs")
        ib = _col_index(header, "balls")
        if ir is None or ib is None:
            continue
        block: list[BattingRow] = []
        for cells in rows[start + 1 :]:
            if len(cells) <= max(ir, ib):
                continue
            if _row_looks_like_batting_stats_header(cells):
                break
            name = (cells[0] or "").strip()
            low = name.lower()
            if not name or low == "batting":
                continue
            if low == "bowling" or low.startswith("bowling "):
                break
            if "fall of wicket" in low or low.startswith("fall of"):
                break
            if low == "extras" or low.startswith("total"):
                break
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
            block.append(BattingRow(player=clean_name, runs=rn, balls=bl, not_out=not_out))
        if block:
            all_out.extend(block)
    return all_out


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

    Supports split-innings: multiple bowling blocks in one table (each valid header
    row starts a block; ends at total/extras/next header).
    """
    if not rows:
        return [], None, None, [], []
    merged: list[tuple[str, int, int]] = []
    best: list[tuple[str, int, int]] = []
    best_header: list[str] | None = None
    best_idx: tuple[int, int, int] | None = None
    best_raw_data: list[list[str]] = []
    best_norm_data: list[list[str]] = []
    max_start = min(len(rows), 500)
    for start in range(max_start):
        header = rows[start]
        idx = _bowling_column_indices(header)
        if idx is None:
            continue
        pi, wi, ri = idx
        out: list[tuple[str, int, int]] = []
        for cells in rows[start + 1 :]:
            if _bowling_column_indices(cells) is not None:
                break
            try:
                norm = _normalize_bowling_data_row(cells, header)
                pl_name = (norm[pi] if pi < len(norm) else "").strip()
                low = pl_name.lower()
                if low in ("bowling", "bowler"):
                    break
                if low.startswith("total") or low == "extras":
                    break
                parsed = _parse_bowling_row_mapped(norm, pi, wi, ri)
                if parsed:
                    out.append(parsed)
            except Exception:
                continue
        if out:
            merged.extend(out)
            if len(out) > len(best):
                best = out
                best_header = header
                best_idx = idx
                best_raw_data = rows[start + 1 : start + 21]
                best_norm_data = [
                    _normalize_bowling_data_row(c, header)
                    for c in best_raw_data
                ]
    return merged, best_header, best_idx, best_raw_data, best_norm_data


def parse_bowling_table(rows: list[list[str]]) -> list[BowlingRow]:
    """Return bowling rows using header column mapping."""
    parsed, _, _, _, _ = _parse_bowling_table_with_meta(rows)
    return [
        BowlingRow(player=n, wickets=w, runs_conceded=r) for n, w, r in parsed
    ]


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
    # Debug-only file snapshot removed.


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


def _dedupe_bowling_rows(rows: list[BowlingRow]) -> list[BowlingRow]:
    seen: set[tuple[str, int, int]] = set()
    out: list[BowlingRow] = []
    for t in rows:
        k = (t.player.lower(), t.wickets, t.runs_conceded)
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def _extend_batting_from_matrices(
    matrices: list[list[list[str]]], acc: list[BattingRow]
) -> int:
    """Parse every batting table matrix; parse_batting_table scans full table for all innings."""
    n = 0
    for tbl in matrices:
        if not tbl:
            continue
        if "batting" not in _table_text_blob(tbl, 16):
            continue
        rows = parse_batting_table(tbl)
        if rows:
            acc.extend(rows)
            n += 1
    return n


def _extend_bowling_from_matrices(
    matrices: list[list[list[str]]], acc: list[BowlingRow]
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
            acc.extend(
                BowlingRow(player=n, wickets=w, runs_conceded=r)
                for n, w, r in best
            )
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
    all_bowl: list[BowlingRow],
    resolved: dict[str, Any] | None,
    rep: ScorecardExtractReport,
    *,
    include_batting: bool = True,
    include_bowling: bool = True,
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
        ctx = (sec.get("sectionContext") or "")[:200]
        if kind == "batting":
            if not include_batting:
                continue
            br = parse_batting_table(rows)
            if not br:
                continue
            n0 = len(all_bat)
            all_bat.extend(br)
            lab = hint.strip()
            comb = f"{lab} {ctx}".strip().lower()
            if _innings_label_matches_resolved_mitcham_side(
                lab, resolved, rep
            ) or _innings_label_matches_resolved_mitcham_side(ctx, resolved, rep):
                _tag_batting_slice(
                    all_bat,
                    n0,
                    side_owner="mitcham",
                    source_method="fallback",
                    source_confidence="medium",
                )
            elif resolved and _fixture_both_sides_mitcham_named(rep):
                o_dist = _opponent_distinct_team_tokens_vs_mitcham(resolved)
                if o_dist and any(t in comb for t in o_dist):
                    _tag_batting_slice(
                        all_bat,
                        n0,
                        side_owner="opponent",
                        source_method="fallback",
                        source_confidence="medium",
                    )
                else:
                    _tag_batting_slice(
                        all_bat,
                        n0,
                        side_owner="unknown",
                        source_method="fallback",
                        source_confidence="low",
                    )
            else:
                if _innings_is_mitcham(lab) or _innings_is_mitcham(ctx):
                    _tag_batting_slice(
                        all_bat,
                        n0,
                        side_owner="mitcham",
                        source_method="fallback",
                        source_confidence="medium",
                    )
                else:
                    _tag_batting_slice(
                        all_bat,
                        n0,
                        side_owner="opponent",
                        source_method="fallback",
                        source_confidence="medium",
                    )
            bat_tables += 1
        elif kind == "bowling":
            if not include_bowling:
                continue
            # Mitcham bowling: opponent batted this innings — hint is not Mitcham's batting innings.
            h = hint.strip()
            if h and _innings_is_mitcham(h):
                continue
            if not h and len(bowling_secs) != 1:
                continue
            bw = parse_bowling_table(rows)
            if not bw:
                continue
            n1 = len(all_bowl)
            all_bowl.extend(bw)
            _tag_bowling_slice(
                all_bowl,
                n1,
                side_owner="mitcham",
                source_method="fallback",
                source_confidence="medium",
            )
            bowl_tables += 1
    return bat_tables, bowl_tables


def _parse_scorecard_full_page_matrices_mitcham_batting(
    page: Page,
    all_bat: list[BattingRow],
    *,
    matrices: list[list[list[str]]] | None = None,
) -> int:
    """
    Last resort: batting matrices whose text blob suggests Mitcham's innings.
    (Bowling is handled via heading-based sections — avoid guessing from grids alone.)
    """
    mats = matrices if matrices is not None else _all_scorecard_matrices(page)
    bat_n = 0
    for tbl in mats:
        blob = _table_text_blob(tbl, min(24, len(tbl)))
        low = blob.lower()
        if "batting" not in low:
            continue
        if not (
            "mitcham" in low
            or re.search(r"\d+\s*(st|nd|rd|th)\s+mit\b", low)
        ):
            continue
        br = parse_batting_table(tbl)
        if br:
            n0 = len(all_bat)
            all_bat.extend(br)
            _tag_batting_slice(
                all_bat,
                n0,
                side_owner="mitcham",
                source_method="fallback",
                source_confidence="medium",
            )
            bat_n += 1
    return bat_n


def _collect_batting_blocks_from_all_matrices(
    page: Page,
) -> list[tuple[list[BattingRow], str]]:
    """
    Every distinct batting table on the scorecard (no Mitcham keyword filter).
    Skips duplicate parses by a short row fingerprint.
    """
    _scroll_scorecard_tables_into_view(page)
    matrices = _all_scorecard_matrices(page)
    out: list[tuple[list[BattingRow], str]] = []
    seen_fp: set[str] = set()
    for tbl in matrices:
        if not tbl:
            continue
        blob = _table_text_blob(tbl, min(36, len(tbl)))
        if "batting" not in blob.lower():
            continue
        rows = parse_batting_table(tbl)
        if not rows:
            continue
        fp = "|".join(f"{r.player.lower()}:{r.runs}:{r.balls}" for r in rows[:12])
        if fp in seen_fp:
            continue
        seen_fp.add(fp)
        out.append((rows, blob))
    return out


_TEAM_MATCHING_STOPWORDS: frozenset[str] = frozenset(
    {
        "xi",
        "grade",
        "shield",
        "overs",
        "players",
        "split",
        "innings",
        "one",
        "day",
        "under",
        "men",
        "women",
        "junior",
        "senior",
        "cricket",
        "club",
        "the",
        "and",
        "match",
        "team",
        "nth",
        "st",
        "nd",
        "rd",
        "th",
    }
)


def _team_tokens_for_matching(name: str) -> set[str]:
    """Strong lowercase tokens from a team label; excludes weak/generic words."""
    raw = re.sub(r"[^a-zA-Z0-9\s\-]", " ", (name or "").lower())
    raw = re.sub(r"\s+", " ", raw).strip()
    out: set[str] = set()
    for p in raw.replace("-", " ").split():
        if len(p) < 3:
            continue
        if p in _TEAM_MATCHING_STOPWORDS:
            continue
        out.add(p)
    if "mitcham" in raw:
        out.add("mitcham")
    return out


def _mitcham_opponent_token_hits(
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> tuple[int, int]:
    """Rough hit counts for Mitcham vs opposition tokens in blob text."""
    mtoks = _team_tokens_for_matching(resolved.get("mitcham_team") or "")
    otoks = _team_tokens_for_matching(resolved.get("opponent") or "")
    m_hits = 0
    for t in mtoks:
        if len(t) >= 3 and t in blob_lower:
            m_hits += 1
    if "mitcham" in blob_lower:
        m_hits += 4
    if re.search(r"(?i)\bmit\b", blob_lower) or re.search(
        r"(?i)\d+(?:st|nd|rd|th)\s+mit\b", blob_lower
    ):
        m_hits += 3
    o_hits = 0
    for t in otoks:
        if len(t) >= 3 and t in blob_lower:
            o_hits += 1
    fh_h = (rep.fixture_header_home_team or "").strip()
    fh_a = (rep.fixture_header_away_team or "").strip()
    if _mitcham_in_string(fh_h):
        for t in _team_tokens_for_matching(fh_h):
            if len(t) >= 3 and t in blob_lower and t != "mitcham":
                m_hits += 1
    if _mitcham_in_string(fh_a):
        for t in _team_tokens_for_matching(fh_a):
            if len(t) >= 3 and t in blob_lower and t != "mitcham":
                m_hits += 1
    if fh_h and not _mitcham_in_string(fh_h):
        for t in _team_tokens_for_matching(fh_h):
            if len(t) >= 4 and t in blob_lower:
                o_hits += 1
    if fh_a and not _mitcham_in_string(fh_a):
        for t in _team_tokens_for_matching(fh_a):
            if len(t) >= 4 and t in blob_lower:
                o_hits += 1
    return m_hits, o_hits


def _has_literal_mitcham_in_blob(blob_lower: str) -> bool:
    return "mitcham" in blob_lower


def _has_literal_mit_innings_markers(blob_lower: str) -> bool:
    """Mitcham innings chip / abbreviation signals (not substring of unrelated words)."""
    if re.search(r"(?i)\bmit\b", blob_lower):
        return True
    if re.search(r"(?i)\d+(?:st|nd|rd|th)\s+mit\b", blob_lower):
        return True
    if "mit " in blob_lower or "mit-" in blob_lower or "mit(" in blob_lower:
        return True
    return False


def _has_mitcham_blob_signals(blob_lower: str) -> bool:
    """True if blob has literal Mitcham name or MIT innings markers."""
    return _has_literal_mitcham_in_blob(blob_lower) or _has_literal_mit_innings_markers(
        blob_lower
    )


def _count_strong_mitcham_resolved_team_tokens(
    blob_lower: str, resolved: dict[str, Any]
) -> int:
    mtoks = _team_tokens_for_matching(resolved.get("mitcham_team") or "")
    return sum(1 for t in mtoks if len(t) >= 3 and t in blob_lower)


def _mitcham_distinct_team_tokens_vs_opponent(resolved: dict[str, Any]) -> set[str]:
    """Tokens present in Mitcham team name but not in opponent (e.g. black vs yellow)."""
    mt = _team_tokens_for_matching(resolved.get("mitcham_team") or "")
    ot = _team_tokens_for_matching(resolved.get("opponent") or "")
    return {t for t in mt if t not in ot and len(t) >= 3}


def _blob_aligns_with_mitcham_fixture_side(
    blob_lower: str, rep: ScorecardExtractReport
) -> bool:
    """
    Fixture lists Mitcham on home or away; blob text clearly matches that Mitcham label.
    """
    for label in (
        (rep.fixture_header_home_team or "").strip(),
        (rep.fixture_header_away_team or "").strip(),
    ):
        if not label or not _mitcham_in_string(label):
            continue
        ll = label.lower()
        if len(ll) >= 10 and ll[: min(90, len(ll))] in blob_lower:
            return True
        mtoks = _team_tokens_for_matching(label)
        hits = sum(1 for t in mtoks if len(t) >= 3 and t in blob_lower)
        if hits >= 2:
            return True
        if "mitcham" in blob_lower and hits >= 1:
            return True
    return False


def _blob_aligns_with_non_mitcham_fixture_side(
    blob_lower: str, rep: ScorecardExtractReport
) -> bool:
    """Blob matches the opponent's fixture team name (home or away), not Mitcham."""
    for label in (
        (rep.fixture_header_home_team or "").strip(),
        (rep.fixture_header_away_team or "").strip(),
    ):
        if not label or _mitcham_in_string(label):
            continue
        ll = label.lower()
        if len(ll) >= 10 and ll[: min(90, len(ll))] in blob_lower:
            return True
        otoks = _team_tokens_for_matching(label)
        hits = sum(1 for t in otoks if len(t) >= 4 and t in blob_lower)
        if hits >= 2:
            return True
    return False


def _is_confident_mitcham_batting_block(
    _rows: list[BattingRow],
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> bool:
    """
    Hard gate: accept a recovered batting block as Mitcham only with strong evidence.
    """
    if _has_literal_mitcham_in_blob(blob_lower):
        return True
    distinct = _mitcham_distinct_team_tokens_vs_opponent(resolved)
    if _has_literal_mit_innings_markers(blob_lower):
        if not distinct:
            return True
        if any(t in blob_lower for t in distinct):
            return True
        if _blob_aligns_with_mitcham_fixture_side(blob_lower, rep):
            return True
        return False
    m_hits, o_hits = _mitcham_opponent_token_hits(blob_lower, rep, resolved)
    strong_tok = _count_strong_mitcham_resolved_team_tokens(blob_lower, resolved)
    if strong_tok >= 2 and o_hits < m_hits:
        return True
    if _blob_aligns_with_mitcham_fixture_side(blob_lower, rep):
        return True
    return False


def _is_confident_opponent_batting_block(
    _rows: list[BattingRow],
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> bool:
    """
    Hard reject: never use this block for Mitcham batting recovery when True.
    """
    m_hits, o_hits = _mitcham_opponent_token_hits(blob_lower, rep, resolved)
    if o_hits >= 2 and m_hits == 0:
        return True
    if o_hits > m_hits and not _has_mitcham_blob_signals(blob_lower):
        return True
    if _blob_aligns_with_non_mitcham_fixture_side(blob_lower, rep):
        if not _has_mitcham_blob_signals(blob_lower):
            return True
    return False


def _is_plausible_mitcham_batting_block(
    rows: list[BattingRow],
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> bool:
    """
    Softer acceptance than confident_mitcham: still excludes confident_opponent
    blocks (caller must not weaken opponent rejection).
    """
    if _is_confident_opponent_batting_block(rows, blob_lower, rep, resolved):
        return False
    has_strong = _count_strong_mitcham_resolved_team_tokens(blob_lower, resolved) >= 1
    pos_signal = (
        _has_literal_mitcham_in_blob(blob_lower)
        or _has_literal_mit_innings_markers(blob_lower)
        or has_strong
        or _blob_aligns_with_mitcham_fixture_side(blob_lower, rep)
    )
    if not pos_signal:
        return False
    distinct = _mitcham_distinct_team_tokens_vs_opponent(resolved)
    if not distinct:
        return True
    if any(t in blob_lower for t in distinct):
        return True
    if _blob_aligns_with_mitcham_fixture_side(blob_lower, rep):
        return True
    return False


def _block_looks_like_opposition_only_batting(
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> bool:
    """True when blob evidence is clearly opposition innings, not Mitcham."""
    if "mitcham" in blob_lower:
        return False
    if re.search(r"(?i)\bmit\b", blob_lower) or re.search(
        r"(?i)\d+(?:st|nd|rd|th)\s+mit\b", blob_lower
    ):
        return False
    m_hits, o_hits = _mitcham_opponent_token_hits(blob_lower, rep, resolved)
    if m_hits >= 2:
        return False
    if o_hits >= 2 and m_hits == 0:
        return True
    if o_hits >= 3 and m_hits <= 1:
        return True
    if o_hits >= m_hits + 3 and o_hits >= 4:
        return True
    return False


def _block_looks_like_mitcham_batting(
    _rows: list[BattingRow],
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> bool:
    """
    True only when Mitcham-side evidence outweighs opposition for this batting block.
    """
    b = blob_lower
    if "mitcham" in b:
        return True
    if re.search(r"(?i)\bmit\b", b) or re.search(
        r"(?i)\d+(?:st|nd|rd|th)\s+mit\b", b
    ):
        return True
    m_hits, o_hits = _mitcham_opponent_token_hits(b, rep, resolved)
    if _block_looks_like_opposition_only_batting(b, rep, resolved):
        return False
    if m_hits >= 2 and m_hits > o_hits:
        return True
    if m_hits >= 1 and o_hits == 0:
        return True
    meta_blob = (
        (rep.raw_match_team_blob or "")
        + " "
        + " ".join(rep.scorecard_result_lines or [])
    ).lower()
    mt = (resolved.get("mitcham_team") or "").strip().lower()
    if mt and len(mt) >= 8 and mt[: min(60, len(mt))] in meta_blob:
        if m_hits >= o_hits:
            return True
    return m_hits > o_hits and m_hits >= 2


def _score_mitcham_batting_block_likeness(
    rows: list[BattingRow],
    blob_lower: str,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
) -> float:
    """Higher = more likely Mitcham batting (not opposition). Used to rank Mitcham blocks."""
    s = 0.0
    b = blob_lower
    if "mitcham" in b:
        s += 160.0
    if re.search(r"(?i)\bmit\b", b) or re.search(
        r"(?i)\d+(?:st|nd|rd|th)\s+mit\b", b
    ):
        s += 110.0
    m_hits, o_hits = _mitcham_opponent_token_hits(b, rep, resolved)
    s += m_hits * 18.0
    s -= o_hits * 35.0
    mt = (resolved.get("mitcham_team") or "").strip().lower()
    if mt:
        for tok in _team_tokens_for_matching(mt):
            if tok in b and tok != "mitcham":
                s += 14.0
    fh_h = (rep.fixture_header_home_team or "").strip().lower()
    fh_a = (rep.fixture_header_away_team or "").strip().lower()
    if _mitcham_in_string(fh_h) and fh_h[:90] in b:
        s += 45.0
    if _mitcham_in_string(fh_a) and fh_a[:90] in b:
        s += 45.0
    opp = (resolved.get("opponent") or "").strip().lower()
    if opp:
        for tok in _team_tokens_for_matching(opp):
            if tok in b and "mitcham" not in b and not re.search(r"\bmit\b", b):
                s -= 55.0
                break
    if fh_h and not _mitcham_in_string(fh_h) and fh_h[:50] in b:
        s -= 40.0
    if fh_a and not _mitcham_in_string(fh_a) and fh_a[:50] in b:
        s -= 40.0
    if rows:
        s += min(len(rows), 12) * 2.0
        s += min(sum(r.runs for r in rows), 500) * 0.015
    return s


def _attempt_full_page_mitcham_batting_recovery(
    page: Page,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any],
    *,
    min_runs: int,
    match_url: str,
) -> tuple[list[BattingRow], dict[str, Any]]:
    """
    When normal innings/tab path yields no batting rows: scan all scorecard batting
    tables. Reject confident-opposition blocks; prefer confident Mitcham blocks, else
    plausible Mitcham blocks; no unrestricted best-score fallback.
    """
    blocks = _collect_batting_blocks_from_all_matrices(page)
    dbg: dict[str, Any] = {
        "url": match_url,
        "candidate_blocks_found": len(blocks),
        "method_used": "full_page_matrix_scan_mitcham_only",
    }
    if not blocks:
        return [], dbg

    scored: list[tuple[list[BattingRow], str, float, dict[str, Any]]] = []
    for rows, blob in blocks:
        bl = blob.lower()
        sc = _score_mitcham_batting_block_likeness(rows, bl, rep, resolved)
        m_hits, o_hits = _mitcham_opponent_token_hits(bl, rep, resolved)
        conf_m = _is_confident_mitcham_batting_block(rows, bl, rep, resolved)
        conf_o = _is_confident_opponent_batting_block(rows, bl, rep, resolved)
        plausible_m = _is_plausible_mitcham_batting_block(rows, bl, rep, resolved)
        has_lit_m = _has_literal_mitcham_in_blob(bl)
        has_lit_i = _has_literal_mit_innings_markers(bl)
        preview = re.sub(r"\s+", " ", blob)[:140]
        if conf_o:
            selection_tier: Literal["confident", "plausible", "rejected"] = "rejected"
            reject_reason: str | None = "confident_opponent"
        elif conf_m:
            selection_tier = "confident"
            reject_reason = None
        elif plausible_m:
            selection_tier = "plausible"
            reject_reason = None
        else:
            selection_tier = "rejected"
            reject_reason = "not_confident_or_plausible_mitcham"
        meta = {
            "blob_preview": preview,
            "score": round(sc, 2),
            "mitcham_token_hits": m_hits,
            "opponent_token_hits": o_hits,
            "confident_mitcham": conf_m,
            "confident_opponent": conf_o,
            "plausible_mitcham": plausible_m,
            "selection_tier": selection_tier,
            "has_literal_mitcham": has_lit_m,
            "has_literal_mit": has_lit_i,
            "selected": False,
            "reject_reason": reject_reason,
        }
        scored.append((rows, blob, sc, meta))

    remaining = [t for t in scored if not t[3]["confident_opponent"]]
    tier_confident = [t for t in remaining if t[3]["confident_mitcham"]]
    tier_plausible = [t for t in remaining if t[3]["plausible_mitcham"]]
    survivors = tier_confident if tier_confident else tier_plausible
    if not survivors:
        dbg["method_used"] = "rejected_ambiguous_blocks"
        return [], dbg

    best = max(survivors, key=lambda x: x[2])
    best[3]["selected"] = True
    best[3]["reject_reason"] = None
    used_tier = "confident" if tier_confident else "plausible"
    dbg["method_used"] = (
        "full_page_matrix_scan_confident_mitcham"
        if used_tier == "confident"
        else "full_page_matrix_scan_plausible_mitcham"
    )
    for t in survivors:
        if t is not best:
            t[3]["reject_reason"] = "not_selected_lower_score"

    merged: list[BattingRow] = list(best[0])
    merged = _dedupe_batting_rows(merged)
    mu = str(dbg.get("method_used") or "")
    if mu == "full_page_matrix_scan_plausible_mitcham":
        for r in merged:
            r.side_owner = "unknown"
            r.source_method = "full_page_recovery"
            r.source_confidence = "low"
    elif mu == "full_page_matrix_scan_confident_mitcham":
        for r in merged:
            r.side_owner = "mitcham"
            r.source_method = "full_page_recovery"
            r.source_confidence = "high"
    else:
        for r in merged:
            r.side_owner = "unknown"
            r.source_method = "full_page_recovery"
            r.source_confidence = "low"
    return merged, dbg


#
# Debug-only log helpers removed.
#


def _log_highlight_final_counts(
    batting_highlights: list[dict[str, Any]],
    bowling_highlights: list[dict[str, Any]],
    grouped_bat: list[dict[str, Any]],
    grouped_bowl: list[dict[str, Any]],
) -> None:
    logger.info(
        "[HighlightFinalCounts] batting_total_rows=%d bowling_total_rows=%d "
        "grouped_batting_team_count=%d grouped_bowling_team_count=%d",
        len(batting_highlights or []),
        len(bowling_highlights or []),
        len(grouped_bat or []),
        len(grouped_bowl or []),
    )


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
    r"(?i)^(?P<code>[A-Z]{2,4})\s+(?P<a>\d+)\s*[-/]\s*(?P<b>\d+)\s*$"
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


def _wait_organisation_season_dropdown_open(page: Page, timeout_ms: int = 8_000) -> bool:
    """True when panel is open: aria-expanded on wrapper and/or list items mounted."""
    try:
        page.wait_for_function(
            """() => {
          const wrap = document.querySelector('#organisation-seasons-options');
          if (wrap && wrap.getAttribute('aria-expanded') === 'true') return true;
          const ul = document.querySelector('#organisation-seasons-options-list');
          if (!ul) return false;
          return ul.querySelectorAll('li.o-dropdown__options-item').length > 0;
        }""",
            timeout=timeout_ms,
        )
        return True
    except Exception:
        return False


def _collect_season_labels_from_organisation_dropdown(page: Page) -> list[str]:
    """
    Read labels from opened #organisation-seasons list only (aria-label / button / li text).
    Scrolls the list so virtualized / long histories expose every season row.
    """
    raw = page.evaluate(
        r"""() => {
          const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
          const seen = new Set();
          const out = [];
          const pushLabel = (t) => {
            const n = norm(t);
            if (!n || seen.has(n)) return;
            seen.add(n);
            out.push(n);
          };
          const readItems = () => {
            const items = document.querySelectorAll(
              '#organisation-seasons-options-list li.o-dropdown__options-item'
            );
            for (const li of items) {
              const btn = li.querySelector('button.o-dropdown__item-trigger');
              let t = '';
              if (btn) {
                t = (btn.getAttribute('aria-label') || '').trim();
                if (!t) {
                  t = (btn.textContent || '').replace(/\s+/g, ' ').trim();
                }
              }
              if (!t) {
                t = (li.textContent || '').replace(/\s+/g, ' ').trim();
              }
              if (t) pushLabel(t);
            }
          };
          const listEl = document.querySelector('#organisation-seasons-options-list')
            || document.querySelector('#organisation-seasons-options');
          if (listEl && listEl.scrollHeight > listEl.clientHeight) {
            const step = Math.max(48, Math.floor(listEl.clientHeight * 0.45) || 80);
            let maxH = listEl.scrollHeight;
            for (let pass = 0; pass < 3; pass++) {
              for (let top = 0; top <= maxH + step; top += step) {
                listEl.scrollTop = Math.min(top, listEl.scrollHeight);
                maxH = Math.max(maxH, listEl.scrollHeight);
                readItems();
              }
              maxH = listEl.scrollHeight;
            }
            listEl.scrollTop = 0;
            readItems();
          } else {
            readItems();
          }
          return out;
        }"""
    )
    if not isinstance(raw, list):
        return []
    return [str(x) for x in raw]


def discover_season_labels_from_page(page: Page) -> list[str]:
    """Read season labels from #organisation-seasons dropdown list (DOM source of truth)."""
    t0 = time.perf_counter()
    page.goto(f"{CLUB_PAGE}?tab=teams", wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(900)
    trig = page.locator("#organisation-seasons").first
    trig.wait_for(state="visible", timeout=15_000)

    dropdown_opened = False
    for attempt in range(2):
        try:
            trig.click(timeout=12_000)
            page.wait_for_timeout(200)
            if _wait_organisation_season_dropdown_open(page, 8_000):
                dropdown_opened = True
                break
        except Exception as ex:
            logger.info(
                "[SeasonDiscovery] open_attempt=%d error=%r",
                attempt + 1,
                ex,
            )
        page.wait_for_timeout(400)

    raw_texts: list[str] = []
    if dropdown_opened:
        try:
            raw_texts = _collect_season_labels_from_organisation_dropdown(page)
        except Exception as ex:
            logger.warning("[SeasonDiscovery] collect_from_list failed: %s", ex)
    else:
        logger.warning(
            "[SeasonDiscovery] dropdown_opened=False — could not open #organisation-seasons"
        )

    logger.info(
        "[SeasonDiscovery] dropdown_opened=%s raw_count=%d raw=%s",
        dropdown_opened,
        len(raw_texts),
        raw_texts,
    )

    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    page.wait_for_timeout(200)

    def season_sort_key(s: str) -> int:
        m = re.search(r"Summer\s+(\d{4})/", s)
        return -int(m.group(1)) if m else 0

    deduped = _ordered_dedupe_season_labels(raw_texts)
    labels = sorted(deduped, key=season_sort_key)
    logger.info(
        "Season discovery: count=%d labels=%s",
        len(labels),
        labels,
    )
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
    # Removed (debug-only).
    return


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
    used_batting_recovery: bool = False
    used_bowling_recovery: bool = False
    normal_parse_sec: float = 0.0
    fallback_parse_sec: float = 0.0
    skipped_no_relevant_scope: bool = False
    relevant_scope_reason: str = ""


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


def _batting_row_debug_repr(br: BattingRow) -> str:
    return f"{br.player} {br.runs}/{br.balls} no={br.not_out}"


#
# Debug-only extraction log removed.
#


def _metadata_suggests_two_team_completed_innings(
    rep: ScorecardExtractReport,
    resolved: dict[str, Any] | None,
) -> bool:
    """
    Heuristic: page metadata suggests a normal two-team completed scorecard with Mitcham involved.
    Used to gate innings-chip recovery (avoid firing on junk pages).
    """
    parts = [
        rep.raw_match_team_blob or "",
        rep.fixture_header_home_team or "",
        rep.fixture_header_away_team or "",
        rep.fixture_header_result_text or "",
        " ".join(rep.scorecard_result_lines or []),
    ]
    blob = " ".join(parts).lower()
    if not _mitcham_in_string(blob):
        return False
    fh_h = (rep.fixture_header_home_team or "").strip()
    fh_a = (rep.fixture_header_away_team or "").strip()
    if fh_h and fh_a:
        if _mitcham_in_string(fh_h) ^ _mitcham_in_string(fh_a):
            return True
    if resolved:
        opp = (resolved.get("opponent") or "").strip()
        if opp and not _mitcham_in_string(opp):
            return True
    if len(re.findall(r"\d+\s*[-/]\s*\d+", blob)) >= 2:
        return True
    if " v " in blob or " vs " in blob:
        return True
    return False


def has_incomplete_completed_innings_discovery(
    rep: ScorecardExtractReport,
    resolved: dict[str, Any] | None,
    *,
    match_completed: bool,
) -> bool:
    """
    True when chip/toggle discovery is asymmetric or too sparse for a two-team scorecard,
    so a page-wide DOM recovery pass should run.
    """
    if not match_completed:
        return False
    if not _metadata_suggests_two_team_completed_innings(rep, resolved):
        return False
    toggles = list(rep.toggles or rep.innings_chips_detected or [])
    mbu = list(rep.mitcham_batting_tabs_used or [])
    obu = list(rep.opposition_innings_for_bowling or [])
    if len(toggles) <= 1:
        return True
    if not mbu:
        return True
    if not obu:
        return True
    return False


def _innings_discovery_fallback_extract(
    page: Page,
    match_url: str,
    all_bat: list[BattingRow],
    all_bowl: list[BowlingRow],
    resolved: dict[str, Any] | None,
    rep: ScorecardExtractReport,
    *,
    recover_batting: bool = True,
    recover_bowling: bool = True,
    matrices: list[list[list[str]]] | None = None,
) -> tuple[int, int, dict[str, Any]]:
    """
    Page-wide recovery without relying on innings chips: heading sections + all matrices.
    Does not invent data — only parses tables already in the DOM.
    """
    mats = matrices if matrices is not None else _all_scorecard_matrices(page)
    n_tables = len(mats)
    n_bat0 = len(all_bat)
    n_bowl0 = len(all_bowl)

    hb = hbw = 0
    hmit = 0
    hit_b = 0
    if recover_batting or recover_bowling:
        hb, hbw = _parse_flat_scorecard_by_headings(
            page,
            all_bat,
            all_bowl,
            resolved,
            rep,
            include_batting=recover_batting,
            include_bowling=recover_bowling,
        )
    if recover_batting:
        hmit = _parse_scorecard_full_page_matrices_mitcham_batting(
            page, all_bat, matrices=mats
        )
    if recover_bowling:
        n_before_matrix_bowl = len(all_bowl)
        hit_b, _ = _extend_bowling_from_matrices(mats, all_bowl)
        if hit_b:
            _tag_bowling_slice(
                all_bowl,
                n_before_matrix_bowl,
                side_owner="mitcham",
                source_method="fallback",
                source_confidence="medium",
            )

    bat_blocks = hb + hmit
    bowl_blocks = hbw + hit_b
    bat_added = all_bat[n_bat0:]
    bowl_added = all_bowl[n_bowl0:]
    info: dict[str, Any] = {
        "tables_scanned": n_tables,
        "bat_blocks": bat_blocks,
        "bowl_blocks": bowl_blocks,
        "bat_rows_delta": len(bat_added),
        "bowl_rows_delta": len(bowl_added),
        "heading_bat_blocks": hb,
        "heading_bowl_blocks": hbw,
        "matrix_mitcham_bat_blocks": hmit,
        "matrix_bowl_blocks": hit_b,
    }
    return bat_blocks, bowl_blocks, info


#
# Innings recovery log removed (debug-only / verbose).
#


#
# Batting extraction fallback log removed.
#


#
# Bowling extraction fallback log removed.
#


def _log_match_highlight_contribution(
    match_url: str,
    mitcham_team: str,
    bats: list[BattingRow],
    bowls: list[BowlingRow],
    min_runs: int,
    min_wickets: int,
) -> None:
    # Removed (debug-only, heavy string building).
    return


def select_partial_innings_scope(
    rep: ScorecardExtractReport, completed_match: bool
) -> dict[str, Any]:
    """
    For completed matches, use all toggles. For partial / in-progress, only the
    earliest visible innings (display order from rep.toggles).
    """
    toggles = list(rep.toggles or rep.innings_chips_detected or [])
    if completed_match:
        return {
            "mode": "full",
            "available_toggles": toggles,
            "selected_toggles": list(toggles),
            "extraction_mode": "full",
        }
    if not toggles:
        return {
            "mode": "partial",
            "available_toggles": [],
            "selected_toggles": [],
            "extraction_mode": "partial",
            "earliest_is_mitcham": None,
        }
    earliest = toggles[0]
    return {
        "mode": "partial",
        "available_toggles": toggles,
        "selected_toggles": [earliest],
        "extraction_mode": "partial",
        "earliest_is_mitcham": _innings_is_mitcham(earliest),
    }


def scrape_match_scorecard_metadata_only(
    page: Page,
    match_url: str,
    match_date: date | None = None,
) -> ScorecardExtractReport:
    """
    Open match, scorecard tab, fixture header + title/blob metadata only (no innings tables).
    Page left on scorecard for scrape_match_scorecard_batting_bowling when needed.
    """
    rep = ScorecardExtractReport(match_url=match_url, scorecard_reached=False)

    try:
        page.goto(match_url, wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_timeout(350)
        rep.scorecard_reached = True
    except Exception as e:
        rep.note = f"goto_failed:{e!r}"
        return rep

    try:
        page.get_by_text("Scorecard", exact=True).first.click(timeout=4000)
        page.wait_for_timeout(150)
        rep.scorecard_tab_clicked = True
    except Exception:
        pass

    ready = _wait_for_scorecard_content(page)
    if not ready:
        rep.note = (rep.note + " scorecard_content_wait_timeout;").strip()
    page.wait_for_timeout(180)

    try:
        fh_meta = _extract_fixture_header_metadata(page)
        rep.fixture_header_home_team = fh_meta.get("homeTeam") or ""
        rep.fixture_header_away_team = fh_meta.get("awayTeam") or ""
        rep.fixture_header_result_text = fh_meta.get("resultText") or ""
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

    return rep


def _open_match_summary_without_scorecard(
    page: Page,
    match_url: str,
    match_date: date | None = None,
) -> ScorecardExtractReport:
    """
    Single goto to the match page; fixture header + blob/result lines without Scorecard tab.
    Used for fixtures-only mode, abandoned rows, and validation when scorecard stats are skipped.
    """
    rep = ScorecardExtractReport(match_url=match_url, scorecard_reached=False)
    try:
        page.goto(match_url, wait_until="domcontentloaded", timeout=120_000)
        page.wait_for_timeout(240)
        rep.scorecard_reached = True
    except Exception as e:
        rep.note = f"goto_failed:{e!r}"
        return rep

    # Fixture header is usually visible on the match page without opening Scorecard.
    try:
        fh_meta = _extract_fixture_header_metadata(page)
        rep.fixture_header_home_team = fh_meta.get("homeTeam") or ""
        rep.fixture_header_away_team = fh_meta.get("awayTeam") or ""
        rep.fixture_header_result_text = fh_meta.get("resultText") or ""
    except Exception:
        pass

    # Try to capture blob/result lines if visible without Scorecard.
    try:
        meta = _extract_match_page_metadata(page)
        rep.raw_match_team_blob = (meta.get("blob") or "").strip()
        rep.scorecard_result_lines = list(meta.get("resultLines") or [])
        rep.mitcham_team_from_page = meta.get("mitcham_from_blob")
        rep.opponent_from_scorecard = meta.get("opponent_from_blob")
    except Exception:
        pass

    return rep


def scrape_match_scorecard_batting_bowling(
    page: Page,
    rep: ScorecardExtractReport,
    *,
    resolved: dict[str, Any] | None = None,
    match_completed: bool = True,
    extraction_mode: Literal["full", "partial"] = "full",
    min_runs: int = 0,
    min_wickets: int = 0,
    enable_recovery_parsing: bool = False,
    parse_batting: bool = True,
    parse_bowling: bool = True,
) -> tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]:
    """
    Batting/bowling extraction only. Page must already be on this match's scorecard.

    resolved: optional resolved fixture row (e.g. opponent) for incomplete chip detection.
    match_completed: set False when status unknown (disables innings recovery heuristics).
    extraction_mode: partial = earliest innings toggle only (split / in-progress games).
    """
    match_url = rep.match_url
    t_norm0 = time.perf_counter()
    all_bat: list[BattingRow] = []
    all_bowl: list[BowlingRow] = []
    bat_tables_total = 0
    bowl_tables_total = 0

    toggles = _merge_innings_toggle_labels(
        _discover_innings_toggle_labels(page),
        _discover_innings_toggle_labels_broad(page),
        _discover_innings_score_chips(page),
    )
    if not toggles:
        toggles = _fallback_short_innings_labels(page)
    rep.toggles = list(toggles)
    last_matrices: list[list[list[str]]] | None = None

    mitcham_tabs = (
        [
            t
            for t in toggles
            if _innings_label_matches_resolved_mitcham_side(t, resolved, rep)
        ]
        if parse_batting
        else []
    )
    opp_tabs = (
        [
            t
            for t in toggles
            if _innings_label_matches_resolved_opposition_bowling_tab(t, resolved, rep)
        ]
        if parse_bowling
        else []
    )
    rep.innings_chips_detected = list(toggles)
    rep.mitcham_innings_chips = list(mitcham_tabs)
    rep.opposition_innings_chips = list(opp_tabs)

    use_partial = extraction_mode == "partial"
    if use_partial:
        scope = select_partial_innings_scope(rep, completed_match=False)
        sel = set(scope.get("selected_toggles") or [])
        if sel:
            mitcham_tabs = [t for t in mitcham_tabs if t in sel]
            opp_tabs = [t for t in opp_tabs if t in sel]

    if toggles and parse_batting:
        for lab in mitcham_tabs:
            try:
                _click_innings_toggle(page, lab)
                rep.mitcham_batting_tabs_used.append(lab)
                matrices = _all_scorecard_matrices(page)
                last_matrices = matrices
                n_bat = len(all_bat)
                bat_tables_total += _extend_batting_from_matrices(matrices, all_bat)
                _tag_batting_slice(
                    all_bat,
                    n_bat,
                    side_owner="mitcham",
                    source_method="chip",
                    source_confidence="high",
                )
            except Exception as e:
                rep.note = (rep.note + f" bat_tab_err({lab}):{e!r};").strip()

    if toggles and parse_bowling:
        for lab in opp_tabs:
            try:
                _click_innings_toggle(page, lab)
                rep.opposition_innings_for_bowling.append(lab)
                matrices = _all_scorecard_matrices(page)
                last_matrices = matrices
                n_bowl = len(all_bowl)
                hit, _bdbg = _extend_bowling_from_matrices(matrices, all_bowl)
                _tag_bowling_slice(
                    all_bowl,
                    n_bowl,
                    side_owner="mitcham",
                    source_method="chip",
                    source_confidence="high",
                )
                bowl_tables_total += hit
            except Exception as e:
                rep.note = (rep.note + f" bowl_tab_err({lab}):{e!r};").strip()

    rep.normal_parse_sec = time.perf_counter() - t_norm0

    if parse_batting and min_runs > 0:
        all_bat = [
            r for r in all_bat if r.side_owner == "mitcham" and r.runs >= min_runs
        ]
    if parse_bowling and min_wickets > 0:
        all_bowl = [
            r
            for r in all_bowl
            if r.side_owner == "mitcham" and r.wickets >= min_wickets
        ]

    has_bat_cand = bool(all_bat) if parse_batting else True
    has_bowl_cand = bool(all_bowl) if parse_bowling else True
    bat_ok = (not parse_batting) or has_bat_cand
    bowl_ok = (not parse_bowling) or has_bowl_cand

    def _finalize() -> tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]:
        all_bat[:] = _dedupe_batting_rows(all_bat)
        all_bowl[:] = _dedupe_bowling_rows(all_bowl)
        rep.batting_tables_parsed = bat_tables_total
        rep.bowling_tables_parsed = bowl_tables_total
        rep.batting_rows = len(all_bat)
        rep.bowling_rows = len(all_bowl)
        logger.info(
            "[ScorecardParsed] url=%s bat_rows=%d bowl_rows=%d",
            match_url,
            len(all_bat),
            len(all_bowl),
        )
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

    if not enable_recovery_parsing:
        rep.fallback_parse_sec = 0.0
        return _finalize()

    if bat_ok and bowl_ok:
        rep.fallback_parse_sec = 0.0
        return _finalize()

    t_fb0 = time.perf_counter()
    need_bat = (not has_bat_cand) and parse_batting
    need_bowl = (not has_bowl_cand) and parse_bowling
    if not (rep.raw_match_team_blob or rep.scorecard_result_lines):
        try:
            meta = _extract_match_page_metadata(page)
            rep.raw_match_team_blob = (meta.get("blob") or "").strip()
            rep.scorecard_result_lines = list(meta.get("resultLines") or [])
            rep.mitcham_team_from_page = meta.get("mitcham_from_blob")
            rep.opponent_from_scorecard = meta.get("opponent_from_blob")
        except Exception:
            pass

    if (
        match_completed
        and has_incomplete_completed_innings_discovery(
            rep, resolved, match_completed=match_completed
        )
    ):
        bb, bw, _fb_info = _innings_discovery_fallback_extract(
            page,
            match_url,
            all_bat,
            all_bowl,
            resolved,
            rep,
            recover_batting=need_bat,
            recover_bowling=need_bowl,
            matrices=last_matrices,
        )
        if need_bat:
            rep.used_batting_recovery = True
        if need_bowl:
            rep.used_bowling_recovery = True
        bat_tables_total += bb
        bowl_tables_total += bw
        rep.note = (rep.note + " innings_recovery_fallback;").strip()
    else:
        hb, hbw = _parse_flat_scorecard_by_headings(
            page,
            all_bat,
            all_bowl,
            resolved,
            rep,
            include_batting=need_bat,
            include_bowling=need_bowl,
        )
        if need_bat:
            rep.used_batting_recovery = True
        if need_bowl:
            rep.used_bowling_recovery = True
        bat_tables_total += hb
        bowl_tables_total += hbw

    if min_runs > 0 and need_bat:
        all_bat = [
            r for r in all_bat if r.side_owner == "mitcham" and r.runs >= min_runs
        ]
    if min_wickets > 0 and need_bowl:
        all_bowl = [
            r for r in all_bowl if r.side_owner == "mitcham" and r.wickets >= min_wickets
        ]

    rep.fallback_parse_sec = time.perf_counter() - t_fb0
    rep.skipped_no_relevant_scope = False
    return _finalize()


def extract_partial_match_highlights(
    page: Page,
    rep: ScorecardExtractReport,
    resolved: dict[str, Any] | None,
) -> tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]:
    """Partial scorecard: earliest innings only; same row types as the full path."""
    return scrape_match_scorecard_batting_bowling(
        page,
        rep,
        resolved=resolved,
        match_completed=False,
        extraction_mode="partial",
    )


def scrape_match_scorecard(
    page: Page,
    match_url: str,
    match_date: date | None = None,
) -> tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]:
    """
    Full scorecard: metadata then batting/bowling (same behavior as before refactor).
    """
    rep = scrape_match_scorecard_metadata_only(page, match_url, match_date)
    if not rep.scorecard_reached:
        return [], [], rep
    return scrape_match_scorecard_batting_bowling(
        page,
        rep,
        resolved=None,
        match_completed=False,
        extraction_mode="full",
    )


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


def _top_bowling_highlights(bowls: list[BowlingRow], n: int = 2) -> str:
    if not bowls:
        return "—"
    s = sorted(bowls, key=lambda x: (-x.wickets, x.runs_conceded))
    return "; ".join(
        format_bowling_display(a.player, a.wickets, a.runs_conceded) for a in s[:n]
    )


def _better_bowl(
    current: tuple[str, int, int] | None, name: str, w: int, r: int
) -> tuple[str, int, int] | None:
    if current is None:
        return (name, w, r)
    _, aw, ar = current
    if w > aw or (w == aw and r < ar):
        return (name, w, r)
    return current


_SEASON_DROPDOWN_VISIBLE_RE = re.compile(
    r"Summer\s+\d{4}/\d{2}",
    flags=re.I,
)


def _norm_season_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _open_club_season_dropdown(page: Page) -> None:
    """
    Open the PlayCricket club season control. Prefer the real trigger button so the
    options panel becomes visible (avoids clicking duplicate hidden option text).
    """
    wrap = page.locator(".o-dropdown__select-wrapper").filter(
        has_text=_SEASON_DROPDOWN_VISIBLE_RE
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
    """
    Click the season row in the open dropdown. Scrolls the list (virtualized / long
    histories) so older seasons (e.g. Summer 2019/20) mount and become clickable.
    """
    want = _norm_season_label(season_label)
    return bool(
        page.evaluate(
            """(want) => {
          const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const wantN = norm(want);
          const readLiLabel = (li) => {
            const btn = li.querySelector('button.o-dropdown__item-trigger');
            if (btn) {
              const a = norm(btn.getAttribute('aria-label') || '');
              if (a) return a;
              return norm(btn.innerText || '');
            }
            return norm(li.innerText || '');
          };
          const clickLi = (li) => {
            li.scrollIntoView({ block: 'center', inline: 'nearest' });
            const btn = li.querySelector('button.o-dropdown__item-trigger');
            if (btn) {
              btn.click();
              return true;
            }
            return false;
          };
          const scanList = (listEl) => {
            if (!listEl) return false;
            const step = Math.max(48, Math.floor(listEl.clientHeight * 0.45) || 80);
            let maxH = listEl.scrollHeight;
            for (let pass = 0; pass < 3; pass++) {
              for (let top = 0; top <= maxH + step; top += step) {
                listEl.scrollTop = Math.min(top, listEl.scrollHeight);
                maxH = Math.max(maxH, listEl.scrollHeight);
                const items = listEl.querySelectorAll('li.o-dropdown__options-item');
                for (const li of items) {
                  if (readLiLabel(li) !== wantN) continue;
                  if (clickLi(li)) return true;
                }
              }
              maxH = listEl.scrollHeight;
            }
            return false;
          };
          const list = document.querySelector('#organisation-seasons-options-list')
            || document.querySelector('#organisation-seasons-options');
          if (list && scanList(list)) return true;
          const roots = document.querySelectorAll(
            '#organisation-seasons-options, #organisation-seasons-options-list, .o-dropdown__options-wrapper'
          );
          for (const root of roots) {
            for (const el of root.querySelectorAll(
              '[role="option"], button.o-dropdown__item-trigger, li.o-dropdown__options-item'
            )) {
              const t = norm(
                el.getAttribute('aria-label') || el.innerText || ''
              );
              if (t !== wantN) continue;
              el.scrollIntoView({ block: 'center', inline: 'nearest' });
              const btn = el.tagName === 'BUTTON' && el.classList.contains('o-dropdown__item-trigger')
                ? el
                : el.querySelector('button.o-dropdown__item-trigger');
              if (btn) {
                btn.click();
                return true;
              }
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
            want,
        )
    )


def _click_season_in_open_dropdown(page: Page, season_label: str) -> None:
    """
    Click the season row inside the open dropdown only (avoids strict-mode duplicate
    matches from page-wide get_by_text, which often resolves to a hidden list span).
    """
    label_n = _norm_season_label(season_label)
    logger.info("[MitchamStats] _select_season: requested=%r", label_n)
    if _click_season_option_via_js(page, label_n):
        logger.info(
            "[MitchamStats] _select_season: clicked ok container=%r for %r",
            "js_scroll_virtualized_list",
            label_n,
        )
        return
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
        opt = container.get_by_text(label_n, exact=True)
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
            label_n,
        )
        return
    raise TimeoutError(
        f"Could not select season {season_label!r}: dropdown option not visible or not found"
    )


def _select_season(page: Page, season_label: str) -> None:
    """Open club Teams tab and choose season from PlayCricket dropdown (controls team list)."""
    page.goto(f"{CLUB_PAGE}?tab=teams", wait_until="domcontentloaded", timeout=120_000)
    page.wait_for_timeout(900)
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(100)
    except Exception:
        pass
    wrap = page.locator(".o-dropdown__select-wrapper").filter(
        has_text=_SEASON_DROPDOWN_VISIBLE_RE
    ).first
    try:
        cur = _norm_season_label(wrap.inner_text(timeout=8000))
    except Exception:
        cur = ""
    if _norm_season_label(season_label) in cur:
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
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except Exception:
        pass
    page.wait_for_timeout(320)
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


#
# Debug-only bowling extraction log removed.
#


def _log_fetch_scope(payload: dict[str, Any]) -> None:
    msg = " | ".join(f"{k}={v!r}" for k, v in payload.items())
    logger.info("[FetchScope] %s", msg)
    try:
        logf = Path(__file__).resolve().parent / "mitcham_fetch_scope.log"
        with logf.open("a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except OSError:
        pass


def _log_fetch_start(
    season_label: str,
    d_from: date,
    d_to: date,
    *,
    include_juniors: bool,
    include_seniors: bool,
    fetch_scope_key: str,
) -> None:
    logger.info(
        "[FetchStart] season=%r date_from=%s date_to=%s juniors_checked=%s "
        "seniors_checked=%s fetch_scope_key=%r",
        season_label,
        d_from.isoformat(),
        d_to.isoformat(),
        include_juniors,
        include_seniors,
        fetch_scope_key,
    )


def _log_fetch_runtime_reset(
    fetch_scope_key: str,
    *,
    cleared_transient_state: bool,
    cleared_report: bool,
    cleared_scorecard_cache: bool,
    cleared_locator_state: bool,
) -> None:
    # Removed (debug-only / noisy).
    return


def _log_fresh_dom_query(action: str) -> None:
    # Removed (debug-only / noisy).
    return


def _normalize_bowling_agg_tuple(
    t: tuple[str, int, int, str, str] | tuple[str, int, int, str, str, str],
) -> tuple[str, int, int, str, str, str]:
    """Ensure 6-tuple (…, mitcham_team) for bowling aggregate rows."""
    if len(t) >= 6:
        return (t[0], t[1], t[2], t[3], t[4], str(t[5] or ""))
    return (t[0], t[1], t[2], t[3], t[4], "")


def _scorecard_cache_matches_mode(
    item: dict[str, Any],
    rep_fin: ScorecardExtractReport,
    d_from: date,
    d_to: date,
    status: str,
    cand_mode: str,
) -> bool:
    window_partial, _ = is_partial_window_for_match(item, rep_fin, d_from, d_to)
    completed_effective = (status == "Completed") and not window_partial
    want_full = completed_effective
    return (cand_mode == "full" and want_full) or (cand_mode == "partial" and not want_full)


def _try_cached_scorecard_parse(
    match_url: str,
    item: dict[str, Any],
    d_from: date,
    d_to: date,
    status: str,
    scorecard_store: dict[
        str, tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]
    ],
) -> tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport] | None:
    for cand in ("full", "partial"):
        k = f"{match_url}||sc||{cand}"
        if k not in scorecard_store:
            continue
        b, bow, rep_fin = scorecard_store[k]
        if _scorecard_cache_matches_mode(item, rep_fin, d_from, d_to, status, cand):
            return b, bow, rep_fin
    return None


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
    include_scorecards: bool = True,
    enable_recovery_parsing: bool = False,
    teams_cache: dict[str, list[TeamRef]] | None = None,
    scorecard_cache: dict[
        str, tuple[list[BattingRow], list[BowlingRow], ScorecardExtractReport]
    ]
    | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if not include_juniors and not include_seniors:
        raise ValueError("Select at least one of juniors or seniors.")

    d_from = min(date_from, date_to)
    d_to = max(date_from, date_to)
    fetch_scope_key = (
        f"{season_label}|{d_from.isoformat()}|{d_to.isoformat()}"
        f"|{int(include_juniors)}|{int(include_seniors)}|{int(include_scorecards)}"
        f"|{int(enable_recovery_parsing)}"
    )
    _log_fetch_start(
        season_label,
        d_from,
        d_to,
        include_juniors=include_juniors,
        include_seniors=include_seniors,
        fetch_scope_key=fetch_scope_key,
    )
    _log_fetch_runtime_reset(
        fetch_scope_key,
        cleared_transient_state=True,
        cleared_report=False,
        cleared_scorecard_cache=False,
        cleared_locator_state=True,
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
    all_bowling_agg: list[tuple[str, int, int, str, str, str]] = []

    def prog(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)

    cache = teams_cache if teams_cache is not None else {}
    scorecard_store = scorecard_cache if scorecard_cache is not None else {}
    cache_key = season_label
    t_total = time.perf_counter()
    match_discovery_sec = 0.0
    scorecard_parse_sec = 0.0
    scorecards_attempted = 0
    scorecards_parsed_normal_only = 0
    scorecards_with_recovery = 0
    scorecards_skipped_invalid = 0
    scorecards_skipped_abandoned = 0
    scorecards_skipped_results_only = 0
    scorecards_skipped_no_relevant_scope = 0
    scorecards_from_cache_full = 0
    scorecards_from_cache_partial = 0
    selected_teams: list[TeamRef] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        try:
            t0 = time.perf_counter()
            if cache_key in cache:
                all_teams = cache[cache_key]
                prog("Syncing club page season (cached team list)…")
                _log_fresh_dom_query("season_select")
                _select_season(page, season_label)
            else:
                prog("Selecting season and loading club teams…")
                _log_fresh_dom_query("season_select")
                _select_season(page, season_label)
                _log_fresh_dom_query("team_discovery")
                all_teams = discover_teams_from_page(page)
                cache[cache_key] = all_teams
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
            _log_fresh_dom_query("match_list")
            match_discovery_attempts = 0
            prev_count: int | None = None
            for attempt in range(3):
                for team in selected_teams:
                    tc = classify_team_label(team.label)
                    pairs = list_matches_for_team(page, team)
                    if attempt == 0:
                        per_team_raw_matches[team.label] = len(pairs)
                    for match_url, card in pairs:
                        md = _parse_first_match_date(card)
                        overlap, overlap_dbg = fixture_overlaps_selected_window(
                            {"md": md, "team_category": tc}, card, d_from, d_to
                        )
                        _log_fixture_window_overlap(
                            match_url,
                            d_from,
                            d_to,
                            team_category=overlap_dbg.get("team_category"),
                            multi_day_type_detected=overlap_dbg.get(
                                "multi_day_type_detected"
                            ),
                            used_fallback_second_day=bool(
                                overlap_dbg.get("used_fallback_second_day")
                            ),
                            fallback_second_day=overlap_dbg.get("fallback_second_day"),
                            primary_match_date=overlap_dbg.get("primary_match_date"),
                            scheduled_dates_detected=list(
                                overlap_dbg.get("scheduled_dates_detected") or []
                            ),
                            overlap_result=overlap,
                            overlap_reason=overlap_dbg.get("overlap_reason"),
                        )
                        if not overlap:
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
                c_now = len(by_url)
                match_discovery_attempts = attempt + 1
                if prev_count is not None and c_now == prev_count:
                    break
                prev_count = c_now
                if attempt < 2:
                    page.wait_for_timeout(400)
            _perf("match discovery (date-filtered)", t1)

            ordered = sorted(
                by_url.values(),
                key=lambda x: (x["md"] or date.min, x["match_url"]),
            )
            match_discovery_sec = time.perf_counter() - t1
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
                    "match_discovery_attempts": match_discovery_attempts,
                    "matches_junior": n_matches_junior,
                    "matches_senior_men": n_matches_senior_men,
                    "matches_senior_women": n_matches_senior_women,
                }
            )
            prog(
                f"Loading {n_m} scorecards…"
                if include_scorecards
                else f"Processing {n_m} matches (fixtures only)…"
            )
            _log_fresh_dom_query("scorecard_tab")
            t2 = time.perf_counter()
            team_accepted: dict[str, int] = defaultdict(int)
            team_rejected: dict[str, int] = defaultdict(int)
            scorecard_parse_sec = 0.0
            for i, item in enumerate(ordered):
                match_url = item["match_url"]
                md: date = item["md"]
                status = item["status"]
                oc = item["oc"]
                disc_lab = item.get("discovered_team_label") or ""

                window_partial = False
                window_dbg: dict[str, Any] = {}
                bats: list[BattingRow] = []
                bowls: list[BowlingRow] = []
                track_parse_counters = False
                _rep_final: ScorecardExtractReport

                if not include_scorecards:
                    _rep = _open_match_summary_without_scorecard(page, match_url, md)
                    window_partial, window_dbg = is_partial_window_for_match(
                        item, _rep, d_from, d_to
                    )
                    resolved_pre = _resolve_match_row_fields(item, _rep)
                    ok, rej = is_valid_mitcham_match(
                        _rep, resolved_pre, item.get("card") or ""
                    )
                    if not ok:
                        scorecards_skipped_invalid += 1
                        team_rejected[disc_lab] += 1
                        if n_m and (i + 1) % max(1, n_m // 8) == 0:
                            prog(f"Matches {i + 1}/{n_m}…")
                        continue
                    team_accepted[disc_lab] += 1
                    scorecards_skipped_results_only += 1
                    _rep_final = _rep
                    resolved = _resolve_match_row_fields(item, _rep_final)

                elif status == "Abandoned":
                    _rep = _open_match_summary_without_scorecard(page, match_url, md)
                    window_partial, window_dbg = is_partial_window_for_match(
                        item, _rep, d_from, d_to
                    )
                    resolved_pre = _resolve_match_row_fields(item, _rep)
                    ok, rej = is_valid_mitcham_match(
                        _rep, resolved_pre, item.get("card") or ""
                    )
                    if not ok:
                        scorecards_skipped_invalid += 1
                        team_rejected[disc_lab] += 1
                        if n_m and (i + 1) % max(1, n_m // 8) == 0:
                            prog(f"Scorecards {i + 1}/{n_m}…")
                        continue
                    team_accepted[disc_lab] += 1
                    scorecards_skipped_abandoned += 1
                    _rep_final = _rep
                    resolved = _resolve_match_row_fields(item, _rep_final)

                else:
                    track_parse_counters = True
                    cached = _try_cached_scorecard_parse(
                        match_url, item, d_from, d_to, status, scorecard_store
                    )
                    if cached is not None:
                        team_accepted[disc_lab] += 1
                        scorecards_attempted += 1
                        bats, bowls, _rep_final = cached
                        window_partial, window_dbg = is_partial_window_for_match(
                            item, _rep_final, d_from, d_to
                        )
                        completed_effective = (status == "Completed") and not window_partial
                        if completed_effective:
                            scorecards_from_cache_full += 1
                        else:
                            scorecards_from_cache_partial += 1
                        resolved = _resolve_match_row_fields(item, _rep_final)
                        cache_sc_key = f"{match_url}||sc||{'full' if completed_effective else 'partial'}"
                        if (
                            enable_recovery_parsing
                            and completed_effective
                            and not bats
                            and (resolved.get("mitcham_team") or "").strip()
                            and not _rep_final.used_batting_recovery
                        ):
                            t_sc1 = time.perf_counter()
                            rec, _br_dbg = _attempt_full_page_mitcham_batting_recovery(
                                page,
                                _rep_final,
                                resolved,
                                min_runs=min_runs,
                                match_url=match_url,
                            )
                            scorecard_parse_sec += time.perf_counter() - t_sc1
                            if rec:
                                bats = rec
                                scorecard_store[cache_sc_key] = (
                                    bats,
                                    bowls,
                                    _rep_final,
                                )
                                _rep_final.note = (
                                    (_rep_final.note or "")
                                    + " full_page_batting_recovery;"
                                ).strip()
                    else:
                        _rep_sc = scrape_match_scorecard_metadata_only(
                            page, match_url, md
                        )
                        window_partial, window_dbg = is_partial_window_for_match(
                            item, _rep_sc, d_from, d_to
                        )
                        resolved_pre = _resolve_match_row_fields(item, _rep_sc)
                        ok, rej = is_valid_mitcham_match(
                            _rep_sc, resolved_pre, item.get("card") or ""
                        )
                        if not ok:
                            scorecards_skipped_invalid += 1
                            team_rejected[disc_lab] += 1
                            if n_m and (i + 1) % max(1, n_m // 8) == 0:
                                prog(f"Scorecards {i + 1}/{n_m}…")
                            continue
                        team_accepted[disc_lab] += 1

                        completed_effective = (status == "Completed") and not window_partial
                        needed_mode = "full" if completed_effective else "partial"
                        cache_sc_key = f"{match_url}||sc||{needed_mode}"

                        scorecards_attempted += 1
                        t_sc0 = time.perf_counter()
                        bats, bowls, _rep_final = scrape_match_scorecard_batting_bowling(
                            page,
                            _rep_sc,
                            resolved=resolved_pre,
                            match_completed=completed_effective,
                            extraction_mode="full"
                            if completed_effective
                            else "partial",
                            min_runs=min_runs,
                            min_wickets=min_wickets,
                            enable_recovery_parsing=enable_recovery_parsing,
                        )
                        scorecard_parse_sec += time.perf_counter() - t_sc0
                        scorecard_store[cache_sc_key] = (bats, bowls, _rep_final)
                        resolved = _resolve_match_row_fields(item, _rep_final)
                        if (
                            enable_recovery_parsing
                            and completed_effective
                            and not bats
                            and (resolved.get("mitcham_team") or "").strip()
                            and not _rep_final.used_batting_recovery
                        ):
                            t_sc1 = time.perf_counter()
                            rec, _br_dbg = _attempt_full_page_mitcham_batting_recovery(
                                page,
                                _rep_final,
                                resolved,
                                min_runs=min_runs,
                                match_url=match_url,
                            )
                            scorecard_parse_sec += time.perf_counter() - t_sc1
                            if rec:
                                bats = rec
                                scorecard_store[cache_sc_key] = (
                                    bats,
                                    bowls,
                                    _rep_final,
                                )
                                _rep_final.note = (
                                    (_rep_final.note or "")
                                    + " full_page_batting_recovery;"
                                ).strip()

                extraction_mode_logged = (
                    "full"
                    if (status == "Completed") and not window_partial
                    else "partial"
                )

                if track_parse_counters:
                    if (
                        _rep_final.used_batting_recovery
                        or _rep_final.used_bowling_recovery
                    ):
                        scorecards_with_recovery += 1
                    else:
                        scorecards_parsed_normal_only += 1

                resolved["scheduled_dates_detected"] = window_dbg.get(
                    "scheduled_dates_detected"
                )
                resolved["multi_day_type_detected"] = window_dbg.get(
                    "multi_day_type_detected"
                )
                resolved["partial_window_reason"] = window_dbg.get(
                    "partial_window_reason"
                )
                if window_partial and status != "Abandoned":
                    resolved["result"] = "In Progress"
                    resolved["display_status"] = "In Progress"
                    resolved["window_partial_for_range"] = True

                if window_partial:
                    in_progress += 1
                elif status == "Completed":
                    if oc == "win":
                        wins += 1
                    elif oc == "loss":
                        losses += 1
                    elif oc == "draw":
                        draws += 1
                elif status != "Abandoned":
                    in_progress += 1

                ds = md.isoformat() if md else ""
                partial_window_match = window_partial or (
                    status not in ("Completed", "Abandoned")
                    and md is not None
                    and d_from <= md <= d_to
                )
                resolved["partial_window_match"] = partial_window_match

                agg_bat_rows += len(bats)
                agg_bowl_rows += len(bowls)
                agg_bat_pass_min += sum(
                    1
                    for br in bats
                    if br.runs >= min_runs and br.side_owner == "mitcham"
                )
                mt = str(resolved.get("mitcham_team") or "").strip() or "Mitcham"
                if include_scorecards:
                    _log_match_highlight_contribution(
                        match_url, mt, bats, bowls, min_runs, min_wickets
                    )
                for br in bats:
                    if br.runs >= min_runs:
                        if br.side_owner != "mitcham":
                            _log_highlight_guard_rail(
                                url=match_url,
                                mitcham_team=mt,
                                player_name=br.player,
                                side_owner=br.side_owner,
                                source_method=br.source_method,
                                reason="side_owner_not_mitcham",
                            )
                            continue
                        batting_highlights.append(
                            {
                                "date": ds,
                                "player": br.player,
                                "runs": br.runs,
                                "balls": br.balls,
                                "not_out": br.not_out,
                                "formatted": format_batting_display(br),
                                "match_url": match_url,
                                "mitcham_team": mt,
                            }
                        )
                for bowl in bowls:
                    if bowl.wickets >= min_wickets and bowl.side_owner != "mitcham":
                        _log_highlight_guard_rail(
                            url=match_url,
                            mitcham_team=mt,
                            player_name=bowl.player,
                            side_owner=bowl.side_owner,
                            source_method=bowl.source_method,
                            reason="side_owner_not_mitcham",
                        )
                    if bowl.side_owner != "mitcham":
                        continue
                    all_bowling_agg.append(
                        (bowl.player, bowl.wickets, bowl.runs_conceded, ds, match_url, mt)
                    )

                disp_st = (
                    resolved.get("display_status")
                    or normalize_card_status_for_ui(status)
                )
                match_rows.append(
                    {
                        "date": ds,
                        "mitcham_team": resolved["mitcham_team"],
                        "opponent": resolved["opponent"],
                        "status": disp_st,
                        "result": resolved["result"],
                        "match_url": match_url,
                        "partial_window_match": partial_window_match,
                    }
                )
                _log_match_results_row(match_url, resolved, status)
                if n_m and (i + 1) % max(1, n_m // 8) == 0:
                    prog(
                        f"Scorecards {i + 1}/{n_m}…"
                        if include_scorecards
                        else f"Matches {i + 1}/{n_m}…"
                    )

                if track_parse_counters and (
                    float(getattr(_rep_final, "normal_parse_sec", 0.0) or 0.0) > 4.0
                    or float(getattr(_rep_final, "fallback_parse_sec", 0.0) or 0.0) > 2.0
                ):
                    logger.warning(
                        "[SlowScorecard] url=%s normal_parse_sec=%.3f fallback_sec=%.3f "
                        "status=%s final_mitcham_team=%r extraction_mode=%s",
                        match_url,
                        float(getattr(_rep_final, "normal_parse_sec", 0.0) or 0.0),
                        float(getattr(_rep_final, "fallback_parse_sec", 0.0) or 0.0),
                        status,
                        str(resolved.get("mitcham_team") or ""),
                        extraction_mode_logged,
                    )

            agg_bowl_pass_min = sum(
                1 for t in all_bowling_agg if t[1] >= min_wickets
            )
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
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    bowling_highlights = [
        {
            "date": t[3],
            "player": t[0],
            "wickets": t[1],
            "runs_conceded": t[2],
            "formatted": format_bowling_display(t[0], t[1], t[2]),
            "match_url": t[4],
            "mitcham_team": t[5],
        }
        for t in all_bowling_agg
        if t[1] >= min_wickets
    ]
    for t in all_bowling_agg:
        if t[1] >= min_wickets:
            best_bowl = _better_bowl(best_bowl, t[0], t[1], t[2])
    # Removed verbose bowling aggregate debug logging.

    _total_fetch_sec = time.perf_counter() - t_total
    logger.info("[Perf] include_scorecards=%s", include_scorecards)
    logger.info("[Perf] match_discovery_sec=%.3f", match_discovery_sec)
    logger.info("[Perf] scorecard_parse_sec=%.3f", scorecard_parse_sec)
    logger.info("[Perf] total_fetch_sec=%.3f", _total_fetch_sec)
    _perf("total fetch", t_total)

    batting_highlights.sort(
        key=lambda r: (
            (r.get("mitcham_team") or "").lower(),
            -int(r.get("runs") or 0),
            int(r.get("balls") or 0),
            str(r.get("player") or "").lower(),
        )
    )
    bowling_highlights.sort(
        key=lambda r: (
            (r.get("mitcham_team") or "").lower(),
            -int(r.get("wickets") or 0),
            int(r.get("runs_conceded") or 0),
            str(r.get("player") or "").lower(),
        )
    )
    match_rows.sort(key=lambda r: (r["date"] or "", r["mitcham_team"]))

    grouped_batting_highlights = group_highlights_by_mitcham_team(batting_highlights)
    grouped_bowling_highlights = group_highlights_by_mitcham_team(bowling_highlights)
    _log_highlight_final_counts(
        batting_highlights,
        bowling_highlights,
        grouped_batting_highlights,
        grouped_bowling_highlights,
    )

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
        "include_scorecards": include_scorecards,
        "enable_recovery_parsing": enable_recovery_parsing,
        "scorecards_attempted": scorecards_attempted,
        "scorecards_parsed_normal_only": scorecards_parsed_normal_only,
        "scorecards_with_recovery": scorecards_with_recovery,
        "scorecards_skipped_invalid": scorecards_skipped_invalid,
        "scorecards_skipped_abandoned": scorecards_skipped_abandoned,
        "scorecards_skipped_results_only": scorecards_skipped_results_only,
        "scorecards_skipped_no_relevant_scope": scorecards_skipped_no_relevant_scope,
        "scorecards_from_cache_full": scorecards_from_cache_full,
        "scorecards_from_cache_partial": scorecards_from_cache_partial,
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
        "grouped_batting_highlights": grouped_batting_highlights,
        "grouped_bowling_highlights": grouped_bowling_highlights,
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


def _facebook_batting_lines(
    bh: list[dict[str, Any]], *, limit: int | None = 30
) -> list[str]:
    """Comma-list lines; same player + multiple innings -> 'Name 24 & 20' (order = sorted rows)."""
    rows = sorted(
        bh,
        key=lambda r: (-int(r.get("runs") or 0), str(r.get("player") or "").lower()),
    )
    capped = rows if limit is None else rows[:limit]
    order: list[str] = []
    by_name: dict[str, list[str]] = {}
    for r in capped:
        name = str(r.get("player") or "").strip()
        runs = int(r.get("runs") or 0)
        if not name:
            continue
        suffix = " no" if r.get("not_out") else ""
        stat = f"{runs}{suffix}"
        if name not in by_name:
            order.append(name)
            by_name[name] = []
        by_name[name].append(stat)
    out: list[str] = []
    for name in order:
        stats = by_name[name]
        if len(stats) == 1:
            out.append(f"{name} {stats[0]}")
        else:
            out.append(f"{name} " + " & ".join(stats))
    return out


def _facebook_bowling_combined_lines(
    bo: list[dict[str, Any]], *, limit: int | None = 30
) -> list[str]:
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
    capped = ranked if limit is None else ranked[:limit]
    return [x[3] for x in capped]


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

    bat_flat = _flatten_grouped_highlight_entries(
        data.get("grouped_batting_highlights")
    )
    bowl_flat = _flatten_grouped_highlight_entries(
        data.get("grouped_bowling_highlights")
    )
    bh = (
        bat_flat
        if bat_flat is not None
        else list(data.get("batting_highlights") or [])
    )
    bo_rows = (
        bowl_flat
        if bowl_flat is not None
        else list(data.get("bowling_highlights") or [])
    )

    if bo_rows:
        lines.append("")
        lines.append("Best with the ball:")
        bbowl = _facebook_bowling_combined_lines(bo_rows, limit=None)
        if bbowl:
            lines.append(", ".join(bbowl))
        else:
            lines.append("—")

    if bh:
        lines.append("")
        lines.append("Best with the bat:")
        bbat = _facebook_batting_lines(bh, limit=None)
        if bbat:
            lines.append(", ".join(bbat))
        else:
            lines.append("—")

    return "\n".join(lines)
