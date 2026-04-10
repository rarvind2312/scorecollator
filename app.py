"""
Mitcham CC junior stats — Streamlit front-end for play.cricket.com.au (Playwright).
"""

import base64
import logging
import calendar
import html
import re
from datetime import date
from itertools import groupby
from pathlib import Path
from typing import Any, Literal, Optional

import pandas as pd
import streamlit as st

from scraper import (
    default_season_choices,
    discover_all_season_labels,
    facebook_summary,
    run_report,
)

logger = logging.getLogger(__name__)

# --- Cricbuzz-style highlights (UI only; data from scraper unchanged) ---


def normalize_not_out(stat: str) -> str:
    s = (stat or "").strip()
    if re.search(r"\bnot out\b", s, flags=re.I):
        s = re.sub(r"\s*not out\s*", "", s, flags=re.I).strip()
        if not s.endswith("*"):
            s = f"{s}*"
    return s


def extract_runs(stat: str) -> Optional[int]:
    s = (stat or "").strip().rstrip("*").strip()
    m = re.match(r"^(\d+)", s)
    return int(m.group(1)) if m else None


def extract_wickets(stat: str) -> Optional[int]:
    m = re.match(r"^(\d+)\s*/", (stat or "").strip())
    return int(m.group(1)) if m else None


def is_elite_batting(stat: str) -> bool:
    r = extract_runs(stat)
    return r is not None and r >= 50


def is_elite_bowling(stat: str) -> bool:
    """UI-only elite highlight (5+ wickets); scraper thresholds unchanged."""
    w = extract_wickets(stat)
    return w is not None and w >= 5


def _split_player_stat(line: str) -> tuple[str, str]:
    for sep in (" – ", " - "):
        if sep in line:
            i = line.index(sep)
            return line[:i].strip(), line[i + len(sep) :].strip()
    return (line or "").strip(), ""


def _entry_formatted(entry: Any) -> str:
    if isinstance(entry, str):
        return entry.strip()
    if isinstance(entry, dict):
        return str(entry.get("formatted") or "").strip()
    return ""


def _grouped_highlights_for_ui(data: dict, kind: Literal["bat", "bowl"]) -> list[dict[str, Any]]:
    gkey = (
        "grouped_batting_highlights" if kind == "bat" else "grouped_bowling_highlights"
    )
    grouped = data.get(gkey)
    if grouped:
        return list(grouped)
    fkey = "batting_highlights" if kind == "bat" else "bowling_highlights"
    flat: list[dict[str, Any]] = list(data.get(fkey) or [])
    if not flat:
        return []

    def team_key(r: dict[str, Any]) -> str:
        t = (r.get("mitcham_team") or "").strip()
        return t if t else "Mitcham"

    if kind == "bat":
        flat.sort(
            key=lambda r: (
                (r.get("mitcham_team") or "").lower(),
                -int(r.get("runs") or 0),
                int(r.get("balls") or 0),
                str(r.get("player") or "").lower(),
            )
        )
    else:
        flat.sort(
            key=lambda r: (
                (r.get("mitcham_team") or "").lower(),
                -int(r.get("wickets") or 0),
                int(r.get("runs_conceded") or 0),
                str(r.get("player") or "").lower(),
            )
        )
    out: list[dict[str, Any]] = []
    for team, grp in groupby(flat, key=team_key):
        out.append({"mitcham_team": team, "entries": list(grp)})
    return out


def _badge_label(
    entries: list[Any], typ: Literal["bat", "bowl"], visible_n: int
) -> str:
    n = max(visible_n, 0)
    if typ == "bat":
        elite = sum(
            1
            for e in entries
            if is_elite_batting(_split_player_stat(_entry_formatted(e))[1])
        )
        if elite:
            return f"{elite} x 50+"
    else:
        elite = sum(
            1
            for e in entries
            if is_elite_bowling(_split_player_stat(_entry_formatted(e))[1])
        )
        if elite:
            return f"{elite} x 5+"
    return f"{n} highlights"


def render_cricbuzz_highlights(
    title: str,
    grouped_data: list[dict[str, Any]],
    typ: Literal["bat", "bowl"],
) -> str:
    empty_msg = (
        "No performances reached your minimum runs threshold."
        if typ == "bat"
        else "No performances reached your minimum wickets threshold."
    )
    blocks: list[str] = [
        '<div class="cbz-highlights">',
        f'<h3 class="cbz-highlights-title">{html.escape(title)}</h3>',
    ]
    if not grouped_data:
        blocks.append(
            f'<p class="cbz-highlights-empty">{html.escape(empty_msg)}</p></div>'
        )
        return "".join(blocks)

    blocks.append('<div class="cbz-grid">')
    for grp in grouped_data:
        team = (grp.get("mitcham_team") or "").strip() or "Mitcham"
        entries = list(grp.get("entries") or [])
        if not entries:
            continue
        row_html: list[str] = []
        vis = 0
        for ent in entries:
            line = _entry_formatted(ent)
            if not line:
                continue
            name, stat_raw = _split_player_stat(line)
            stat_disp = normalize_not_out(stat_raw) if typ == "bat" else (stat_raw or "").strip()
            if typ == "bat":
                elite = is_elite_batting(stat_raw)
            else:
                elite = is_elite_bowling(stat_raw)
            row_cls = ["cbz-row"]
            if elite:
                row_cls.append("cbz-row--elite")
            elif vis == 0:
                row_cls.append("cbz-row--first")
            vis += 1
            el = " cbz-stat-elite" if elite else ""
            row_html.append(
                f'<div class="{" ".join(row_cls)}">'
                f'<span class="cbz-name">{html.escape(name)}</span>'
                f'<span class="cbz-stat{el}">{html.escape(stat_disp)}</span>'
                f"</div>"
            )
        if not row_html:
            continue
        badge = html.escape(_badge_label(entries, typ, len(row_html)))
        blocks.append('<div class="cbz-card">')
        blocks.append('<div class="cbz-card-head">')
        blocks.append(f'<span class="cbz-team">{html.escape(team)}</span>')
        blocks.append(f'<span class="cbz-badge">{badge}</span>')
        blocks.append('</div><div class="cbz-card-body">')
        blocks.extend(row_html)
        blocks.append("</div></div>")
    blocks.append("</div></div>")
    inner = "".join(blocks)
    if '<div class="cbz-card">' not in inner:
        return (
            f'<div class="cbz-highlights"><h3 class="cbz-highlights-title">'
            f"{html.escape(title)}</h3>"
            f'<p class="cbz-highlights-empty">{html.escape(empty_msg)}</p></div>'
        )
    return inner

_ASSETS = Path(__file__).resolve().parent / "assets"
_LOGO_WEBP = _ASSETS / "mitcham_official_logo.webp"
_LOGO_PNG = _ASSETS / "mitcham_official_logo.png"


def _logo_data_uri() -> Optional[str]:
    for path in (_LOGO_PNG, _LOGO_WEBP):
        if path.exists():
            raw = path.read_bytes()
            mime = "image/png" if path.suffix.lower() == ".png" else "image/webp"
            b64 = base64.standard_b64encode(raw).decode("ascii")
            return f"data:{mime};base64,{b64}"
    return None


def _truncate(s: str, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _match_metrics_html(data: dict) -> str:
    """Semantic colors via CSS classes on values; numbers only, escaped."""
    w = int(data["wins"])
    l = int(data["losses"])
    d = int(data["draws"])
    g = int(data.get("in_progress", 0))
    return (
        '<div class="mcc-metrics-row">'
        '<div class="mcc-metric-card">'
        '<div class="mcc-metric-label">Wins</div>'
        f'<div class="mcc-metric-value mcc-win">{html.escape(str(w))}</div>'
        "</div>"
        '<div class="mcc-metric-card">'
        '<div class="mcc-metric-label">Losses</div>'
        f'<div class="mcc-metric-value mcc-loss">{html.escape(str(l))}</div>'
        "</div>"
        '<div class="mcc-metric-card">'
        '<div class="mcc-metric-label">Draws / ties</div>'
        f'<div class="mcc-metric-value mcc-draw">{html.escape(str(d))}</div>'
        "</div>"
        '<div class="mcc-metric-card">'
        '<div class="mcc-metric-label">Games in progress</div>'
        f'<div class="mcc-metric-value mcc-progress">{html.escape(str(g))}</div>'
        "</div>"
        "</div>"
    )


def _last_day_of_month(d: date) -> date:
    last = calendar.monthrange(d.year, d.month)[1]
    return date(d.year, d.month, last)


_DEFAULT_START_DATE = date(2025, 10, 1)


def _season_first_year_oct_range(label: str) -> tuple[date, date]:
    """
    First calendar year in a PlayCricket-style label, e.g. 'Summer 2025/26' -> Oct 1–Oct 31 2025.
    """
    m = re.search(r"Summer\s+(\d{4})/", (label or "").strip())
    if not m:
        d0 = _DEFAULT_START_DATE
        return d0, _last_day_of_month(d0)
    y = int(m.group(1))
    start = date(y, 10, 1)
    end = date(y, 10, 31)
    return start, end


def _init_date_range_session_state() -> None:
    if "start_date" not in st.session_state:
        st.session_state["start_date"] = _DEFAULT_START_DATE
    if "end_date" not in st.session_state:
        st.session_state["end_date"] = _last_day_of_month(
            st.session_state["start_date"]
        )
    if "end_date_manually_set" not in st.session_state:
        st.session_state["end_date_manually_set"] = False
    if "_prev_start_date" not in st.session_state:
        st.session_state["_prev_start_date"] = st.session_state["start_date"]


def _on_end_date_changed() -> None:
    """User edited end date — stop auto-overwriting from start date."""
    if st.session_state.get("_fb_syncing_end_from_start"):
        return
    st.session_state["end_date_manually_set"] = True


def _on_fetch_clicked() -> None:
    """Ensure fetch + validation run only on an explicit button click, not on other reruns."""
    st.session_state["_fetch_requested"] = True


@st.cache_data(ttl=86_400, show_spinner="Loading season list…")
def _cached_season_labels() -> tuple[str, ...]:
    try:
        return tuple(discover_all_season_labels(headless=True))
    except Exception:
        return tuple(default_season_choices())


st.set_page_config(
    page_title="Mitcham Junior Stats",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
    /* Mitcham theme: light-dark() follows Streamlit color-scheme; vars keep surfaces on-theme. */
    :root {
      --mcc-yellow: #facc15;
      --mcc-yellow-soft: rgba(250, 204, 21, 0.14);
      --mcc-yellow-border: rgba(250, 204, 21, 0.4);
      --mcc-elite-green: #22c55e;
      --mcc-elite-green-soft: rgba(34, 197, 94, 0.16);
      --mcc-ink: #0f172a;
      --mcc-ink-muted: rgba(15, 23, 42, 0.82);
      --mcc-amber-ink: #92400e;
    }
    .stApp {
      background: light-dark(
        color-mix(in srgb, #fef9c3 28%, var(--background-color)),
        var(--background-color)
      );
      color: var(--text-color, inherit);
    }
    /* Hide default Streamlit chrome (deploy, menu bar, status) — app content unchanged */
    header[data-testid="stHeader"],
    div[data-testid="stHeader"] {
      display: none !important;
    }
    div[data-testid="stToolbar"],
    div[data-testid="stDecoration"],
    div[data-testid="stStatusWidget"],
    [data-testid="stDeployButton"],
    [data-testid="stToolbarActions"] {
      display: none !important;
    }
    .main .block-container {
      color: var(--text-color, inherit);
    }
    .block-container {
      padding-top: 0.85rem;
      padding-bottom: 2.5rem;
      max-width: 100%;
      padding-left: 2rem !important;
      padding-right: 2rem !important;
    }
    .mcc-header-shell {
      display: flex;
      align-items: flex-start;
      gap: 1.5rem;
      padding: 0.85rem 1rem 1.15rem 1rem;
      margin-bottom: 0.5rem;
      border-bottom: 1px solid var(--mcc-yellow-border);
      background: light-dark(
        linear-gradient(145deg, #1e293b 0%, #0f172a 55%, #1e293b 100%),
        transparent
      );
      border-radius: light-dark(12px, 0);
      box-shadow: light-dark(0 2px 12px rgba(15, 23, 42, 0.12), none);
    }
    .mcc-logo-cell {
      flex: 0 0 auto;
      padding: 1rem 1.1rem 1rem 0.75rem;
      display: flex;
      align-items: center;
      justify-content: center;
      align-self: flex-start;
      min-width: 0;
      overflow: visible;
    }
    .mcc-logo-cell img {
      max-height: 152px;
      width: auto;
      max-width: min(320px, 38vw);
      height: auto;
      object-fit: contain;
      object-position: left center;
      display: block;
      vertical-align: top;
    }
    .mcc-header-text {
      flex: 1 1 auto;
      min-width: 0;
      padding-top: 0.5rem;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .mcc-title {
      font-family: "Palatino Linotype", Palatino, "Book Antiqua", Georgia, serif;
      font-size: clamp(1.38rem, 2.6vw, 1.92rem);
      font-weight: 700;
      color: var(--mcc-yellow);
      letter-spacing: 0.02em;
      line-height: 1.22;
      margin: 0 0 0.4rem 0;
      text-shadow: light-dark(0 1px 2px rgba(0, 0, 0, 0.35), none);
    }
    .mcc-subtitle {
      font-size: 0.95rem;
      color: var(--text-color, inherit);
      opacity: 0.88;
      font-weight: 400;
      line-height: 1.45;
      margin: 0;
      max-width: 38rem;
    }
    section.main [data-testid="stSelectbox"] label,
    section.main [data-testid="stDateInput"] label,
    section.main [data-testid="stNumberInput"] label,
    section.main [data-testid="stCheckbox"] label,
    section.main [data-testid="stCheckbox"] p,
    section.main [data-testid="stCheckbox"] span {
      font-size: 0.72rem !important;
      text-transform: uppercase;
      letter-spacing: 0.07em;
      color: light-dark(var(--mcc-ink-muted), var(--mcc-yellow)) !important;
      opacity: 1 !important;
      font-weight: 600 !important;
    }
    section.main [data-testid="stCheckbox"] label {
      text-transform: none;
      font-size: 0.95rem !important;
      letter-spacing: 0.02em;
    }
    section.main [data-baseweb="select"] > div,
    section.main [data-baseweb="input"] input,
    section.main input[aria-label],
    section.main [data-testid="stDateInput"] input {
      border-color: var(--mcc-yellow-border) !important;
    }
    /* Fetch column: st.columns(..., vertical_alignment="bottom") does primary alignment */
    .toolbar-align-btn {
      display: flex;
      flex-direction: column;
      justify-content: flex-end;
      width: 100%;
      margin: 0;
      padding: 0;
    }
    @media (max-width: 1100px) {
      section.main div[data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
        row-gap: 0.65rem;
      }
    }
    section.main [data-testid="stButton"] button[kind="primary"],
    section.main [data-testid="stButton"] button[data-testid="baseButton-primary"] {
      background-color: var(--mcc-yellow) !important;
      color: #0a0a0a !important;
      border: 1px solid var(--mcc-yellow-border) !important;
      font-weight: 700 !important;
    }
    section.main [data-testid="stButton"] button[kind="primary"]:hover,
    section.main [data-testid="stButton"] button[data-testid="baseButton-primary"]:hover {
      background-color: #eab308 !important;
      color: #0a0a0a !important;
    }
    section[data-testid="stSidebar"] [data-testid="stButton"] button {
      border: 1px solid var(--mcc-yellow-border) !important;
      color: var(--text-color, inherit) !important;
      background-color: var(--secondary-background-color) !important;
    }
    .summary-card {
      background: light-dark(
        color-mix(in srgb, #ffffff 82%, #fef9c3),
        var(--secondary-background-color)
      );
      border: 1px solid var(--mcc-yellow-border);
      border-left: 4px solid var(--mcc-yellow);
      border-radius: 10px;
      padding: 1.05rem 1.25rem 1.1rem 1.25rem;
      margin: 1rem 0 1.15rem 0;
      box-shadow: light-dark(
        0 2px 14px rgba(15, 23, 42, 0.07),
        0 2px 14px rgba(0, 0, 0, 0.08)
      );
      font-family: Georgia, "Times New Roman", serif;
      font-size: 1.08rem;
      line-height: 1.55;
      color: var(--text-color, inherit);
    }
    .mcc-metrics-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 0.75rem 1rem;
      margin: 0.15rem 0 0.35rem 0;
    }
    @media (max-width: 640px) {
      .mcc-metrics-row {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    .mcc-metric-card {
      background: light-dark(
        color-mix(in srgb, #ffffff 88%, #fefce8),
        var(--secondary-background-color)
      );
      border: 1px solid var(--mcc-yellow-border);
      border-top: 3px solid var(--mcc-yellow);
      border-radius: 12px;
      padding: 0.85rem 1rem 1rem 1rem;
      box-shadow: light-dark(
        0 2px 10px rgba(15, 23, 42, 0.06),
        0 2px 10px rgba(0, 0, 0, 0.06)
      );
      color: var(--text-color, inherit);
      min-width: 0;
    }
    .mcc-metric-label {
      color: light-dark(var(--mcc-ink-muted), var(--text-color)) !important;
      opacity: 0.88;
      font-size: 0.82rem !important;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600 !important;
      margin-bottom: 0.35rem;
    }
    .mcc-metric-value {
      font-weight: 700 !important;
      font-size: 1.85rem !important;
      line-height: 1.15;
    }
    .mcc-metric-value.mcc-win {
      color: light-dark(#16a34a, #4ade80) !important;
    }
    .mcc-metric-value.mcc-loss {
      color: light-dark(#dc2626, #f87171) !important;
    }
    .mcc-metric-value.mcc-draw {
      color: light-dark(#b45309, #fbbf24) !important;
    }
    .mcc-metric-value.mcc-progress {
      color: light-dark(var(--mcc-ink), var(--text-color)) !important;
    }
    .section-heading {
      font-family: Palatino, Georgia, serif;
      font-size: 1.15rem;
      color: light-dark(var(--mcc-ink), var(--mcc-yellow)) !important;
      font-weight: 700;
      margin: 1.5rem 0 0.65rem 0;
      letter-spacing: 0.02em;
    }
    .mcc-section-heading {
      font-size: 1.22rem;
      padding-bottom: 0.35rem;
      border-bottom: 1px solid var(--mcc-yellow-border);
      margin-top: 1.65rem !important;
    }
    section.main [data-testid="stDataFrame"] {
      color: inherit !important;
      border: 1px solid var(--mcc-yellow-border);
      border-radius: 12px;
      overflow: hidden;
    }
    section.main [data-testid="stDataFrame"] [role="grid"],
    section.main [data-testid="stDataFrame"] [role="row"],
    section.main [data-testid="stDataFrame"] [role="cell"] {
      color: var(--text-color, inherit) !important;
    }
    section.main [data-testid="stDataFrame"] [role="columnheader"] {
      color: light-dark(var(--mcc-ink), var(--text-color)) !important;
      font-weight: 600 !important;
    }
    .fb-box textarea,
    section.main .fb-box textarea {
      border-radius: 10px !important;
      border: 1px solid var(--mcc-yellow-border) !important;
      color: var(--text-color, inherit) !important;
      background-color: var(--secondary-background-color) !important;
    }
    div[data-testid="stAlert"] {
      border: 1px solid var(--mcc-yellow-border) !important;
      background-color: var(--secondary-background-color) !important;
    }
    div[data-testid="column"] {
      min-width: 0 !important;
    }
    /* Cricbuzz-style highlight cards */
    .cbz-highlights {
      color: var(--text-color, inherit);
      margin: 0 0 1.25rem 0;
    }
    .cbz-highlights-title {
      font-family: Palatino, Georgia, serif;
      font-size: 1.18rem;
      font-weight: 700;
      color: light-dark(var(--mcc-ink), var(--mcc-yellow));
      margin: 1.25rem 0 0.75rem 0;
      letter-spacing: 0.02em;
    }
    .cbz-highlights + .cbz-highlights .cbz-highlights-title {
      margin-top: 0.55rem;
    }
    .cbz-highlights-empty {
      margin: 0;
      opacity: 0.8;
      font-size: 0.95rem;
      color: var(--text-color, inherit);
    }
    .cbz-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: 0.85rem 1rem;
      align-items: stretch;
    }
    @media (max-width: 640px) {
      .cbz-grid {
        grid-template-columns: 1fr;
      }
    }
    .cbz-card {
      background: light-dark(
        color-mix(in srgb, #ffffff 94%, #fef9c3),
        var(--secondary-background-color)
      );
      border: 1px solid var(--mcc-yellow-border);
      border-radius: 14px;
      box-shadow: light-dark(
        0 2px 12px rgba(15, 23, 42, 0.06),
        0 2px 12px rgba(0, 0, 0, 0.07)
      );
      color: var(--text-color, inherit);
      overflow: hidden;
      min-width: 0;
    }
    .cbz-card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.5rem;
      padding: 0.65rem 0.85rem;
      border-bottom: 1px solid var(--mcc-yellow-border);
      background: light-dark(
        color-mix(in srgb, #fffbeb 55%, #ffffff),
        var(--secondary-background-color)
      );
    }
    .cbz-team {
      font-weight: 700;
      font-size: 0.95rem;
      color: light-dark(var(--mcc-ink), var(--mcc-yellow));
      line-height: 1.3;
      min-width: 0;
    }
    .cbz-badge {
      flex-shrink: 0;
      font-size: 0.68rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 0.22rem 0.5rem;
      border-radius: 999px;
      border: 1px solid var(--mcc-yellow-border);
      color: light-dark(var(--mcc-amber-ink), var(--mcc-yellow));
      background: light-dark(
        rgba(250, 204, 21, 0.38),
        var(--mcc-yellow-soft)
      );
      opacity: 1;
    }
    .cbz-card-body {
      padding: 0.35rem 0.6rem 0.65rem 0.6rem;
    }
    .cbz-row {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 0.65rem;
      padding: 0.4rem 0.4rem;
      border-radius: 8px;
      font-size: 0.92rem;
      line-height: 1.45;
      color: var(--text-color, inherit);
    }
    .cbz-row--first {
      background: light-dark(
        rgba(250, 204, 21, 0.2),
        var(--mcc-yellow-soft)
      );
    }
    .cbz-row--elite {
      background: var(--mcc-elite-green-soft);
    }
    .cbz-name {
      flex: 1 1 auto;
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .cbz-stat {
      flex: 0 0 auto;
      font-variant-numeric: tabular-nums;
      font-weight: 600;
      text-align: right;
      color: var(--text-color, inherit);
    }
    .cbz-stat-elite {
      color: var(--mcc-elite-green);
      font-weight: 700;
    }
    section.main [data-testid="stCheckbox"] input[type="checkbox"] {
      accent-color: var(--mcc-yellow);
    }
    @supports not (color: light-dark(white, black)) {
      @media (prefers-color-scheme: light) {
        .stApp {
          background: color-mix(in srgb, #fef9c3 28%, var(--background-color)) !important;
        }
        .mcc-header-shell {
          background: linear-gradient(
            145deg,
            #1e293b 0%,
            #0f172a 55%,
            #1e293b 100%
          ) !important;
          border-radius: 12px !important;
        }
        .mcc-title {
          color: var(--mcc-yellow) !important;
        }
        .section-heading,
        .cbz-highlights-title {
          color: var(--mcc-ink) !important;
        }
        section.main [data-testid="stSelectbox"] label,
        section.main [data-testid="stDateInput"] label,
        section.main [data-testid="stNumberInput"] label,
        section.main [data-testid="stCheckbox"] label,
        section.main [data-testid="stCheckbox"] p,
        section.main [data-testid="stCheckbox"] span {
          color: var(--mcc-ink-muted) !important;
          opacity: 1 !important;
        }
        .mcc-metric-value.mcc-win {
          color: #16a34a !important;
        }
        .mcc-metric-value.mcc-loss {
          color: #dc2626 !important;
        }
        .mcc-metric-value.mcc-draw {
          color: #b45309 !important;
        }
        .mcc-metric-value.mcc-progress {
          color: var(--mcc-ink) !important;
        }
        .mcc-metric-card {
          background: color-mix(in srgb, #ffffff 88%, #fefce8) !important;
        }
        .cbz-team {
          color: var(--mcc-ink) !important;
        }
        .cbz-badge {
          color: var(--mcc-amber-ink) !important;
          background: rgba(250, 204, 21, 0.38) !important;
        }
        .cbz-card {
          background: color-mix(in srgb, #ffffff 94%, #fef9c3) !important;
        }
        .cbz-card-head {
          background: color-mix(in srgb, #fffbeb 55%, #ffffff) !important;
        }
        .cbz-row--first {
          background: rgba(250, 204, 21, 0.2) !important;
        }
        .summary-card {
          background: color-mix(in srgb, #ffffff 82%, #fef9c3) !important;
        }
      }
      @media (prefers-color-scheme: dark) {
        .mcc-title,
        .section-heading,
        .cbz-highlights-title {
          color: var(--mcc-yellow) !important;
        }
        section.main [data-testid="stSelectbox"] label,
        section.main [data-testid="stDateInput"] label,
        section.main [data-testid="stNumberInput"] label,
        section.main [data-testid="stCheckbox"] label,
        section.main [data-testid="stCheckbox"] p,
        section.main [data-testid="stCheckbox"] span {
          color: var(--mcc-yellow) !important;
          opacity: 0.88 !important;
        }
        .mcc-metric-value.mcc-win {
          color: #4ade80 !important;
        }
        .mcc-metric-value.mcc-loss {
          color: #f87171 !important;
        }
        .mcc-metric-value.mcc-draw {
          color: #fbbf24 !important;
        }
        .mcc-metric-value.mcc-progress {
          color: var(--text-color) !important;
        }
        .cbz-team {
          color: var(--mcc-yellow) !important;
        }
        .cbz-badge {
          color: var(--mcc-yellow) !important;
          background: var(--mcc-yellow-soft) !important;
        }
      }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "teams_cache" not in st.session_state:
    st.session_state["teams_cache"] = {}
if st.session_state.get("teams_cache_schema_v") != 2:
    st.session_state["teams_cache"] = {}
    st.session_state["teams_cache_schema_v"] = 2
if "scorecard_cache" not in st.session_state:
    st.session_state["scorecard_cache"] = {}

_init_date_range_session_state()

with st.sidebar:
    st.caption("Troubleshooting")
    if st.button(
        "Clear cached results",
        help="Clears the on-screen report and per-URL scorecard cache. Team list cache is kept.",
    ):
        st.session_state.pop("report", None)
        st.session_state["scorecard_cache"] = {}
        st.session_state.pop("_fetch_run_seq", None)
        logger.info(
            "[FetchRuntimeReset] fetch_scope_key=manual_clear cleared_transient_state=True "
            "cleared_report=True cleared_scorecard_cache=True cleared_locator_state=True "
            "context=app_user_clear"
        )
        st.rerun()

logo_uri = _logo_data_uri()
logo_html = (
    f'<div class="mcc-logo-cell"><img src="{logo_uri}" alt="Mitcham Cricket Club" /></div>'
    if logo_uri
    else '<div class="mcc-logo-cell"></div>'
)

st.markdown(
    f"""
    <div class="mcc-header-shell">
      {logo_html}
      <div class="mcc-header-text">
        <h1 class="mcc-title">Mitcham Cricket Club — Junior & Senior Stats</h1>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# Full list only — never derive from the current selection (avoids shrinking the dropdown).
full_season_options = list(_cached_season_labels()) or list(default_season_choices())
st.session_state["season_options"] = list(full_season_options)

if "selected_season" not in st.session_state:
    st.session_state["selected_season"] = (
        full_season_options[0] if full_season_options else ""
    )
elif (
    st.session_state["selected_season"] not in full_season_options
    and full_season_options
):
    st.session_state["selected_season"] = full_season_options[0]

t1, t2, t3, t4, t5, t6 = st.columns(
    [1.68, 1.02, 1.02, 0.92, 0.92, 1.22],
    gap="small",
    vertical_alignment="bottom",
)
with t1:
    st.selectbox("Season", options=full_season_options, key="selected_season")

if "previous_selected_season" not in st.session_state:
    st.session_state["previous_selected_season"] = st.session_state["selected_season"]
elif st.session_state["selected_season"] != st.session_state["previous_selected_season"]:
    s0, e0 = _season_first_year_oct_range(st.session_state["selected_season"])
    st.session_state["_fb_syncing_end_from_start"] = True
    try:
        st.session_state["start_date"] = s0
        st.session_state["end_date"] = e0
        st.session_state["end_date_manually_set"] = False
        st.session_state["_prev_start_date"] = s0
    finally:
        st.session_state["_fb_syncing_end_from_start"] = False
    st.session_state["previous_selected_season"] = st.session_state["selected_season"]

season = st.session_state["selected_season"]

with t2:
    st.date_input("Start date", key="start_date")
sd = st.session_state["start_date"]
pr = st.session_state["_prev_start_date"]
if sd != pr:
    if not st.session_state["end_date_manually_set"]:
        st.session_state["_fb_syncing_end_from_start"] = True
        try:
            st.session_state["end_date"] = _last_day_of_month(sd)
        finally:
            st.session_state["_fb_syncing_end_from_start"] = False
    st.session_state["_prev_start_date"] = sd
with t3:
    st.date_input("End date", key="end_date", on_change=_on_end_date_changed)

if st.session_state["end_date"] < st.session_state["start_date"]:
    st.session_state["end_date"] = st.session_state["start_date"]
    st.session_state["_show_date_range_reset_warning"] = True
    st.rerun()

if st.session_state.pop("_show_date_range_reset_warning", False):
    st.warning(
        "End date was before the start date; it has been reset to match the start date."
    )

d0_eff = st.session_state["start_date"]
d1_eff = st.session_state["end_date"]

with t4:
    min_runs = st.number_input("Min runs", min_value=0, max_value=400, value=20, step=1)
with t5:
    min_wkts = st.number_input("Min wickets", min_value=0, max_value=12, value=2, step=1)
with t6:
    st.markdown('<div class="toolbar-align-btn">', unsafe_allow_html=True)
    st.button(
        "Fetch",
        type="primary",
        width="stretch",
        on_click=_on_fetch_clicked,
        key="fetch_report",
    )
    st.markdown("</div>", unsafe_allow_html=True)

cb1, cb2, _ = st.columns([0.42, 0.42, 5.64])
with cb1:
    include_juniors = st.checkbox("Juniors", value=True, key="include_juniors")
with cb2:
    include_seniors = st.checkbox("Seniors", value=False, key="include_seniors")

# Scorecards are always loaded. Recovery/fallback parsing stays off in the scraper for speed.
include_scorecards = True
enable_recovery_parsing = False

_cur_scope = (
    f"{season}|{d0_eff.isoformat()}|{d1_eff.isoformat()}"
    f"|{int(include_juniors)}|{int(include_seniors)}|{int(include_scorecards)}"
    f"|{int(enable_recovery_parsing)}"
)
_prev_report = st.session_state.get("report")
if _prev_report is not None and _prev_report.get("fetch_scope_key") != _cur_scope:
    st.session_state.pop("report", None)
    st.session_state["scorecard_cache"] = {}

if st.session_state.pop("_fetch_requested", False):
    if not include_juniors and not include_seniors:
        st.error("Select at least one of Juniors or Seniors.")
        st.stop()
    if d0_eff > d1_eff:
        st.error("Start date cannot be after end date.")
        st.stop()
    _fetch_scope_app = (
        f"{season}|{d0_eff.isoformat()}|{d1_eff.isoformat()}"
        f"|{int(include_juniors)}|{int(include_seniors)}|{int(include_scorecards)}"
        f"|{int(enable_recovery_parsing)}"
    )
    st.session_state["_fetch_run_seq"] = int(st.session_state.get("_fetch_run_seq", 0)) + 1
    st.session_state.pop("_last_fetch_error", None)
    logger.info(
        "[FetchRuntimeReset] fetch_scope_key=%r cleared_transient_state=True "
        "cleared_report=False cleared_scorecard_cache=False cleared_locator_state=True "
        "context=app_fetch_start",
        _fetch_scope_app,
    )
    with st.status("Fetching…", expanded=True) as status:
        try:

            def _prog(msg: str) -> None:
                status.update(label=msg)

            data = run_report(
                season,
                d0_eff,
                d1_eff,
                min_runs=int(min_runs),
                min_wickets=int(min_wkts),
                headless=True,
                include_juniors=include_juniors,
                include_seniors=include_seniors,
                include_scorecards=include_scorecards,
                enable_recovery_parsing=enable_recovery_parsing,
                teams_cache=st.session_state["teams_cache"],
                scorecard_cache=st.session_state["scorecard_cache"],
                progress_callback=_prog,
            )
            status.update(label="Fetch completed", state="complete")
        except Exception as e:
            status.update(label="Failed.", state="error")
            st.error(f"Fetch failed: {e}")
            st.stop()

    st.session_state["report"] = data

if st.session_state.get("report") is not None:
    data = st.session_state["report"]

    summary = (data.get("summary_sentence") or "").strip()
    st.markdown(
        f'<div class="summary-card">{html.escape(summary)}</div>',
        unsafe_allow_html=True,
    )

    st.markdown(_match_metrics_html(data), unsafe_allow_html=True)

    st.markdown('<div style="height:0.65rem"></div>', unsafe_allow_html=True)
    st.markdown(
        render_cricbuzz_highlights(
            "Best with the Bat",
            _grouped_highlights_for_ui(data, "bat"),
            "bat",
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        render_cricbuzz_highlights(
            "Best with the Ball",
            _grouped_highlights_for_ui(data, "bowl"),
            "bowl",
        ),
        unsafe_allow_html=True,
    )

    st.markdown(
        '<p class="section-heading mcc-section-heading">Match results</p>',
        unsafe_allow_html=True,
    )
    rows = data.get("match_rows") or []
    if rows:
        mdf = pd.DataFrame(
            [
                {
                    "Date": r["date"],
                    "Mitcham Team": r.get("mitcham_team") or "",
                    "Opponent": r["opponent"],
                    "Result": _truncate(r["result"], 44),
                    "Status": r["status"],
                    "Match Link": r["match_url"],
                }
                for r in rows
            ]
        )
        st.dataframe(
            mdf,
            column_config={
                "Date": st.column_config.TextColumn("Date", width="small"),
                "Mitcham Team": st.column_config.TextColumn(
                    "Mitcham Team", width="large"
                ),
                "Opponent": st.column_config.TextColumn("Opponent", width="large"),
                "Result": st.column_config.TextColumn("Result", width="medium"),
                "Status": st.column_config.TextColumn("Status", width="small"),
                "Match Link": st.column_config.LinkColumn(
                    "Match Link", display_text="Open", width="small"
                ),
            },
            hide_index=True,
            width="stretch",
        )
    else:
        st.warning("No matches in the selected season and date range.")

    st.markdown(
        '<p class="section-heading mcc-section-heading">Facebook-ready summary</p>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="fb-box">', unsafe_allow_html=True)
    st.text_area(
        "Facebook summary",
        value=facebook_summary(data),
        height=260,
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)
