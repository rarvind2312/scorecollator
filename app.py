"""
Mitcham CC junior stats — Streamlit front-end for play.cricket.com.au (Playwright).
"""

import base64
import logging
import calendar
import html
import re
from datetime import date
from typing import Literal
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from scraper import (
    default_season_choices,
    discover_all_season_labels,
    facebook_summary,
    run_report,
)

logger = logging.getLogger(__name__)

_ASSETS = Path(__file__).resolve().parent / "assets"
_LOGO_WEBP = _ASSETS / "mitcham_official_logo.webp"
_LOGO_PNG = _ASSETS / "mitcham_official_logo.png"


def _highlight_card(
    title: str,
    lines: list[str],
    empty_msg: str,
    *,
    card_extra_class: str = "",
) -> str:
    extra = f" {card_extra_class}" if card_extra_class else ""
    if lines:
        body = "<ul>" + "".join(f"<li>{html.escape(t)}</li>" for t in lines) + "</ul>"
    else:
        body = (
            f"<p style='margin:0;opacity:0.82'>{html.escape(empty_msg)}</p>"
        )
    return (
        f'<div class="highlight-card{extra}">'
        f"<h4>{html.escape(title)}</h4>"
        f"{body}</div>"
    )


def _highlight_rows_ordered_flat(data: dict, kind: Literal["bat", "bowl"]) -> list[dict]:
    """
    Full highlight rows in run_report order: prefer flat lists; if absent, flatten grouped_*.
    """
    fkey = "batting_highlights" if kind == "bat" else "bowling_highlights"
    gkey = (
        "grouped_batting_highlights" if kind == "bat" else "grouped_bowling_highlights"
    )
    flat = data.get(fkey)
    if flat is not None:
        return list(flat)
    grouped = data.get(gkey)
    if grouped is not None:
        out: list[dict] = []
        for g in grouped:
            out.extend(g.get("entries") or [])
        return out
    return []


def _format_highlight_line_with_team(row: dict) -> str:
    """e.g. Mitcham U14 (1): Player – 29"""
    fmt = str(row.get("formatted") or "").strip()
    if not fmt:
        return ""
    team = (row.get("mitcham_team") or "").strip() or "Mitcham"
    return f"{team}: {fmt}"


def render_full_highlight_list(
    title: str,
    rows: list[dict],
    empty_msg: str,
) -> str:
    """Single card, one list, all qualifying entries (no expanders, no cap)."""
    lines: list[str] = []
    for r in rows:
        line = _format_highlight_line_with_team(r)
        if line:
            lines.append(line)
    return _highlight_card(
        title,
        lines,
        empty_msg,
        card_extra_class="highlight-card-full-list",
    )


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
    .block-container {
      padding-top: 0.85rem;
      padding-bottom: 2.5rem;
      max-width: 100%;
      padding-left: 2rem !important;
      padding-right: 2rem !important;
    }
    .stApp {
      background: var(--background-color);
      color: inherit;
    }
    .mcc-header-shell {
      display: flex;
      align-items: flex-start;
      gap: 1.5rem;
      padding: 0.85rem 0.5rem 1.35rem 0.35rem;
      border-bottom: 1px solid var(--border-color, rgba(128, 128, 128, 0.22));
      margin-bottom: 0.35rem;
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
      color: var(--primary-color, inherit);
      letter-spacing: 0.02em;
      line-height: 1.22;
      margin: 0 0 0.4rem 0;
    }
    .mcc-subtitle {
      font-size: 0.95rem;
      color: inherit;
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
      color: inherit !important;
      opacity: 0.88;
      font-weight: 600 !important;
    }
    section.main [data-testid="stCheckbox"] label {
      text-transform: none;
      font-size: 0.95rem !important;
      letter-spacing: 0.02em;
    }
    .toolbar-align-btn {
      display: flex;
      align-items: flex-end;
      padding-bottom: 0.12rem;
    }
    .summary-card {
      background: var(--secondary-background-color);
      border-left: 4px solid var(--primary-color, rgba(184, 149, 47, 0.85));
      border-radius: 0 10px 10px 0;
      padding: 1.05rem 1.25rem 1.1rem 1.25rem;
      margin: 1rem 0 1.15rem 0;
      box-shadow: 0 2px 14px rgba(0, 0, 0, 0.06);
      font-family: Georgia, "Times New Roman", serif;
      font-size: 1.08rem;
      line-height: 1.55;
      color: inherit;
    }
    div[data-testid="stMetric"] {
      background: var(--secondary-background-color) !important;
      border: 1px solid var(--border-color, rgba(128, 128, 128, 0.28));
      border-radius: 12px;
      padding: 0.85rem 1rem 1rem 1rem;
      box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
      color: inherit !important;
    }
    div[data-testid="stMetricValue"] {
      color: var(--primary-color, inherit) !important;
      font-weight: 700 !important;
      font-size: 1.85rem !important;
    }
    div[data-testid="stMetricLabel"] {
      color: inherit !important;
      opacity: 0.82;
      font-size: 0.82rem !important;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600 !important;
    }
    .highlight-card {
      background: var(--secondary-background-color) !important;
      border: 1px solid var(--border-color, rgba(128, 128, 128, 0.26));
      border-radius: 14px;
      padding: 1.15rem 1.25rem 1.35rem 1.25rem;
      box-shadow: 0 3px 16px rgba(0, 0, 0, 0.06);
      height: 100%;
      min-height: 120px;
      color: inherit !important;
    }
    .highlight-card h4 {
      font-family: Palatino, Georgia, serif;
      font-size: 1.05rem;
      color: var(--primary-color, inherit) !important;
      margin: 0 0 0.75rem 0;
      font-weight: 700;
      letter-spacing: 0.03em;
    }
    .highlight-card ul {
      margin: 0;
      padding-left: 1.1rem;
      color: inherit !important;
      line-height: 1.65;
      font-size: 0.98rem;
    }
    .highlight-card li {
      color: inherit !important;
    }
    .highlight-team-name {
      font-family: Palatino, Georgia, serif;
      font-size: 1.0rem;
      font-weight: 700;
      color: var(--primary-color, inherit) !important;
      margin: 0.75rem 0 0.35rem 0;
    }
    .highlight-card .highlight-team-name:first-of-type {
      margin-top: 0.35rem;
    }
    .highlight-card.highlight-card-full-list {
      height: auto !important;
      min-height: 0 !important;
      margin-bottom: 0.75rem;
    }
    .highlight-card.highlight-card-full-list ul {
      font-size: 0.92rem;
      line-height: 1.55;
    }
    .highlight-card.highlight-card-full-list li {
      word-wrap: break-word;
      overflow-wrap: anywhere;
    }
    .section-heading {
      font-family: Palatino, Georgia, serif;
      font-size: 1.15rem;
      color: var(--primary-color, inherit) !important;
      font-weight: 700;
      margin: 1.5rem 0 0.65rem 0;
      letter-spacing: 0.02em;
    }
    section.main [data-testid="stDataFrame"] {
      color: inherit !important;
    }
    section.main [data-testid="stDataFrame"] [role="grid"],
    section.main [data-testid="stDataFrame"] [role="row"],
    section.main [data-testid="stDataFrame"] [role="cell"] {
      color: inherit !important;
    }
    .fb-box textarea,
    section.main .fb-box textarea {
      border-radius: 10px !important;
      border: 1px solid var(--border-color, rgba(128, 128, 128, 0.28)) !important;
      color: inherit !important;
      background-color: var(--secondary-background-color) !important;
    }
    div[data-testid="column"] {
      min-width: 0 !important;
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

t1, t2, t3, t4, t5, t6 = st.columns([2.0, 1.05, 1.05, 0.95, 0.95, 1.05])
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

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Wins", data["wins"])
    k2.metric("Losses", data["losses"])
    k3.metric("Draws / ties", data["draws"])
    k4.metric("Games in progress", data.get("in_progress", 0))

    st.markdown('<div style="height:0.65rem"></div>', unsafe_allow_html=True)
    st.markdown(
        render_full_highlight_list(
            "Best with the bat",
            _highlight_rows_ordered_flat(data, "bat"),
            "No performances reached your minimum runs threshold.",
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        render_full_highlight_list(
            "Best with the ball",
            _highlight_rows_ordered_flat(data, "bowl"),
            "No performances reached your minimum wickets threshold.",
        ),
        unsafe_allow_html=True,
    )

    st.markdown('<p class="section-heading">Match results</p>', unsafe_allow_html=True)
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

    st.markdown('<p class="section-heading">Facebook-ready summary</p>', unsafe_allow_html=True)
    st.markdown('<div class="fb-box">', unsafe_allow_html=True)
    st.text_area(
        "Facebook summary",
        value=facebook_summary(data),
        height=260,
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)
