"""
Mitcham CC junior stats — Streamlit front-end for play.cricket.com.au (Playwright).
"""

import base64
import calendar
import html
from datetime import date
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

_ASSETS = Path(__file__).resolve().parent / "assets"
_LOGO_WEBP = _ASSETS / "mitcham_official_logo.webp"
_LOGO_PNG = _ASSETS / "mitcham_official_logo.png"


def _highlight_card(title: str, lines: list[str], empty_msg: str) -> str:
    if lines:
        body = "<ul>" + "".join(f"<li>{html.escape(t)}</li>" for t in lines) + "</ul>"
    else:
        body = f"<p style='color:#2a3d3a;margin:0'>{html.escape(empty_msg)}</p>"
    return (
        f'<div class="highlight-card">'
        f"<h4>{html.escape(title)}</h4>"
        f"{body}</div>"
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
      background: linear-gradient(165deg, #faf9f5 0%, #eef2f0 45%, #e8ece9 100%);
    }
    .mcc-header-shell {
      display: flex;
      align-items: flex-start;
      gap: 1.5rem;
      padding: 0.85rem 0.5rem 1.35rem 0.35rem;
      border-bottom: 1px solid rgba(12, 74, 69, 0.12);
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
      color: #0c4a45 !important;
      letter-spacing: 0.02em;
      line-height: 1.22;
      margin: 0 0 0.4rem 0;
      -webkit-text-fill-color: #0c4a45 !important;
    }
    .mcc-subtitle {
      font-size: 0.95rem;
      color: #2a3d3a !important;
      font-weight: 400;
      line-height: 1.45;
      margin: 0;
      max-width: 38rem;
      -webkit-text-fill-color: #2a3d3a !important;
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
      color: #2a3d3a !important;
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
      background: linear-gradient(135deg, #ffffff 0%, #f6faf8 100%);
      border-left: 4px solid #b8952f;
      border-radius: 0 10px 10px 0;
      padding: 1.05rem 1.25rem 1.1rem 1.25rem;
      margin: 1rem 0 1.15rem 0;
      box-shadow: 0 2px 14px rgba(12, 74, 69, 0.07);
      font-family: Georgia, "Times New Roman", serif;
      font-size: 1.08rem;
      line-height: 1.55;
      color: #142220 !important;
      -webkit-text-fill-color: #142220 !important;
    }
    div[data-testid="stMetric"] {
      background: #fff !important;
      border: 1px solid rgba(12, 74, 69, 0.1);
      border-radius: 12px;
      padding: 0.85rem 1rem 1rem 1rem;
      box-shadow: 0 2px 10px rgba(12, 74, 69, 0.06);
      color: #142220 !important;
    }
    div[data-testid="stMetricValue"] {
      color: #0c4a45 !important;
      font-weight: 700 !important;
      font-size: 1.85rem !important;
      -webkit-text-fill-color: #0c4a45 !important;
    }
    div[data-testid="stMetricLabel"] {
      color: #3d524f !important;
      font-size: 0.82rem !important;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600 !important;
      -webkit-text-fill-color: #3d524f !important;
    }
    .highlight-card {
      background: #fff !important;
      border: 1px solid rgba(12, 74, 69, 0.11);
      border-radius: 14px;
      padding: 1.15rem 1.25rem 1.35rem 1.25rem;
      box-shadow: 0 3px 16px rgba(12, 74, 69, 0.07);
      height: 100%;
      min-height: 120px;
      color: #142220 !important;
    }
    .highlight-card h4 {
      font-family: Palatino, Georgia, serif;
      font-size: 1.05rem;
      color: #0c4a45 !important;
      margin: 0 0 0.75rem 0;
      font-weight: 700;
      letter-spacing: 0.03em;
      -webkit-text-fill-color: #0c4a45 !important;
    }
    .highlight-card ul {
      margin: 0;
      padding-left: 1.1rem;
      color: #1a2e2c !important;
      line-height: 1.65;
      font-size: 0.98rem;
    }
    .highlight-card li {
      color: #1a2e2c !important;
    }
    .section-heading {
      font-family: Palatino, Georgia, serif;
      font-size: 1.15rem;
      color: #0c4a45 !important;
      font-weight: 700;
      margin: 1.5rem 0 0.65rem 0;
      letter-spacing: 0.02em;
      -webkit-text-fill-color: #0c4a45 !important;
    }
    section.main [data-testid="stDataFrame"] {
      color: #142220 !important;
    }
    section.main [data-testid="stDataFrame"] [role="grid"],
    section.main [data-testid="stDataFrame"] [role="row"],
    section.main [data-testid="stDataFrame"] [role="cell"] {
      color: #142220 !important;
      -webkit-text-fill-color: #142220 !important;
    }
    .fb-box textarea,
    section.main .fb-box textarea {
      border-radius: 10px !important;
      border: 1px solid rgba(12, 74, 69, 0.12) !important;
      color: #142220 !important;
      background-color: #ffffff !important;
      -webkit-text-fill-color: #142220 !important;
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

_init_date_range_session_state()

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
        <h1 class="mcc-title">Mitcham Cricket Club — Junior Stats</h1>
        <p class="mcc-subtitle">Weekend and date-range junior match highlights</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

season_options = list(_cached_season_labels())
if not season_options:
    season_options = list(default_season_choices())
t1, t2, t3, t4, t5, t6 = st.columns([2.0, 1.05, 1.05, 0.95, 0.95, 1.05])
with t1:
    season = st.selectbox("Season", options=season_options, index=0)
with t2:
    d0 = st.date_input("Start date", key="start_date")
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
    d1 = st.date_input("End date", key="end_date", on_change=_on_end_date_changed)
with t4:
    min_runs = st.number_input("Min runs", min_value=0, max_value=400, value=20, step=1)
with t5:
    min_wkts = st.number_input("Min wickets", min_value=0, max_value=12, value=2, step=1)
with t6:
    st.markdown('<div class="toolbar-align-btn">', unsafe_allow_html=True)
    fetch = st.button("Fetch", type="primary", width="stretch")
    st.markdown("</div>", unsafe_allow_html=True)

cb1, cb2, _ = st.columns([0.42, 0.42, 5.5])
with cb1:
    include_juniors = st.checkbox("Juniors", value=True, key="include_juniors")
with cb2:
    include_seniors = st.checkbox("Seniors", value=False, key="include_seniors")

_cur_scope = f"{season}|{d0}|{d1}|{int(include_juniors)}|{int(include_seniors)}"
_prev_report = st.session_state.get("report")
if _prev_report is not None and _prev_report.get("fetch_scope_key") != _cur_scope:
    st.session_state.pop("report", None)

if fetch:
    if not include_juniors and not include_seniors:
        st.error("Select at least one of Juniors or Seniors.")
        st.stop()
    with st.status("Fetching…", expanded=True) as status:
        try:

            def _prog(msg: str) -> None:
                status.update(label=msg)

            data = run_report(
                season,
                d0,
                d1,
                min_runs=int(min_runs),
                min_wickets=int(min_wkts),
                headless=True,
                include_juniors=include_juniors,
                include_seniors=include_seniors,
                teams_cache=st.session_state["teams_cache"],
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
    hb1, hb2 = st.columns(2, gap="large")
    bh = data.get("batting_highlights") or []
    bo = data.get("bowling_highlights") or []
    with hb1:
        st.markdown(
            _highlight_card(
                "Best with the bat",
                [r["formatted"] for r in bh],
                "No performances reached your minimum runs threshold.",
            ),
            unsafe_allow_html=True,
        )
    with hb2:
        st.markdown(
            _highlight_card(
                "Best with the ball",
                [r["formatted"] for r in bo],
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
