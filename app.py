# app.py
import time
import csv
import io
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

import streamlit as st
import requests
import cloudscraper

# ----------------- Constants -----------------
PULLPUSH = ("pullpush", "https://api.pullpush.io/reddit/search/submission/")
PUSHSHIFT = ("pushshift", "https://api.pushshift.io/reddit/submission/search/")
UA = "Mozilla/5.0 (compatible; Subreddit_TopAll_1000plus_1600chars/1.0; +https://example.org)"

DEFAULTS = dict(
    BATCH_SIZE=500,
    REQUEST_SLEEP=0.8,
    RETRY_SLEEP=3.0,
    MAX_RETRIES=3,
    TIMEOUT=30,
    CHUNK_DAYS=7,
    START_FROM_YEARS_AGO=20,
)

# ----------------- Helpers -----------------
def build_scraper():
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    s.headers.update({"User-Agent": UA})
    return s

def parse_subreddit_and_mode(url_text: str):
    s = (url_text or "").strip()
    if not s:
        return "", False

    if not s.lower().startswith("http"):
        name = s[2:] if s.lower().startswith("r/") else s
        return name, False

    try:
        p = urlparse(s)
        parts = [seg for seg in p.path.split("/") if seg]
        name = ""
        for i, seg in enumerate(parts):
            if seg.lower() == "r" and i + 1 < len(parts):
                name = parts[i + 1]
                break
        if not name and len(parts) >= 2 and parts[0].lower() == "r":
            name = parts[1]

        q = parse_qs(p.query)
        t = (q.get("t", [""])[0] or "").lower()
        is_top_path = any(seg.lower() == "top" for seg in parts)
        is_top_all = bool(is_top_path and (t == "all"))
        return name, is_top_all
    except Exception:
        return "", False

def is_deleted_or_removed(text: str) -> bool:
    if text is None:
        return False
    t = text.strip().lower()
    return t in ("[deleted]", "[removed]", "[deleted by user]")

def clean_paragraphs(original_text: str) -> str:
    if not original_text:
        return ""
    lines = original_text.split("\n")
    lines = [line.rstrip() for line in lines]
    paragraphs, current = [], []
    for line in lines:
        if line.strip() == "":
            if current:
                paragraphs.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        paragraphs.append("\n".join(current))
    return "\n\n".join(paragraphs)

def request_json(session, url: str, params: dict, label: str):
    tries = 0
    while tries < DEFAULTS["MAX_RETRIES"]:
        try:
            resp = session.get(url, params=params, timeout=DEFAULTS["TIMEOUT"])
            code = resp.status_code
            if 200 <= code < 300:
                return resp.json()
            if code in (403, 429, 500, 502, 503, 504):
                time.sleep(DEFAULTS["RETRY_SLEEP"]); tries += 1; continue
            return None
        except Exception:
            time.sleep(DEFAULTS["RETRY_SLEEP"]); tries += 1
    return None

def fetch_batch(primary, fallback, endpoint, subreddit, before_ts, after_ts, sort_type):
    name, url = endpoint
    params = {
        "subreddit": subreddit,
        "size": DEFAULTS["BATCH_SIZE"],
        "sort": "desc",
        "sort_type": sort_type,
        "before": before_ts,
        "after": after_ts,
    }
    data = request_json(primary, url, params, name)
    if data is None:
        # switch endpoint
        other = PUSHSHIFT if endpoint == PULLPUSH else PULLPUSH
        name, url = other
        data = request_json(fallback, url, params, name)
        endpoint = other
    # normalize result list
    if data is None:
        return endpoint, []
    if isinstance(data, dict) and "data" in data:
        return endpoint, data["data"]
    if isinstance(data, list):
        return endpoint, data
    if isinstance(data, dict) and "results" in data:
        return endpoint, data["results"]
    return endpoint, []

def time_range_bounds(choice: str):
    now = datetime.now(timezone.utc)
    if choice == "This Week":
        start = now - timedelta(days=7)
    elif choice == "This Month":
        start = now - timedelta(days=30)
    elif choice == "This Year":
        start = now - timedelta(days=365)
    else:
        start = now - timedelta(days=365 * DEFAULTS["START_FROM_YEARS_AGO"])
    return start, now

# ----------------- Streamlit UI -----------------
st.set_page_config(page_title="Reddit Scraper — Free", page_icon="🧲", layout="wide")

st.markdown("## Scraping Control Panel")
st.caption("Enter a subreddit, choose your settings, and start the crawl.")

c1, c2 = st.columns([2, 1])
with c1:
    subreddit_in = st.text_input("Subreddit", value="AITAH", placeholder="AITAH or r/AITAH or full URL")
with c2:
    max_stories = st.number_input("Max Stories (optional)", min_value=0, value=200, step=50)

c3, c4 = st.columns(2)
with c3:
    time_range = st.selectbox("Time Range", ["This Week", "This Month", "This Year", "All Time"], index=3)
with c4:
    st.write("")  # spacer
    st.write("")  # spacer

c5, c6 = st.columns(2)
with c5:
    min_comments = st.number_input("Minimum Comments", min_value=0, value=1000, step=50)
with c6:
    min_chars = st.number_input("Minimum Characters", min_value=0, value=1600, step=50)

adv_exp = st.expander("Advanced (keep defaults unless needed)")
with adv_exp:
    DEFAULTS["BATCH_SIZE"] = st.number_input("BATCH_SIZE", 100, 1000, DEFAULTS["BATCH_SIZE"], step=100)
    DEFAULTS["CHUNK_DAYS"] = st.number_input("CHUNK_DAYS", 1, 14, DEFAULTS["CHUNK_DAYS"])
    DEFAULTS["REQUEST_SLEEP"] = st.number_input("REQUEST_SLEEP (seconds)", 0.0, 5.0, DEFAULTS["REQUEST_SLEEP"])
    DEFAULTS["RETRY_SLEEP"] = st.number_input("RETRY_SLEEP (seconds)", 0.0, 10.0, DEFAULTS["RETRY_SLEEP"])
    DEFAULTS["MAX_RETRIES"] = st.number_input("MAX_RETRIES", 1, 10, DEFAULTS["MAX_RETRIES"])

btn_start = st.button("🚀 Start Crawl", type="primary")
btn_stop = st.button("⛔ Stop")

# ----------------- Session State -----------------
if "running" not in st.session_state:
    st.session_state.running = False
if "paused" not in st.session_state:
    st.session_state.paused = False
if "kept" not in st.session_state:
    st.session_state.kept = 0
if "seen" not in st.session_state:
    st.session_state.seen = set()
if "csv_buf" not in st.session_state:
    st.session_state.csv_buf = io.StringIO()
if "csv_writer" not in st.session_state:
    w = csv.writer(st.session_state.csv_buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    w.writerow(["id", "title", "selftext", "score", "num_comments", "created_utc", "permalink", "full_url"])
    st.session_state.csv_writer = w
if "progress" not in st.session_state:
    st.session_state.progress = dict(scanned_batches=0, scanned_windows=0, api="pullpush", next_before=None)

# ----------------- Start / Stop logic -----------------
if btn_start:
    st.session_state.running = True
    st.session_state.paused = False
if btn_stop:
    st.session_state.running = False
    st.session_state.paused = True

# ----------------- Crawl -----------------
status = st.empty()
logbox = st.empty()
progress_bar = st.progress(0)

# detect subreddit + mode
subreddit, is_top_all = parse_subreddit_and_mode(subreddit_in)
mode_label = "top-all (score)" if is_top_all else "chronological (created_utc)"
st.info(f"Mode: **{mode_label}**  |  Range: **{time_range}**")

if st.session_state.running and subreddit:
    # prepare clients
    primary = build_scraper()
    fallback = requests.Session()
    fallback.headers.update({"User-Agent": UA})

    # bounds & window setup
    start_dt, end_dt = time_range_bounds(time_range)
    oldest_time = start_dt
    window_end = end_dt
    sort_type = "score" if is_top_all else "created_utc"

    while st.session_state.running and window_end > oldest_time:
        window_start = max(oldest_time, window_end - timedelta(days=DEFAULTS["CHUNK_DAYS"]))
        after_ts = int(window_start.timestamp())
        before_ts = int(window_end.timestamp())
        endpoint = PULLPUSH
        before_cursor = None

        st.session_state.progress["scanned_windows"] += 1

        while st.session_state.running:
            ep_used, data = fetch_batch(primary, fallback, endpoint, subreddit, before_cursor or before_ts, after_ts, sort_type)
            endpoint = ep_used
            st.session_state.progress["api"] = endpoint[0]

            if not data:
                break

            # normalize & filter
            kept_in_batch = 0
            for d in data:
                sid = (d or {}).get("id")
                if not sid or sid in st.session_state.seen:
                    continue

                title = (d.get("title") or "")
                selftext = d.get("selftext") or d.get("self_text") or ""
                if is_deleted_or_removed(selftext):
                    continue
                if (d.get("num_comments", d.get("comments", 0)) or 0) < min_comments:
                    continue
                text_clean = clean_paragraphs(selftext)
                if len(text_clean) < min_chars:
                    continue

                perma = d.get("permalink") or (f"/r/{subreddit}/comments/{sid}/" if sid else "")
                full_url = f"https://www.reddit.com{perma}" if perma.startswith("/") else perma
                score = d.get("score", 0) or 0
                created = d.get("created_utc") or d.get("created") or ""

                st.session_state.csv_writer.writerow([sid, title, text_clean, score, d.get("num_comments", d.get("comments", 0)) or 0, created, perma, full_url])
                st.session_state.seen.add(sid)
                st.session_state.kept += 1
                kept_in_batch += 1

                # respect limit
                if max_stories and max_stories > 0 and st.session_state.kept >= max_stories:
                    st.session_state.running = False
                    break

            # update cursors
            try:
                oldest_in_batch = min(int((d.get("created_utc") or d.get("created") or 0)) for d in data if d)
                before_cursor = oldest_in_batch - 1
            except Exception:
                before_cursor = None

            st.session_state.progress["scanned_batches"] += 1
            st.session_state.progress["next_before"] = before_cursor

            # UI progress
            if max_stories and max_stories > 0:
                progress = min(1.0, st.session_state.kept / max_stories)
            else:
                progress = 0.0
            progress_bar.progress(progress)

            status.write(f"**Saved:** {st.session_state.kept}  |  **Batches:** {st.session_state.progress['scanned_batches']}  |  **Windows:** {st.session_state.progress['scanned_windows']}  |  **API:** {st.session_state.progress['api']}  |  **next_before:** {before_cursor}")

            logbox.code(
                f"Window: {datetime.utcfromtimestamp(after_ts)} → {datetime.utcfromtimestamp(before_ts)} UTC\n"
                f"Batch kept: {kept_in_batch}\n"
                f"Total kept: {st.session_state.kept}\n"
                f"Mode: {mode_label}\n",
                language="text",
            )

            time.sleep(DEFAULTS["REQUEST_SLEEP"])

            if not st.session_state.running:
                break
            if before_cursor is not None and before_cursor <= after_ts:
                break

        window_end = window_start

# ----------------- Download CSV -----------------
st.markdown("---")
csv_bytes = st.session_state.csv_buf.getvalue().encode("utf-8")
st.download_button("⬇️ Download CSV", data=csv_bytes, file_name=f"{subreddit or 'subreddit'}.csv", mime="text/csv")

# Info about resume/stop
if st.session_state.paused:
    st.warning("Stopped. Progress is saved in memory. Keep this tab open to resume.")
