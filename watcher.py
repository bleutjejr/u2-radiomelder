#!/usr/bin/env python3
"""U2 radio watcher for JOE (DPG Media) and Radio 10 (Talpa Network).

Polls both stations ~every 30 s and calls notify() once per U2 play.
Only polls a station on its active dates (Europe/Amsterdam, whole days);
exits by itself once every window has ended.

Sources (all reachable from the box without auth):
  JOE      primary : ICY metadata in the official stream
                     https://icecast-qmusicnl-cdp.triple-it.nl/Joe_nl.mp3  ("Title - ARTIST")
           backup  : relisten.nl playlist page (has exact start times, ~30 tracks history)
           (official api.joe.nl/2.4/tracks/plays is blocked for the box IP by DPG's WAF)
  Radio 10 primary : Triton Digital now-playing XML (artist, title, start ms, duration)
                     https://np.tritondigital.com/public/nowplaying?mountName=RADIO10&numberToFetch=3&eventType=track
           backup  : ICY metadata in the Triton stream ("Artist - Title")

The ntfy.sh topic is read only from the NTFY_TOPIC environment variable.
Nothing in this repo stores the topic.

Usage:
  NTFY_TOPIC=... python3 watcher.py                 # run loop (respects schedule)
  python3 watcher.py --once                         # single poll, then exit
  python3 watcher.py --ignore-schedule --duration 120   # test run
  python3 watcher.py --selftest                     # run matcher tests
  python3 watcher.py --probe                        # check sources, no notifications
  python3 watcher.py --check-schedule               # exit 3 after 2026-11-05
  NTFY_TOPIC=... python3 watcher.py --test-push     # one labelled test notification
"""
import argparse, datetime as dt, html, json, logging, os, re, signal, sys, time
import urllib.request
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(HERE, "watcher.log")
STATE_FILE = os.path.join(HERE, "state.json")
TZ = ZoneInfo("Europe/Amsterdam")
POLL_SECONDS = 30
DEDUPE_WINDOW = 30 * 60       # same station+song not re-alerted within 30 min
CATCHUP_WINDOW = 15 * 60      # history items older than this are never alerted
UA = "Mozilla/5.0 (X11; Linux x86_64) u2-radio-watcher/1.0"
NTFY_SERVER = "https://ntfy.sh"


def _days(start, end):
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    return {s + dt.timedelta(n) for n in range((e - s).days + 1)}

# Active dates per station (local Europe/Amsterdam calendar days, inclusive).
SCHEDULE = {
    "JOE": _days("2026-10-29", "2026-10-30") | _days("2026-11-02", "2026-11-05"),
    "Radio 10": _days("2026-11-02", "2026-11-05"),
}

# ---------------------------------------------------------------- matching
U2_RE = re.compile(r"(?<![\w])U\s?2(?![\w])", re.IGNORECASE)

def is_u2(artist: str) -> bool:
    """True if 'U2' appears as a standalone token in the artist string.
    Matches 'U2', 'u2', 'U2 & Green Day', 'Jam & Spoon feat. U2';
    rejects 'UB40', 'U23', 'MU2', 'U2K'."""
    return bool(artist and U2_RE.search(artist))

def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip().lower())

# ---------------------------------------------------------------- fetchers
def http_get(url, timeout=12, headers=None):
    h = {"User-Agent": UA}; h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def icy_title(url, timeout=12):
    req = urllib.request.Request(url, headers={"Icy-MetaData": "1", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        mi = int(r.headers.get("icy-metaint") or 0)
        if not mi:
            raise RuntimeError("no icy-metaint")
        for _ in range(3):
            r.read(mi)
            n = r.read(1)[0] * 16
            if n:
                meta = r.read(n).rstrip(b"\0").decode("utf-8", "replace")
                m = re.search(r"StreamTitle='(.*?)';", meta)
                return m.group(1).strip() if m else ""
    return ""

def joe_icy():
    t = icy_title("https://icecast-qmusicnl-cdp.triple-it.nl/Joe_nl.mp3")
    if " - " not in t:
        return []
    title, artist = t.rsplit(" - ", 1)          # JOE format: "Title - ARTIST"
    return [{"artist": artist.strip(), "title": title.strip(), "start": None, "src": "joe-icy"}]

def joe_relisten():
    page = http_get("https://www.relisten.nl/playlists/joe")
    i = page.find('id="playlist"')          # skip the "Nieuwe tracks" sidebar
    page = page[i:] if i >= 0 else ""
    out = []
    for li in re.findall(r'<li class="media" itemprop="track".*?</li>', page, re.S):
        t = re.search(r'itemprop="name">([^<]+)', li)
        a = re.search(r'itemprop="byArtist">([^<]+)', li)
        ts = re.search(r'title="(\d\d-\d\d-\d{4} \d\d:\d\d:\d\d)"', li)
        if t and a and ts:
            start = dt.datetime.strptime(ts.group(1), "%d-%m-%Y %H:%M:%S").replace(tzinfo=TZ)
            out.append({"artist": html.unescape(a.group(1)).strip(), "title": html.unescape(t.group(1)).strip(),
                        "start": start.timestamp(), "src": "relisten"})
    return out[:5]

def radio10_triton():
    x = http_get("https://np.tritondigital.com/public/nowplaying?mountName=RADIO10&numberToFetch=3&eventType=track")
    out = []
    for info in ET.fromstring(x).findall("nowplaying-info"):
        p = {e.get("name"): (e.text or "").strip() for e in info.findall("property")}
        start = p.get("cue_time_start")
        out.append({"artist": p.get("track_artist_name", ""), "title": p.get("cue_title", ""),
                    "start": int(start) / 1000 if start else float(info.get("timestamp")),
                    "duration": int(p.get("cue_time_duration") or 0) / 1000, "src": "triton"})
    return out

def radio10_icy():
    t = icy_title("https://playerservices.streamtheworld.com/api/livestream-redirect/RADIO10.mp3")
    if " - " not in t:
        return []
    artist, title = t.split(" - ", 1)           # Triton format: "Artist - Title"
    return [{"artist": artist.strip(), "title": title.strip(), "start": None, "src": "r10-icy"}]

STATIONS = {
    "JOE": [joe_icy, joe_relisten],          # both are queried (ICY = instant, relisten = history/start times)
    "Radio 10": [radio10_triton, radio10_icy],  # ICY used only if Triton fails
}
QUERY_ALL = {"JOE": True, "Radio 10": False}

# ---------------------------------------------------------------- notify (ntfy.sh push)
WHATSAPP_R10 = "https://wa.me/31357505900?text=tickets"
JOE_LINK = "https://joe.nl"   # joe.nl/* is a universal/app link for the JOE app (nl.joe.app)

def ntfy_topic():
    return os.environ.get("NTFY_TOPIC", "").strip()

def ntfy_publish(title, message, click=None, actions=None, priority=4, tags=None):
    """Publish via ntfy JSON API (UTF-8 safe). Returns HTTP status."""
    topic = ntfy_topic()
    if not topic:
        raise RuntimeError("NTFY_TOPIC is not set")
    body = {"topic": topic, "title": title, "message": message, "priority": priority}
    if click: body["click"] = click
    if actions: body["actions"] = actions
    if tags: body["tags"] = tags
    req = urllib.request.Request(NTFY_SERVER,
                                 data=json.dumps(body).encode("utf-8"),
                                 headers={"Content-Type": "application/json", "User-Agent": UA}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status

def build_message(station, track):
    if station == "Radio 10":
        return dict(title="U2 op Radio 10!",
                    message=f"Nu: {track['title']} van U2. App 'tickets' naar Radio 10.",
                    click=WHATSAPP_R10,
                    actions=[{"action": "view", "label": "WhatsApp tickets", "url": WHATSAPP_R10}])
    return dict(title="U2 op JOE!",
                message=f"Nu: {track['title']} van U2. Stuur 'U2' in de JOE-app.",
                click=JOE_LINK,
                actions=[{"action": "view", "label": "Open JOE", "url": JOE_LINK}])

def notify(station, track):
    when = dt.datetime.fromtimestamp(track["start"], TZ).strftime("%H:%M") if track.get("start") else "nu"
    logging.warning("U2 ALERT: %s speelt nu '%s' van %s (start %s, bron %s)",
                    station, track["title"], track["artist"], when, track["src"])
    for attempt in range(3):
        try:
            status = ntfy_publish(tags=["guitar"], **build_message(station, track))
            logging.warning("ntfy push sent (HTTP %s)", status)
            return
        except Exception as e:
            logging.error("ntfy push failed (attempt %d): %s", attempt + 1, e)
            time.sleep(3)

def send_test_push():
    """One clearly labelled test. Not a song alert."""
    when = dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M %Z")
    status = ntfy_publish(
        title="Test: U2-melder draait op GitHub",
        message=f"Eenmalige test vanaf GitHub Actions ({when}). Dit is geen U2-melding.",
        priority=3,
        tags=["white_check_mark"],
    )
    print(f"test push sent HTTP {status}")
    return status

# ---------------------------------------------------------------- state
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"alerted": {}}

def save_state(st):
    now = time.time()
    st["alerted"] = {k: v for k, v in st["alerted"].items() if now - v < 7 * 86400}
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE_FILE)

def handle(station, track, st):
    """Fire notify once per play. Key = station + title; a play counts as new if
    no alert for that station+title in the last 30 min (covers ICY items without
    a start time and the same play reported by two sources)."""
    now = time.time()
    if track.get("start") and now - track["start"] > CATCHUP_WINDOW:
        return False
    key = f"{station}|{norm(track['title'])}"
    last = st["alerted"].get(key)
    if last and now - last < DEDUPE_WINDOW:
        return False
    st["alerted"][key] = now
    st.setdefault("plays", []).append({"station": station, **track, "alerted_at": now})
    st["plays"] = st["plays"][-200:]
    save_state(st)
    notify(station, track)
    return True

# ---------------------------------------------------------------- loop
def active(station, today):
    return today in SCHEDULE[station]

def poll(st, ignore_schedule=False):
    today = dt.datetime.now(TZ).date()
    for station, fetchers in STATIONS.items():
        if not ignore_schedule and not active(station, today):
            continue
        got_any = False
        for f in fetchers:
            if got_any and not QUERY_ALL[station]:
                break
            try:
                tracks = f()
            except Exception as e:
                logging.error("%s: %s failed: %s", station, f.__name__, e)
                continue
            got_any = got_any or bool(tracks)
            if tracks:
                cur = tracks[0]
                st_ = dt.datetime.fromtimestamp(cur["start"], TZ).strftime("%d-%m %H:%M:%S") if cur.get("start") else "-"
                logging.info("%-8s [%s] now/latest: %s - %s (start %s)", station, f.__name__, cur["artist"], cur["title"], st_)
            for t in tracks:
                if is_u2(t["artist"]):
                    handle(station, t, st)

def finished():
    last = max(max(d) for d in SCHEDULE.values())
    return dt.datetime.now(TZ).date() > last

def selftest():
    yes = ["U2", "u2", "U2 & Green Day", "Jam & Spoon feat. U2", "U2 / Bono", "U2, Kygo", "Kygo x U2", "U 2"]
    no = ["UB40", "U23", "MU2", "U2K", "U-ZiQ", "Bono", "Sniff 'N' The Tears", "", "Supertramp"]
    ok = all(is_u2(a) for a in yes) and not any(is_u2(a) for a in no)
    for a in yes + no:
        print(f"{a!r:30} -> {is_u2(a)}")
    print("SELFTEST", "OK" if ok else "FAILED")
    return ok

def probe():
    """Hit every configured source once. Prints results and sends nothing."""
    failed = []
    for station, fetchers in STATIONS.items():
        station_ok = False
        for f in fetchers:
            try:
                tracks = f()
            except Exception as e:
                failed.append(f.__name__)
                print(f"PROBE {station} {f.__name__} FAIL {type(e).__name__}: {e}")
                continue
            if not tracks:
                failed.append(f.__name__)
                print(f"PROBE {station} {f.__name__} EMPTY")
                continue
            station_ok = True
            cur = tracks[0]
            print(f"PROBE {station} {f.__name__} OK n={len(tracks)} artist={cur.get('artist')!r} title={cur.get('title')!r} src={cur.get('src')}")
        if not station_ok:
            print(f"PROBE {station} NO_WORKING_SOURCE")
    ok = not failed
    print("PROBE", "OK" if ok else "DEGRADED", "failed=" + (",".join(failed) if failed else "-"))
    return ok

def _stop(signum, _frame):
    logging.info("signal %s, exiting", signum)
    raise SystemExit(0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--ignore-schedule", action="store_true", help="poll both stations regardless of dates (testing)")
    ap.add_argument("--duration", type=int, default=0, help="stop after N seconds (testing)")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--probe", action="store_true", help="check sources once, never notify")
    ap.add_argument("--check-schedule", action="store_true", help="exit 3 when every window has ended")
    ap.add_argument("--test-push", action="store_true", help="send one labelled test notification and exit")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if a.check_schedule:
        sys.exit(3 if finished() else 0)
    if a.probe:
        sys.exit(0 if probe() else 1)
    if a.test_push:
        try:
            send_test_push()
        except Exception as e:
            print(f"test push failed: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)
        return
    signal.signal(signal.SIGTERM, _stop)
    logging.Formatter.converter = lambda *x: dt.datetime.now(TZ).timetuple()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])
    st = load_state()
    t0 = time.time()
    today = dt.datetime.now(TZ).date()
    logging.info("watcher start (ignore_schedule=%s, NTFY_TOPIC_set=%s)", a.ignore_schedule, bool(ntfy_topic()))
    for station in STATIONS:
        logging.info("%s active on %s: %s", station, today.isoformat(), active(station, today))
    if not ntfy_topic():
        logging.error("NTFY_TOPIC is not set; U2 detections will be logged but not pushed")
    last_idle = 0.0
    try:
        while True:
            if not a.ignore_schedule and finished():
                logging.info("all schedule windows ended (after 2026-11-05); exiting")
                return
            today = dt.datetime.now(TZ).date()
            if not a.ignore_schedule and not any(active(s, today) for s in STATIONS):
                if time.time() - last_idle >= 600:
                    logging.info("no station active on %s; sleeping until the next window", today.isoformat())
                    last_idle = time.time()
            poll(st, a.ignore_schedule)
            if a.once or (a.duration and time.time() - t0 >= a.duration):
                return
            time.sleep(POLL_SECONDS)
    finally:
        logging.info("watcher stop after %.0fs", time.time() - t0)

if __name__ == "__main__":
    main()
