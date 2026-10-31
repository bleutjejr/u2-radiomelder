#!/usr/bin/env python3
"""U2 radio watcher for JOE (DPG Media) and Radio 10 (Talpa Network).

Listens continuously and calls notify() once per U2 play.
Only listens to a station on its active dates (Europe/Amsterdam, whole days)
and exits once every window has ended.

Schedule (Europe/Amsterdam, inclusive):
  JOE      29–30 Oct 2026 and 2–5 Nov 2026
  Radio 10 2–6 Nov 2026
The process exits after 6 Nov 2026. That is also when the workflow disables
itself: Radio 10 is still on the air on the 6th, JOE is not.

Sources (all reachable without auth). Whichever source reports a play first
wins; the other is deduped:
  JOE      fast    : relisten.nl playlist (updated a few seconds after start;
                     the stream title trailed it by about 11–18s when measured)
           stream  : ICY metadata, one long-lived connection
                     https://icecast-qmusicnl-cdp.triple-it.nl/Joe_nl.mp3
                     ("Title - ARTIST")
           (official api.joe.nl/2.4/tracks/plays is blocked for this host by DPG's WAF)
  Radio 10 fast    : ICY metadata, one long-lived connection. On the songs that
                     were measured it moved within about a second of Triton,
                     roughly 6–7s after Triton's own start timestamp.
           backup  : Triton now-playing XML, polled every couple of seconds
                     https://np.tritondigital.com/public/nowplaying?mountName=RADIO10&numberToFetch=3&eventType=track
                     ("Artist - Title" on the stream)

The ntfy topic is read only from the NTFY_TOPIC environment variable.
Nothing about the topic is read from disk, and config.json is not used.

Usage:
  python3 watcher.py                 # run loop (respects schedule)
  python3 watcher.py --once          # single poll, then exit
  python3 watcher.py --ignore-schedule --duration 120   # test run
  python3 watcher.py --selftest      # matcher, schedule, message, dedupe
  python3 watcher.py --probe         # check sources, never notify
  python3 watcher.py --test-push     # one labelled test notification, then exit
"""
import argparse, base64, datetime as dt, hashlib, http.client, html, json, logging, os, queue, re, sys, tempfile, threading, time
import urllib.request
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(HERE, "watcher.log")
STATE_FILE = os.path.join(HERE, "state.json")
TZ = ZoneInfo("Europe/Amsterdam")
# HTTP backups are polled this often. The JOE playlist is the fast source there;
# Radio 10's fast source is the open ICY stream, and Triton is the backup.
RELISTEN_SECONDS = 2
TRITON_SECONDS = 2
ICY_TIMEOUT = 8
HTTP_TIMEOUT = 6
NTFY_TIMEOUT = 4
DEDUPE_WINDOW = 30 * 60       # same station+song not re-alerted within 30 min
CATCHUP_WINDOW = 15 * 60      # history items older than this are never alerted
UA = "Mozilla/5.0 (X11; Linux x86_64) u2-radio-watcher/1.1"

JOE_ICY_URL = "https://icecast-qmusicnl-cdp.triple-it.nl/Joe_nl.mp3"
R10_ICY_URL = "https://playerservices.streamtheworld.com/api/livestream-redirect/RADIO10.mp3"
RELISTEN_URL = "https://www.relisten.nl/playlists/joe"
TRITON_URL = "https://np.tritondigital.com/public/nowplaying?mountName=RADIO10&numberToFetch=3&eventType=track"


def _days(start, end):
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    return {s + dt.timedelta(n) for n in range((e - s).days + 1)}

# Active dates per station (local Europe/Amsterdam calendar days, inclusive).
# Radio 10 runs one day longer than JOE, so the watcher stays up through 6 Nov.
SCHEDULE = {
    "JOE": _days("2026-10-29", "2026-10-30") | _days("2026-11-02", "2026-11-05"),
    "Radio 10": _days("2026-11-02", "2026-11-06"),
}

def last_show_day():
    """Last calendar day any station is still on the air."""
    return max(max(days) for days in SCHEDULE.values())

# ---------------------------------------------------------------- matching
U2_RE = re.compile(r"(?<![\w])U\s?2(?![\w])", re.IGNORECASE)

def is_u2(artist: str) -> bool:
    """True if 'U2' appears as a standalone token in the artist string.
    Matches 'U2', 'u2', 'U2 & Green Day', 'Jam & Spoon feat. U2';
    rejects 'UB40', 'U23', 'MU2', 'U2K'."""
    return bool(artist and U2_RE.search(artist))

def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def should_alert(station, track, st, now=None):
    """True when this play is inside the catch-up window and not deduped."""
    now = time.time() if now is None else now
    if track.get("start") and now - float(track["start"]) > CATCHUP_WINDOW:
        return False
    key = f"{station}|{norm(track['title'])}"
    last = st.get("alerted", {}).get(key)
    if last and now - float(last) < DEDUPE_WINDOW:
        return False
    return True

# ---------------------------------------------------------------- fetchers
def http_get(url, timeout=HTTP_TIMEOUT, headers=None):
    h = {"User-Agent": UA}
    h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def _stream_title(meta):
    text = meta.rstrip(b"\0").decode("utf-8", "replace")
    m = re.search(r"StreamTitle='(.*?)';", text)
    return m.group(1).strip() if m else ""

def icy_title(url, timeout=ICY_TIMEOUT):
    """Open the stream, return the first StreamTitle, then close it."""
    req = urllib.request.Request(url, headers={"Icy-MetaData": "1", "User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        mi = int(r.headers.get("icy-metaint") or 0)
        if not mi:
            raise RuntimeError("no icy-metaint")
        for _ in range(8):
            chunk = r.read(mi)
            if len(chunk) < mi:
                break
            nbyte = r.read(1)
            if len(nbyte) != 1:
                break
            n = nbyte[0] * 16
            if not n:
                continue
            title = _stream_title(r.read(n))
            if title:
                return title
    return ""

def parse_joe_icy(raw):
    if not raw or " - " not in raw:
        return None
    title, artist = raw.rsplit(" - ", 1)          # JOE format: "Title - ARTIST"
    return {"artist": artist.strip(), "title": title.strip(), "start": None, "src": "joe-icy"}

def parse_r10_icy(raw):
    if not raw or " - " not in raw:
        return None
    artist, title = raw.split(" - ", 1)           # Triton format: "Artist - Title"
    return {"artist": artist.strip(), "title": title.strip(), "start": None, "src": "r10-icy"}

def joe_icy():
    track = parse_joe_icy(icy_title(JOE_ICY_URL))
    return [track] if track else []

def joe_relisten():
    page = http_get(RELISTEN_URL)
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
    x = http_get(TRITON_URL)
    out = []
    for info in ET.fromstring(x).findall("nowplaying-info"):
        p = {e.get("name"): (e.text or "").strip() for e in info.findall("property")}
        start = p.get("cue_time_start")
        out.append({"artist": p.get("track_artist_name", ""), "title": p.get("cue_title", ""),
                    "start": int(start) / 1000 if start else float(info.get("timestamp")),
                    "duration": int(p.get("cue_time_duration") or 0) / 1000, "src": "triton"})
    return out

def radio10_icy():
    track = parse_r10_icy(icy_title(R10_ICY_URL))
    return [track] if track else []

STATIONS = {
    "JOE": [joe_icy, joe_relisten],
    "Radio 10": [radio10_triton, radio10_icy],
}
# Long-lived ICY plus a short HTTP poll. Both always run; the first U2 hit alerts.
ICY_STREAMS = {
    "JOE": (JOE_ICY_URL, parse_joe_icy, "joe_icy"),
    "Radio 10": (R10_ICY_URL, parse_r10_icy, "radio10_icy"),
}
HTTP_POLLS = {
    "JOE": [(joe_relisten, RELISTEN_SECONDS)],
    "Radio 10": [(radio10_triton, TRITON_SECONDS)],
}

# ---------------------------------------------------------------- notify (ntfy.sh push)
WHATSAPP_R10 = "https://wa.me/31357505900?text=tickets"
JOE_LINK = "https://joe.nl"   # joe.nl/* is a universal/app link for the JOE app (nl.joe.app)

_ntfy_lock = threading.Lock()
_ntfy_conn = None

def _ntfy_close():
    global _ntfy_conn
    conn, _ntfy_conn = _ntfy_conn, None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

def _ntfy_connection():
    global _ntfy_conn
    if _ntfy_conn is None:
        _ntfy_conn = http.client.HTTPSConnection("ntfy.sh", timeout=NTFY_TIMEOUT)
    _ntfy_conn.timeout = NTFY_TIMEOUT
    if getattr(_ntfy_conn, "sock", None) is not None:
        _ntfy_conn.sock.settimeout(NTFY_TIMEOUT)
    return _ntfy_conn

def ntfy_publish(title, message, click=None, actions=None, priority=4, tags=None):
    """Publish via ntfy JSON API (UTF-8 safe). Returns HTTP status.

    The topic comes only from NTFY_TOPIC. It is never read from a file,
    never written to the request path, and must not be logged.
    One warm connection is reused. A dead socket is opened again once here;
    notify() does the further retries.
    """
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        raise RuntimeError("NTFY_TOPIC is not set")
    body = {"topic": topic, "title": title, "message": message, "priority": priority}
    if click:
        body["click"] = click
    if actions:
        body["actions"] = actions
    if tags:
        body["tags"] = tags
    data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json", "User-Agent": UA, "Connection": "keep-alive"}
    err = None
    with _ntfy_lock:
        for _attempt in range(2):
            try:
                conn = _ntfy_connection()
                conn.request("POST", "/", body=data, headers=headers)
                resp = conn.getresponse()
                resp.read()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}")
                return resp.status
            except Exception as e:
                err = e
                _ntfy_close()
    raise err

def ntfy_warm_loop(stop):
    """Keep a TLS session to ntfy.sh open so the real push skips the handshake.

    Uses the health endpoint only. Nothing is published.
    """
    if not os.environ.get("NTFY_TOPIC", "").strip():
        return
    while not stop.is_set():
        try:
            with _ntfy_lock:
                conn = _ntfy_connection()
                conn.request("GET", "/v1/health", headers={"User-Agent": UA, "Connection": "keep-alive"})
                resp = conn.getresponse()
                resp.read()
                if resp.status >= 400:
                    _ntfy_close()
        except Exception:
            _ntfy_close()
        if stop.wait(20):
            return

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

def test_push():
    """One clearly labelled test notification. Does not report a U2 play."""
    try:
        status = ntfy_publish(
            title="Test: U2-melder draait op GitHub",
            message="Eenmalige test: de U2-melder draait op GitHub Actions. Dit is geen uitzending.",
            priority=3,
            tags=["information"],
        )
    except Exception as e:
        print(f"test push failed: {type(e).__name__}")
        return False
    print(f"test push sent (HTTP {status})")
    return True

def probe():
    """Check every configured source. Prints PROBE lines and never notifies.

    A station passes when at least one of its sources returns a track.
    Sources are fetched together so a slow stream does not hold up the others.
    """
    from concurrent.futures import ThreadPoolExecutor

    tasks = [(station, f) for station, fetchers in STATIONS.items() for f in fetchers]

    def run(item):
        station, fetcher = item
        try:
            return station, fetcher.__name__, fetcher(), None
        except Exception as e:
            return station, fetcher.__name__, None, e

    with ThreadPoolExecutor(max_workers=len(tasks) or 1) as pool:
        results = list(pool.map(run, tasks))

    by_station = {station: [] for station in STATIONS}
    for station, name, tracks, err in results:
        by_station[station].append((name, tracks, err))

    all_ok = True
    for station, fetchers in STATIONS.items():
        station_ok = False
        order = {f.__name__: i for i, f in enumerate(fetchers)}
        for name, tracks, err in sorted(by_station[station], key=lambda row: order.get(row[0], 0)):
            if err is not None:
                print(f"PROBE FAIL {station} {name}: {type(err).__name__}: {err}")
                continue
            if not tracks:
                print(f"PROBE EMPTY {station} {name}")
                continue
            cur = tracks[0]
            print(f"PROBE OK {station} {name}: {cur['artist']} - {cur['title']} ({len(tracks)} tracks)")
            station_ok = True
        if not station_ok:
            all_ok = False
            print(f"PROBE FAIL {station}: no working source")
    print("PROBE", "OK" if all_ok else "FAILED")
    return all_ok

def notify(station, track):
    """Push once, retrying quickly. Returns False when every attempt failed.

    Missing NTFY_TOPIC is not retried and nothing is sent.
    The exception text is not logged: a transport error can echo the request.
    """
    if not os.environ.get("NTFY_TOPIC", "").strip():
        logging.error("ntfy push failed: NTFY_TOPIC is not set")
        return False
    for attempt, delay in enumerate((0, 0.25, 0.5), start=1):
        if delay:
            time.sleep(delay)
        try:
            status = ntfy_publish(tags=["guitar"], **build_message(station, track))
            logging.warning("ntfy push sent (HTTP %s)", status)
            return True
        except Exception as e:
            logging.error("ntfy push failed (attempt %d): %s", attempt, type(e).__name__)
    return False

# ---------------------------------------------------------------- shared dedupe
# Two runners overlap for a few minutes at each handoff. Local state cannot
# see the other process, so the runner that alerts first creates a git ref.
# Creating a ref is atomic: the loser gets "Reference already exists" and
# does not push. The topic is not part of the ref name.
_gh_local = threading.local()
_claim_warned = False

def _shared_dedupe_enabled():
    return (os.environ.get("U2_SHARED_DEDUPE") == "1"
            and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_TOKEN", "").strip()
            and os.environ.get("GITHUB_REPOSITORY", "").strip()
            and os.environ.get("GITHUB_SHA", "").strip())

def _gh_connection():
    conn = getattr(_gh_local, "conn", None)
    if conn is None:
        conn = http.client.HTTPSConnection("api.github.com", timeout=4)
        _gh_local.conn = conn
    conn.timeout = 4
    if getattr(conn, "sock", None) is not None:
        conn.sock.settimeout(4)
    return conn

def _gh_close():
    conn = getattr(_gh_local, "conn", None)
    _gh_local.conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

def _gh(method, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Authorization": f"Bearer {os.environ.get('GITHUB_TOKEN', '').strip()}",
        "Accept": "application/vnd.github+json",
        "User-Agent": UA,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    err = None
    for _attempt in range(2):
        try:
            conn = _gh_connection()
            conn.request(method, path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, raw
        except Exception as e:
            err = e
            _gh_close()
    raise err

def _claim_parts(station, title, bucket):
    digest = hashlib.sha256(f"{station}|{norm(title)}".encode()).hexdigest()[:20]
    return bucket, digest

def _ref_exists(repo, bucket, digest):
    status, _raw = _gh("GET", f"/repos/{repo}/git/ref/u2-seen/{bucket}/{digest}")
    return status == 200

def claim_shared(station, title):
    """Return True if this process may push.

    False only when another run already holds the ref. Network and permission
    failures fail open: missing a contest alert is worse than a rare duplicate.
    """
    global _claim_warned
    if not _shared_dedupe_enabled():
        return True
    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    bucket = int(time.time() // DEDUPE_WINDOW)
    _bucket, digest = _claim_parts(station, title, bucket)
    try:
        # A play claimed just before a 30-minute boundary lives in the previous slot.
        if time.time() % DEDUPE_WINDOW < 300 and _ref_exists(repo, bucket - 1, digest):
            return False
        status, raw = _gh("POST", f"/repos/{repo}/git/refs", {
            "ref": f"refs/u2-seen/{bucket}/{digest}",
            "sha": sha,
        })
    except Exception as e:
        if not _claim_warned:
            _claim_warned = True
            logging.warning("shared dedupe unavailable (%s); alerting on local state", type(e).__name__)
        return True
    if status in (200, 201):
        return True
    if status == 422 and b"already exists" in raw.lower():
        return False
    if not _claim_warned:
        _claim_warned = True
        logging.warning("shared dedupe unavailable (HTTP %s); alerting on local state", status)
    return True

def release_shared(station, title):
    """Drop a claim after a failed push so the next attempt, or the other run, can send."""
    if not _shared_dedupe_enabled():
        return
    repo = os.environ["GITHUB_REPOSITORY"]
    bucket = int(time.time() // DEDUPE_WINDOW)
    _bucket, digest = _claim_parts(station, title, bucket)
    try:
        _gh("DELETE", f"/repos/{repo}/git/refs/u2-seen/{bucket}/{digest}")
    except Exception:
        pass

def cleanup_old_claims():
    """Delete dedupe refs from buckets that can no longer suppress a replay."""
    if not _shared_dedupe_enabled():
        return
    repo = os.environ["GITHUB_REPOSITORY"]
    try:
        status, raw = _gh("GET", f"/repos/{repo}/git/matching-refs/u2-seen")
    except Exception:
        return
    if status != 200:
        return
    try:
        items = json.loads(raw)
    except Exception:
        return
    current = int(time.time() // DEDUPE_WINDOW)
    for item in items if isinstance(items, list) else []:
        ref = item.get("ref") or ""
        parts = ref.split("/")
        if len(parts) < 4 or parts[1] != "u2-seen":
            continue
        try:
            bucket = int(parts[2])
        except ValueError:
            continue
        if bucket >= current - 1:
            continue
        try:
            _gh("DELETE", f"/repos/{repo}/git/{ref[len('refs/'):]}")
        except Exception:
            pass

# ---------------------------------------------------------------- state
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"alerted": {}}

def save_state(st):
    now = time.time()
    st["alerted"] = {k: v for k, v in st["alerted"].items() if now - float(v) < 7 * 86400}
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f, indent=1)
    os.replace(tmp, STATE_FILE)

def dedupe_snapshot(st=None):
    """Base64 JSON of alert keys still inside the dedupe window.

    Passed to the overlapping run. Contains station and title keys only.
    """
    if st is None:
        st = load_state()
    now = time.time()
    keep = {}
    for key, ts in st.get("alerted", {}).items():
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            continue
        if now - ts < DEDUPE_WINDOW:
            keep[key] = ts
    return base64.b64encode(json.dumps(keep, separators=(",", ":")).encode()).decode()

def merge_seed(st, blob):
    blob = (blob or "").strip()
    if not blob:
        return 0
    try:
        data = json.loads(base64.b64decode(blob))
    except Exception:
        logging.error("ignoring unreadable dedupe seed")
        return 0
    if not isinstance(data, dict):
        logging.error("ignoring unreadable dedupe seed")
        return 0
    now = time.time()
    alerted = st.setdefault("alerted", {})
    merged = 0
    for key, ts in data.items():
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            continue
        if now - ts >= DEDUPE_WINDOW:
            continue
        if ts >= float(alerted.get(key) or 0):
            if alerted.get(key) != ts:
                merged += 1
            alerted[key] = ts
    if merged:
        save_state(st)
        logging.info("merged %d dedupe keys from the overlapping run", merged)
    return merged

def handle(station, track, st, attempt=0, retries=None):
    """Fire notify once per play. Key = station + title; a play counts as new if
    no alert for that station+title in the last 30 min (covers ICY items without
    a start time and the same play reported by two sources)."""
    now = time.time()
    if not should_alert(station, track, st, now):
        return False
    key = f"{station}|{norm(track['title'])}"
    st.setdefault("alerted", {})[key] = now
    st.setdefault("plays", []).append({
        "station": station, "artist": track.get("artist"), "title": track.get("title"),
        "start": track.get("start"), "src": track.get("src"), "alerted_at": now,
    })
    st["plays"] = st["plays"][-200:]
    save_state(st)
    when = dt.datetime.fromtimestamp(track["start"], TZ).strftime("%H:%M") if track.get("start") else "nu"
    logging.warning("U2 ALERT: %s speelt nu '%s' van %s (start %s, bron %s)",
                    station, track["title"], track["artist"], when, track["src"])
    if not claim_shared(station, track["title"]):
        logging.warning("U2 alert suppressed; the overlapping run already claimed this play")
        return False
    if notify(station, track):
        return True
    release_shared(station, track["title"])
    # No topic means this process cannot send. Keep the mark so the open
    # stream does not retry the same play on every metadata update.
    if not os.environ.get("NTFY_TOPIC", "").strip():
        return False
    if st.get("alerted", {}).get(key) == now:
        st["alerted"].pop(key, None)
        save_state(st)
    if retries is not None and os.environ.get("NTFY_TOPIC", "").strip() and attempt < 8:
        delay = min(20, 0.5 * (2 ** attempt))
        retries.append((time.time() + delay, station, track, attempt + 1))
    return False

# ---------------------------------------------------------------- listen
def notice(station, message):
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    safe_station = station.replace("%", "%25").replace("\n", "").replace("\r", "")
    safe = message.replace("%", "%25").replace("\n", "%0A").replace("\r", "%0D")
    print(f"::notice title={safe_station}::{safe}", flush=True)

def _start_label(track):
    if not track.get("start"):
        return "-"
    return dt.datetime.fromtimestamp(track["start"], TZ).strftime("%d-%m %H:%M:%S")

class Listen:
    """One ICY connection per active station, plus the HTTP polls beside it."""

    def __init__(self, st):
        self.st = st
        self.q = queue.Queue()
        self.shown = {}
        self.err_at = {}
        self.retries = []
        self.stops = {}
        self.shutdown = threading.Event()

    def log_error(self, station, name, exc):
        key = (station, name)
        now = time.time()
        if now - self.err_at.get(key, 0) < 60:
            return
        self.err_at[key] = now
        logging.error("%s: %s failed: %s", station, name, exc)

    def icy_loop(self, station, stop):
        url, _parse, label = ICY_STREAMS[station]
        backoff = 1
        while not stop.is_set():
            try:
                req = urllib.request.Request(url, headers={"Icy-MetaData": "1", "User-Agent": UA})
                with urllib.request.urlopen(req, timeout=ICY_TIMEOUT) as r:
                    mi = int(r.headers.get("icy-metaint") or 0)
                    if not mi:
                        raise RuntimeError("no icy-metaint")
                    backoff = 1
                    last = None
                    while not stop.is_set():
                        chunk = r.read(mi)
                        if len(chunk) < mi:
                            raise RuntimeError("stream closed")
                        nbyte = r.read(1)
                        if len(nbyte) != 1:
                            raise RuntimeError("stream closed")
                        n = nbyte[0] * 16
                        if not n:
                            continue
                        raw = _stream_title(r.read(n))
                        if not raw or raw == last:
                            continue
                        last = raw
                        self.q.put(("icy", station, label, raw))
            except Exception as e:
                self.log_error(station, label, e)
                if stop.wait(backoff):
                    return
                backoff = min(backoff * 2, 10)

    def http_loop(self, station, fetcher, interval, stop):
        while not stop.is_set():
            started = time.monotonic()
            try:
                tracks = fetcher()
            except Exception as e:
                self.q.put(("http-error", station, fetcher.__name__, f"{type(e).__name__}: {e}"))
                text = str(e)
                if "429" in text or "503" in text:
                    if stop.wait(30):
                        return
                    continue
            else:
                self.q.put(("http", station, fetcher.__name__, tracks))
            remain = interval - (time.monotonic() - started)
            if stop.wait(max(0.05, remain)):
                return

    def ensure(self, ignore_schedule):
        today = dt.datetime.now(TZ).date()
        for station in STATIONS:
            want = ignore_schedule or active(station, today)
            if want and station not in self.stops:
                stop = threading.Event()
                self.stops[station] = stop
                threading.Thread(target=self.icy_loop, args=(station, stop), name=f"icy-{station}", daemon=True).start()
                for fetcher, interval in HTTP_POLLS[station]:
                    threading.Thread(
                        target=self.http_loop, args=(station, fetcher, interval, stop),
                        name=f"http-{fetcher.__name__}", daemon=True,
                    ).start()
                logging.info("listening %s (open ICY stream, HTTP every %ss)", station,
                             "/".join(str(s) for _f, s in HTTP_POLLS[station]))
            elif not want and station in self.stops:
                logging.info("stopped listening %s", station)
                self.stops.pop(station).set()

    def show_track(self, station, label, track, raw=None):
        if track:
            sig = (norm(track["artist"]), norm(track["title"]))
            line = f"{track['artist']} - {track['title']}"
        else:
            sig = ("", raw or "")
            line = raw or ""
        if self.shown.get((station, label)) == sig:
            return False
        self.shown[(station, label)] = sig
        if track:
            logging.info("%-8s [%s] now/latest: %s - %s (start %s)",
                         station, label, track["artist"], track["title"], _start_label(track))
        else:
            logging.info("%-8s [%s] now/latest: %s", station, label, line)
        notice(station, f"{label}: {line}")
        return True

    def consider(self, station, tracks):
        for track in tracks:
            if is_u2(track.get("artist")):
                handle(station, track, self.st, retries=self.retries)

    def retry_due(self):
        now = time.time()
        due = [item for item in self.retries if item[0] <= now]
        self.retries = [item for item in self.retries if item[0] > now]
        for _when, station, track, attempt in due:
            handle(station, track, self.st, attempt=attempt, retries=self.retries)

    def process(self, ev):
        kind = ev[0]
        if kind == "icy":
            _kind, station, label, raw = ev
            track = ICY_STREAMS[station][1](raw)
            self.show_track(station, label, track, raw)
            if track:
                self.consider(station, [track])
        elif kind == "http":
            _kind, station, label, tracks = ev
            if not tracks:
                self.log_error(station, label, "empty")
                return
            self.show_track(station, label, tracks[0])
            self.consider(station, tracks)
        elif kind == "http-error":
            _kind, station, label, message = ev
            self.log_error(station, label, message)

    def run(self, ignore_schedule, duration):
        started = time.monotonic()
        self.ensure(ignore_schedule)
        while not self.shutdown.is_set():
            if not ignore_schedule and finished():
                logging.info("all schedule windows ended (after %s); exiting", last_show_day().isoformat())
                break
            self.ensure(ignore_schedule)
            if duration and time.monotonic() - started >= duration:
                break
            timeout = 1
            if duration:
                timeout = min(1, max(0.1, duration - (time.monotonic() - started)))
            try:
                ev = self.q.get(timeout=timeout)
            except queue.Empty:
                self.retry_due()
                continue
            self.process(ev)
            self.retry_due()
        while True:
            try:
                self.process(self.q.get_nowait())
            except queue.Empty:
                break
        for stop in self.stops.values():
            stop.set()
        self.shutdown.set()

# ---------------------------------------------------------------- loop
def active(station, today):
    return today in SCHEDULE[station]

def poll(st, ignore_schedule=False):
    """One parallel round, used by --once. The long run uses Listen instead."""
    from concurrent.futures import ThreadPoolExecutor
    today = dt.datetime.now(TZ).date()
    tasks = []
    for station, fetchers in STATIONS.items():
        if not ignore_schedule and not active(station, today):
            continue
        for fetcher in fetchers:
            tasks.append((station, fetcher))
    if not tasks:
        logging.info("no station is scheduled today")
        return

    def run(item):
        station, fetcher = item
        try:
            return station, fetcher, fetcher(), None
        except Exception as e:
            return station, fetcher, None, e

    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        results = list(pool.map(run, tasks))
    for station, fetcher, tracks, err in results:
        if err is not None:
            logging.error("%s: %s failed: %s", station, fetcher.__name__, err)
            continue
        if not tracks:
            continue
        cur = tracks[0]
        logging.info("%-8s [%s] now/latest: %s - %s (start %s)",
                     station, fetcher.__name__, cur["artist"], cur["title"], _start_label(cur))
        if os.environ.get("GITHUB_ACTIONS") == "true":
            msg = f"{fetcher.__name__}: {cur['artist']} - {cur['title']}".replace("\n", " ").replace("\r", " ")
            notice(station, msg)
        for track in tracks:
            if is_u2(track["artist"]):
                handle(station, track, st)

def finished():
    return dt.datetime.now(TZ).date() > last_show_day()

def _schedule_selftest():
    def on(station, days):
        return all(active(station, dt.date.fromisoformat(d)) for d in days)
    def off(station, days):
        return not any(active(station, dt.date.fromisoformat(d)) for d in days)
    joe_on = ["2026-10-29", "2026-10-30", "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05"]
    joe_off = ["2026-10-28", "2026-10-31", "2026-11-01", "2026-11-06", "2026-11-07"]
    r10_on = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06"]
    r10_off = ["2026-10-30", "2026-11-01", "2026-11-07"]
    last = last_show_day()
    ok = (on("JOE", joe_on) and off("JOE", joe_off)
          and on("Radio 10", r10_on) and off("Radio 10", r10_off)
          and last == dt.date(2026, 11, 6)
          and dt.date(2026, 11, 6) > dt.date(2026, 11, 5)
          and not (dt.date(2026, 11, 6) > last)
          and (dt.date(2026, 11, 7) > last))
    print(f"schedule JOE through 2026-11-05 ({len(SCHEDULE['JOE'])} days), "
          f"Radio 10 through 2026-11-06 ({len(SCHEDULE['Radio 10'])} days), "
          f"disable after {last.isoformat()}")
    if not ok:
        print("SCHEDULE FAILED")
    return ok

def _message_and_dedupe_selftest():
    ok = True
    joe = build_message("JOE", {"title": "One", "artist": "U2"})
    r10 = build_message("Radio 10", {"title": "One", "artist": "U2"})
    ok = ok and joe == dict(
        title="U2 op JOE!",
        message="Nu: One van U2. Stuur 'U2' in de JOE-app.",
        click=JOE_LINK,
        actions=[{"action": "view", "label": "Open JOE", "url": JOE_LINK}],
    )
    ok = ok and r10 == dict(
        title="U2 op Radio 10!",
        message="Nu: One van U2. App 'tickets' naar Radio 10.",
        click=WHATSAPP_R10,
        actions=[{"action": "view", "label": "WhatsApp tickets", "url": WHATSAPP_R10}],
    )
    parsed = parse_joe_icy("Beautiful Day - U2")
    ok = ok and parsed and parsed["artist"] == "U2" and parsed["title"] == "Beautiful Day" and parsed["src"] == "joe-icy"
    ok = ok and parse_joe_icy("Ad break") is None
    parsed = parse_r10_icy("U2 - One")
    ok = ok and parsed and parsed["artist"] == "U2" and parsed["title"] == "One" and parsed["src"] == "r10-icy"
    now = 1_000_000.0
    fresh = {"artist": "U2", "title": "One", "start": None}
    st = {"alerted": {}}
    ok = ok and should_alert("JOE", fresh, st, now)
    st = {"alerted": {"JOE|one": now}}
    ok = ok and not should_alert("JOE", fresh, st, now + 10)
    ok = ok and should_alert("JOE", fresh, st, now + DEDUPE_WINDOW + 1)
    old = {"artist": "U2", "title": "Old", "start": now - CATCHUP_WINDOW - 5}
    ok = ok and not should_alert("JOE", old, {"alerted": {}}, now)
    recent = {"artist": "U2", "title": "New", "start": now - 30}
    ok = ok and should_alert("Radio 10", recent, {"alerted": {}}, now)
    # A second source for the same title must not look like a new play.
    ok = ok and not should_alert("JOE", {"artist": "U2", "title": " One ", "start": None}, {"alerted": {"JOE|one": now}}, now + 1)
    os.environ.pop("U2_SHARED_DEDUPE", None)
    ok = ok and claim_shared("JOE", "One") is True
    removed = os.environ.pop("NTFY_TOPIC", None)
    try:
        try:
            ntfy_publish("t", "m")
            ok = False
        except RuntimeError as e:
            ok = ok and str(e) == "NTFY_TOPIC is not set"
    finally:
        if removed is not None:
            os.environ["NTFY_TOPIC"] = removed
    blob = dedupe_snapshot({"alerted": {"Radio 10|one": time.time(), "JOE|ancient": time.time() - DEDUPE_WINDOW - 5}})
    merged = {"alerted": {}}
    # merge_seed writes STATE_FILE; point it at a temp file for the duration.
    global STATE_FILE
    old_state = STATE_FILE
    fd, path = tempfile.mkstemp(prefix="u2-selftest-")
    os.close(fd)
    STATE_FILE = path
    try:
        n = merge_seed(merged, blob)
        ok = ok and n == 1 and "Radio 10|one" in merged["alerted"] and "JOE|ancient" not in merged["alerted"]
    finally:
        STATE_FILE = old_state
        try:
            os.unlink(path)
        except OSError:
            pass
    print("messages+dedupe", "OK" if ok else "FAILED")
    return ok

def selftest():
    yes = ["U2", "u2", "U2 & Green Day", "Jam & Spoon feat. U2", "U2 / Bono", "U2, Kygo", "Kygo x U2", "U 2"]
    no = ["UB40", "U23", "MU2", "U2K", "U-ZiQ", "Bono", "Sniff 'N' The Tears", "", "Supertramp"]
    ok = all(is_u2(a) for a in yes) and not any(is_u2(a) for a in no)
    for a in yes + no:
        print(f"{a!r:30} -> {is_u2(a)}")
    ok = _schedule_selftest() and ok
    ok = _message_and_dedupe_selftest() and ok
    print("SELFTEST", "OK" if ok else "FAILED")
    return ok

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--ignore-schedule", action="store_true", help="poll both stations regardless of dates (testing)")
    ap.add_argument("--duration", type=int, default=0, help="stop after N seconds (testing)")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--probe", action="store_true", help="check sources and exit; never notify")
    ap.add_argument("--test-push", action="store_true", help="send one labelled test notification and exit")
    ap.add_argument("--seed-state", default="", help="base64 dedupe snapshot from the overlapping run")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if a.probe:
        sys.exit(0 if probe() else 1)
    if a.test_push:
        sys.exit(0 if test_push() else 1)
    logging.Formatter.converter = lambda *x: dt.datetime.now(TZ).timetuple()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])
    st = load_state()
    merge_seed(st, a.seed_state)
    logging.info("watcher start (ignore_schedule=%s, dedupe_keys=%d, last_show_day=%s)",
                 a.ignore_schedule, len(st.get("alerted", {})), last_show_day().isoformat())
    if not a.ignore_schedule and finished():
        logging.info("all schedule windows ended (after %s); exiting", last_show_day().isoformat())
        return
    if a.once:
        poll(st, a.ignore_schedule)
        return
    warm_stop = threading.Event()
    threading.Thread(target=ntfy_warm_loop, args=(warm_stop,), name="ntfy-warm", daemon=True).start()
    threading.Thread(target=cleanup_old_claims, name="dedupe-cleanup", daemon=True).start()
    Listen(st).run(a.ignore_schedule, a.duration)
    warm_stop.set()

if __name__ == "__main__":
    main()
