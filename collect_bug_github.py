import os, json, re, time, requests
from datetime import datetime, timedelta, timezone

# ====== CONFIG ======
GITHUB_API_BASE = "https://api.github.com"
GITHUB_TOKEN = os.getenv("")          
OWNER_REPOS = json.loads(os.getenv("OWNER_REPOS", '["facebook/react"]'))
SINCE = "2024-01-01"                              
BY = "created_at"                                 
PER_PAGE = 100                                    
MAX_TOTAL = 10_000_000

OUT_JSONL = "github_bugs.jsonl"                  

# ====== HELPERS ======
def to_utc_iso_z(s):
    if not s: return ""
    s = str(s)
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z","").replace("z",""))
        else:
            dt = datetime.fromisoformat(s)
    except:
        return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

URL_RE = re.compile(r"https?://\S+")
SPACE_RE = re.compile(r"\s+")

def clean_text(x):
    if x is None: return ""
    if not isinstance(x, str): x = str(x)
    x = URL_RE.sub(" ", x)
    x = SPACE_RE.sub(" ", x).strip()
    return x

def as_list(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]

def as_int_or_none(x):
    try:
        i = int(x)
        return i if i != 0 else None
    except:
        return None

def month_range(start_dt, end_dt):
    cur = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    while cur <= end_dt:
        if cur.month == 12:
            nxt = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            nxt = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)
        yield cur, min(nxt - timedelta(seconds=1), end_dt)
        cur = nxt

def _headers():
    h = {"Accept": "application/vnd.github+json", "User-Agent": "gh-issues-list-collector/1.1"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def _wait_from_headers(resp, attempt):
    retry_after = resp.headers.get("Retry-After")
    reset = resp.headers.get("X-RateLimit-Reset")
    wait = None
    if retry_after:
        try: wait = int(retry_after)
        except: pass
    if wait is None and reset:
        try:
            reset_ts = int(reset)
            now = int(time.time())
            wait = max(1, reset_ts - now)
        except:
            pass
    if wait is None:
        wait = min(60 * (attempt + 1), 300)  # backoff
    print(f"[rate-limit] waiting {wait}s …")
    time.sleep(wait)

def gh_get(url, params, max_retries=5):
    attempt = 0
    while True:
        resp = requests.get(url, params=params, headers=_headers(), timeout=120)
        if resp.status_code == 403 and attempt < max_retries:
            _wait_from_headers(resp, attempt)
            attempt += 1
            continue
        try:
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            print("ERROR", resp.status_code, resp.url)
            try:
                print(resp.text[:5000])
            except:
                pass
            raise

# ====== FETCH  ======
def fetch_bugs_by_date():
    start_dt = datetime.fromisoformat(SINCE).replace(tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)

    all_rows = {}
    total = 0

    for repo in OWNER_REPOS:
        owner, name = repo.split("/", 1)
        base_url = f"{GITHUB_API_BASE}/repos/{owner}/{name}/issues"
        print(f"[repo] {repo}")

        for w_start, w_end in month_range(start_dt, end_dt):
            page = 1
            while True:
                params = {
                    "state": "all",
                    "since": w_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "per_page": PER_PAGE,
                    "page": page
                }
                resp = gh_get(base_url, params)
                items = resp.json()
                if not items:
                    break

                stop_window = False
                for it in items:
                    if "pull_request" in it:
                        continue

                    created = to_utc_iso_z(it.get("created_at"))
                    if not created:
                        continue

    
                    if created > w_end.strftime("%Y-%m-%dT%H:%M:%SZ"):
                        stop_window = True
                        break
                    if created < w_start.strftime("%Y-%m-%dT%H:%M:%SZ"):
                        continue

                    all_rows[it["id"]] = it
                    total += 1
                    if total >= MAX_TOTAL:
                        break

                if total >= MAX_TOTAL or stop_window:
                    break

                if len(items) < PER_PAGE:
                    break
                page += 1

            if total >= MAX_TOTAL:
                break
        if total >= MAX_TOTAL:
            break

    return list(all_rows.values())

# ====== CLEANING  ======
def clean_bug(b):
    url = b.get("html_url", "")
    product = ""
    try:
        parts = url.split("/")
        if len(parts) >= 7:
            product = f"{parts[4]}/{parts[5]}"
    except:
        product = ""

    labels = [lb.get("name","") for lb in (b.get("labels") or []) if isinstance(lb, dict)]
    component = labels[0] if labels else ""
    state = b.get("state","")
    status = "OPEN" if state.lower() == "open" else ("CLOSED" if state.lower() == "closed" else state)
    creator = (b.get("user") or {}).get("login","")
    assignee = (b.get("assignee") or {}).get("login","") if isinstance(b.get("assignee"), dict) else ""

    return {
        "id": int(b.get("id")),
        "summary": clean_text(b.get("title")),
        "status": clean_text(status),
        "resolution": "",
        "product": clean_text(product),   
        "component": clean_text(component), 
        "creation_time": clean_text(to_utc_iso_z(b.get("created_at"))),
        "last_change_time": clean_text(to_utc_iso_z(b.get("updated_at"))),
        "creator": clean_text(creator),
        "assigned_to": clean_text(assignee),
        "keywords": [clean_text(k) for k in labels],
        "url": clean_text(url),
        "depends_on": [],                   
        "dupe_of": None,                    
    }

def clean_dataset(rows):
    tmp = {}
    for b in rows:
        if "id" not in b: continue
        cb = clean_bug(b)
        i = cb["id"]
        if i not in tmp:
            tmp[i] = cb
        else:
            old = tmp[i].get("last_change_time","")
            new = cb.get("last_change_time","")
            if new > old:
                tmp[i] = cb
    out = list(tmp.values())
    out.sort(key=lambda x: x.get("creation_time",""))
    return out

# ====== SAVE ======
def save_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ====== MAIN ======
if __name__ == "__main__":
    print(f"fetching since={SINCE} by={BY} ...")
    raw = fetch_bugs_by_date()
    print("raw:", len(raw))
    clean = clean_dataset(raw)
    print("clean:", len(clean))
    save_jsonl(OUT_JSONL, clean)
    print("saved ->", OUT_JSONL)