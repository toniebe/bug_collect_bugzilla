import os, json, csv, re, requests
from datetime import datetime, timedelta, timezone

# ====== CONFIG ======
BUGZILLA_BASE = "https://bugzilla.mozilla.org"
BUGZILLA_API_KEY = os.getenv("BlcgQ07cwYUdywCWCBqwQSuTH8Vq04yDEZ9XMzA7")  # opsional
SINCE = "2024-01-01"      
BY = "creation_time"       
PER_PAGE = 200000              
MAX_TOTAL = 10000000            
PRODUCTS = [] 

OUT_JSONL = "bugzilla_bugs.jsonl"

# ====== HELPERS ======
def to_utc_iso_z(s):
    if "T" in s:
        dt = datetime.fromisoformat(s.replace("Z","").replace("z",""))
    else:
        dt = datetime.fromisoformat(s) 
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
            nxt = datetime(cur.year+1, 1, 1, tzinfo=timezone.utc)
        else:
            nxt = datetime(cur.year, cur.month+1, 1, tzinfo=timezone.utc)
        yield cur, min(nxt - timedelta(seconds=1), end_dt)
        cur = nxt

def fetch_bugs_by_date():
    base_url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug"
    include_fields = [
        "id","summary","status","resolution","product","component",
        "creation_time","last_change_time","creator","assigned_to",
        "keywords","url","depends_on","dupe_of"
    ]

    start_dt = datetime.fromisoformat(SINCE).replace(tzinfo=timezone.utc)
    end_dt   = datetime.now(timezone.utc)

    all_rows = {}
    for w_start, w_end in month_range(start_dt, end_dt):
        params = {
            "include_fields": include_fields,
            "order": "creation_time asc",
            "limit": 1000,              
            "creation_time": w_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "f1": "creation_ts", "o1": "lessthan", "v1": w_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if PRODUCTS:
            params["product"] = PRODUCTS
        if BUGZILLA_API_KEY:
            params["api_key"] = BUGZILLA_API_KEY

        offset = 0
        while True:
            params["offset"] = offset
            r = requests.get(base_url, params=params, timeout=120)
            r.raise_for_status()
            data = r.json()
            page = data.get("bugs", [])
            if not page:
                break
            for b in page:
                all_rows[b["id"]] = b   # de-dup by id di sini

            if len(page) < params["limit"]:
                break
            offset += params["limit"]

    return list(all_rows.values())

# ====== CLEANING ======
def clean_bug(b):
    return {
        "id": int(b.get("id")),
        "summary": clean_text(b.get("summary")),
        "status": clean_text(b.get("status")),
        "resolution": clean_text(b.get("resolution")),
        "product": clean_text(b.get("product")),
        "component": clean_text(b.get("component")),
        "creation_time": clean_text(b.get("creation_time")),
        "last_change_time": clean_text(b.get("last_change_time")),
        "creator": clean_text(b.get("creator")),
        "assigned_to": clean_text(b.get("assigned_to")),
        "keywords": [clean_text(k) for k in as_list(b.get("keywords"))],
        "url": clean_text(b.get("url")),
        "depends_on": [int(x) for x in as_list(b.get("depends_on")) if as_int_or_none(x) is not None],
        "dupe_of": as_int_or_none(b.get("dupe_of")),
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
