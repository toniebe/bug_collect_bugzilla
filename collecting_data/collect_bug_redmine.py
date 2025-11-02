import os, json, csv, re, requests
from datetime import datetime, timezone

# ====== CONFIG ======
REDMINE_BASE = "https://www.redmine.org"                 
REDMINE_API_KEY = os.getenv("")          
SINCE = "2024-01-01"                                    
BY = "updated_on"                                       
PER_PAGE = 100                                          
MAX_TOTAL = 1000000                                     
PROJECTS = []                                           

OUT_JSONL = "redmine_bugs.jsonl"

# ====== HELPERS ======
def to_utc_iso_z(s):
    if not s:
        return ""
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

def _headers():
    h = {"Accept": "application/json", "User-Agent": "redmine-collector/1.0"}
    if REDMINE_API_KEY:
        h["X-Redmine-API-Key"] = REDMINE_API_KEY
    return h

# ====== FETCH ======
def fetch_bugs_by_date():
    url = f"{REDMINE_BASE.rstrip('/')}/issues.json"

    def fetch_scope(project_id=None):
        issues, offset = [], 0
        while True:
            params = {
                "status_id": "*",
                "limit": PER_PAGE,
                "offset": offset,
                "sort": f"{BY}:desc",
                BY: f">={SINCE}",
            }
            if project_id:
                params["project_id"] = project_id
            r = requests.get(url, params=params, headers=_headers(), timeout=120)
            r.raise_for_status()
            data = r.json()
            page = data.get("issues", [])
            if not page:
                break
            issues.extend(page)
            if len(page) < PER_PAGE or len(issues) >= MAX_TOTAL:
                break
            offset += PER_PAGE
        return issues[:MAX_TOTAL]

    all_issues = []
    if PROJECTS:
        for pid in PROJECTS:
            got = fetch_scope(pid)
            all_issues.extend(got)
            if len(all_issues) >= MAX_TOTAL:
                break
    else:
        all_issues = fetch_scope()

    return all_issues[:MAX_TOTAL]

# ====== CLEANING ======
def clean_bug(b):
    proj = ((b.get("project") or {}).get("name", "")) or ((b.get("project") or {}).get("id", ""))
    tracker = ((b.get("tracker") or {}).get("name", "")) or ""
    status = ((b.get("status") or {}).get("name", "")) or ""
    author = ((b.get("author") or {}).get("name", "")) or ""
    assigned = ((b.get("assigned_to") or {}).get("name", "")) or ""

    created = b.get("created_on") or ""
    updated = b.get("updated_on") or created

    return {
        "id": int(b.get("id")),
        "summary": clean_text(b.get("subject")),
        "status": clean_text(status),
        "resolution": "", 
        "product": clean_text(proj),
        "component": clean_text(tracker), 
        "creation_time": clean_text(to_utc_iso_z(created)),
        "last_change_time": clean_text(to_utc_iso_z(updated)),
        "creator": clean_text(author),
        "assigned_to": clean_text(assigned),
        "keywords": [],           
        "url": f"{REDMINE_BASE.rstrip('/')}/issues/{b.get('id')}",
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
