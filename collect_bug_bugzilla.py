import os, re, json, time, requests
from datetime import datetime, timedelta, timezone

# ====== CONFIG ======
BUGZILLA_BASE = os.getenv("BUGZILLA_BASE", "https://bugzilla.mozilla.org")
BUGZILLA_API_KEY = os.getenv("BUGZILLA_API_KEY", os.getenv("BlcgQ07cwYUdywCWCBqwQSuTH8Vq04yDEZ9XMzA7", ""))
SINCE = os.getenv("SINCE", "2025-01-01")
OUT_JSONL = os.getenv("OUT_JSONL", "bugzilla_bugs.jsonl")
PRODUCTS = [p for p in os.getenv("PRODUCTS", "").split() if p]

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "1000"))
MAX_TOTAL  = int(os.getenv("MAX_TOTAL",  "1000000"))

# Enrichment options
FETCH_COMMENTS        = os.getenv("FETCH_COMMENTS", "1")
FETCH_ATTACHMENTS     = os.getenv("FETCH_ATTACHMENTS", "1") 
MAX_ATTACH_PER_BUG    = int(os.getenv("MAX_ATTACH_PER_BUG", "2"))
MAX_ATTACH_BYTES      = int(os.getenv("MAX_ATTACH_BYTES", "200000"))
FILTER_REQUIRE_COMMIT = os.getenv("FILTER_REQUIRE_COMMIT", "0")

# ====== HELPERS ======
def to_utc_iso_z(s):
    if not s: return ""
    s = str(s)
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z","").replace("z",""))
        else:
            dt = datetime.fromisoformat(s)
    except Exception:
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
    except Exception:
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

# ====== FETCH BUG LIST ======
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
            "limit": PAGE_LIMIT,
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
            r = requests.get(base_url, params=params, timeout=180)
            if r.status_code in (429, 502, 503, 504):
                wait = 5
                print(f"[warn] {r.status_code} retry in {wait}s url={r.url}")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            page = data.get("bugs", [])
            if not page:
                break
            for b in page:
                all_rows[b["id"]] = b
                if len(all_rows) >= MAX_TOTAL:
                    break
            if len(all_rows) >= MAX_TOTAL:
                break
            if len(page) < params["limit"]:
                break
            offset += params["limit"]
        if len(all_rows) >= MAX_TOTAL:
            break
    return list(all_rows.values())

# ====== COMMENTS & ATTACHMENTS ======
def fetch_comments(bug_id):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/comment"
    params = {}
    if BUGZILLA_API_KEY:
        params["api_key"] = BUGZILLA_API_KEY
    r = requests.get(url, params=params, timeout=120)
    if r.status_code in (429, 502, 503, 504):
        time.sleep(5)
        r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()
    comments = []
    bug_node = (data.get("bugs") or {}).get(str(bug_id), {})
    for c in bug_node.get("comments", []) or []:
        txt = c.get("text") or ""
        if txt:
            comments.append(txt)
    return comments

def fetch_attachments_meta(bug_id):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/attachment"
    params = {}
    if BUGZILLA_API_KEY:
        params["api_key"] = BUGZILLA_API_KEY
    r = requests.get(url, params=params, timeout=120)
    if r.status_code in (429, 502, 503, 504):
        time.sleep(5)
        r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()
    return data.get("attachments", []) or []

def fetch_attachment_data(attach_id):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/attachment/{attach_id}"
    params = {}
    if BUGZILLA_API_KEY:
        params["api_key"] = BUGZILLA_API_KEY
    try:
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        atts = data.get("attachments") or {}
        att = atts.get(str(attach_id)) or {}
        content = att.get("data")
        if isinstance(content, str) and content:
            import base64
            raw = base64.b64decode(content, validate=False)
            return raw[:MAX_ATTACH_BYTES]
    except Exception:
        pass
    # 2) fallback CGI stream (binary)
    cgi = f"{BUGZILLA_BASE.rstrip('/')}/attachment.cgi"
    r = requests.get(cgi, params={"id": attach_id}, timeout=180, stream=True)
    r.raise_for_status()
    buf = b""
    for chunk in r.iter_content(chunk_size=8192):
        if not chunk: break
        buf += chunk
        if len(buf) > MAX_ATTACH_BYTES:
            break
    return buf

# ====== EXTRACT COMMIT REFS & FILE CHANGES ======
HG_COMMIT_URL = re.compile(r"https?://[\w\.\-]*hg\.mozilla\.org/\S*/rev/([0-9a-f]{8,40})", re.I)
GH_COMMIT_URL = re.compile(r"https?://github\.com/\S+?/commit/([0-9a-f]{7,40})", re.I)
CHANGESET_HASH = re.compile(r"\bchangeset[: ]+([0-9a-f]{7,40})\b", re.I)

def extract_commit_refs(text):
    refs = set()
    for m in HG_COMMIT_URL.finditer(text or ""):
        refs.add(m.group(0))
    for m in GH_COMMIT_URL.finditer(text or ""):
        refs.add(m.group(0))
    for m in CHANGESET_HASH.finditer(text or ""):
        refs.add(m.group(1))
    return list(refs)

# Diff/header patterns → nama file
DIFF_GIT_FILE = re.compile(r"^diff --git a/(.+?) b/\1$", re.M)
MINUS_FILE    = re.compile(r"^---\s+a/(.+)$", re.M)
PLUS_FILE     = re.compile(r"^\+\+\+\s+b/(.+)$", re.M)
INDEX_FILE    = re.compile(r"^Index:\s+(.+)$", re.M)

# fallback: baris yang terlihat seperti path file kode
CODE_FILE_EXTS = (
    ".c",".cc",".cpp",".h",".hpp",".m",".mm",".java",".kt",".swift",
    ".py",".js",".ts",".jsx",".tsx",".rb",".php",".cs",".go",".rs",
    ".sh",".bash",".zsh",".ps1",".scala",".lua",".pl",".r",".mjs",
    ".css",".scss",".less",".html",".xml",".yml",".yaml",".toml",".ini",".json",".proto"
)
LIKELY_PATH = re.compile(r"([A-Za-z0-9_\-./]+(?:%s))" % "|".join(re.escape(ext) for ext in CODE_FILE_EXTS))

def extract_files_changed(text):
    s = text or ""
    files = set()

    # diff --git a/x b/x
    for m in DIFF_GIT_FILE.finditer(s):
        files.add(m.group(1).strip())

    # --- a/x  +++ b/x
    minus = [m.group(1).strip() for m in MINUS_FILE.finditer(s)]
    plus  = [m.group(1).strip() for m in PLUS_FILE.finditer(s)]
    for name in minus + plus:
        # --- /dev/null pada file baru
        if name and name != "/dev/null":
            files.add(name)

    # Index: path
    for m in INDEX_FILE.finditer(s):
        files.add(m.group(1).strip())

    # fallback
    for m in LIKELY_PATH.finditer(s):
        files.add(m.group(1).strip())

    # Normalisasi kecil: hilangkan prefix a/ atau b/
    norm = []
    for f in files:
        if f.startswith("a/") or f.startswith("b/"):
            norm.append(f[2:])
        else:
            norm.append(f)
    return sorted(set(norm))

def looks_like_code_attachment(att):
    name = (att.get("file_name") or "").lower()
    ctype = (att.get("content_type") or "").lower()
    if any(x in ctype for x in ("text", "log", "patch", "diff", "x-diff", "x-patch", "plain")):
        return True
    if name.endswith((".patch",".diff")):
        return True
    if name.endswith(CODE_FILE_EXTS):
        return True
    return False

# ====== CLEANING ======
def clean_bug(b, commit_refs=None, files_changed=None):
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
        "commit_refs": sorted(set(commit_refs or [])),
        "files_changed": sorted(set(files_changed or [])),
    }

def clean_dataset(rows):
    tmp = {}
    for b in rows:
        if "id" not in b: 
            continue
        i = int(b.get("id"))
        old = tmp.get(i)
        if not old:
            tmp[i] = b
        else:
            if (b.get("last_change_time","") or "") > (old.get("last_change_time","") or ""):
                tmp[i] = b
    out = list(tmp.values())
    out.sort(key=lambda x: x.get("creation_time",""))
    return out

# ====== SAVE ======
def save_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ====== ENRICH ======
def enrich_bug_with_commits_and_files(bug):
    bid = int(bug["id"])
    commit_refs = set()
    files_changed = set()

    if FETCH_COMMENTS:
        try:
            comments = fetch_comments(bid)
            for txt in comments:
                commit_refs.update(extract_commit_refs(txt))
                files_changed.update(extract_files_changed(txt))
        except Exception as e:
            print(f"[warn] comments {bid} -> {e}")

    if FETCH_ATTACHMENTS:
        try:
            metas = fetch_attachments_meta(bid)
            chosen = 0
            for att in metas:
                if chosen >= MAX_ATTACH_PER_BUG:
                    break
                if att.get("is_obsolete"):
                    continue
                if not looks_like_code_attachment(att):
                    continue
                raw = fetch_attachment_data(att["id"])
                if not raw:
                    continue
                try:
                    txt = raw.decode("utf-8", errors="replace")
                except Exception:
                    txt = ""
                if not txt:
                    continue
                commit_refs.update(extract_commit_refs(txt))
                files_changed.update(extract_files_changed(txt))
                chosen += 1
        except Exception as e:
            print(f"[warn] attachments {bid} -> {e}")

    return list(commit_refs), list(files_changed)

# ====== MAIN ======
if __name__ == "__main__":
    print(f"fetching since={SINCE} base={BUGZILLA_BASE} ...")
    raw = fetch_bugs_by_date()
    print("raw:", len(raw))

    base_clean = clean_dataset(raw)

    enriched = []
    for i, b in enumerate(base_clean, 1):
        try:
            commits, files = enrich_bug_with_commits_and_files(b)
        except Exception as e:
            print(f"[warn] enrich bug {b.get('id')} -> {e}")
            commits, files = [], []

        # jika ingin hanya menyimpan bug yg punya commit refs, aktifkan FILTER_REQUIRE_COMMIT=1
        if FILTER_REQUIRE_COMMIT and not commits:
            if i % 100 == 0:
                print(f"  skipped(no-commit) {i}/{len(base_clean)}")
            continue

        cb = clean_bug(b, commit_refs=commits, files_changed=files)
        enriched.append(cb)
        if i % 100 == 0:
            print(f"  enriched {i}/{len(base_clean)}")

    print("clean+enrich:", len(enriched))
    save_jsonl(OUT_JSONL, enriched)
    print("saved ->", OUT_JSONL)
