import os, json, re, requests, time
from datetime import datetime, timedelta, timezone

# ====== CONFIG ======
BUGZILLA_BASE = os.getenv("BUGZILLA_BASE", "https://bugzilla.mozilla.org")
BUGZILLA_API_KEY = os.getenv("BUGZILLA_API_KEY", "BlcgQ07cwYUdywCWCBqwQSuTH8Vq04yDEZ9XMzA7") 
SINCE = os.getenv("SINCE", "2024-01-01")              
BY = os.getenv("BY", "creation_time")                 
PER_PAGE = int(os.getenv("PER_PAGE", "1000"))
MAX_TOTAL = int(os.getenv("MAX_TOTAL", "10000000"))
PRODUCTS = [p for p in os.getenv("PRODUCTS", "").split(",") if p.strip()]

OUT_JSONL = os.getenv("OUT_JSONL", "bugs2.jsonl")

# Enrichment flags
FETCH_COMMENTS        = os.getenv("FETCH_COMMENTS", "1") not in ("0","false","False")
FETCH_ATTACHMENTS     = os.getenv("FETCH_ATTACHMENTS","1") not in ("0","false","False")
MAX_ATTACH_PER_BUG    = int(os.getenv("MAX_ATTACH_PER_BUG", "2"))
MAX_ATTACH_BYTES      = int(os.getenv("MAX_ATTACH_BYTES", "200000"))
FILTER_REQUIRE_COMMIT = os.getenv("FILTER_REQUIRE_COMMIT","1") not in ("0","false","False") 

# Resume flags
RESUME                = os.getenv("RESUME","1") not in ("0","false","False")
RESUME_SINCE_EXISTING = os.getenv("RESUME_SINCE_EXISTING","1") not in ("0","false","False")
SAVE_EVERY            = int(os.getenv("SAVE_EVERY","50"))

# ====== REGEX & CLEAN HELPERS ======
URL_RE   = re.compile(r"https?://\S+")
SPACE_RE = re.compile(r"\s+")
def clean_text(x):
    if x is None: return ""
    if not isinstance(x, str): x = str(x)
    x = URL_RE.sub(" ", x)
    x = SPACE_RE.sub(" ", x).strip()
    return x

def as_list(x): return [] if x is None else (x if isinstance(x, list) else [x])
def as_int_or_none(x):
    try:
        i = int(x); return i if i != 0 else None
    except: return None

# commit/changes extraction
HG_COMMIT_URL  = re.compile(r"https?://[\w\.\-]*hg\.mozilla\.org/\S*/rev/([0-9a-f]{8,40})", re.I)
GH_COMMIT_URL  = re.compile(r"https?://github\.com/\S+?/commit/([0-9a-f]{7,40})", re.I)
CHANGESET_HASH = re.compile(r"\bchangeset[: ]+([0-9a-f]{7,40})\b", re.I)

SUBJECT_LINE   = re.compile(r"^Subject:\s*(.+)$", re.I|re.M)
BUG_TITLE_LINE = re.compile(r"\bBug\s+\d+\s*[-:]\s*(.+)", re.I)
COMMIT_BLOCK   = re.compile(r"(?m)^commit\s+[0-9a-f]{7,40}\s*$[\s\S]*?(?:\n\n([ \t].+?)(?:\n{2,}|\Z))")
NON_URL_LINE   = re.compile(r"^\s*(?!https?://)\S.*$")

DIFF_GIT_FILE = re.compile(r"^diff --git a/(.+?) b/\1$", re.M)
MINUS_FILE    = re.compile(r"^---\s+a/(.+)$", re.M)
PLUS_FILE     = re.compile(r"^\+\+\+\s+b/(.+)$", re.M)
INDEX_FILE    = re.compile(r"^Index:\s+(.+)$", re.M)

CODE_FILE_EXTS = (
    ".c",".cc",".cpp",".h",".hpp",".m",".mm",".java",".kt",".swift",
    ".py",".js",".ts",".jsx",".tsx",".rb",".php",".cs",".go",".rs",
    ".sh",".bash",".zsh",".ps1",".scala",".lua",".pl",".r",".mjs",
    ".css",".scss",".less",".html",".xml",".yml",".yaml",".toml",".ini",".json",".proto"
)
LIKELY_PATH = re.compile(r"([A-Za-z0-9_\-./]+(?:%s))" % "|".join(re.escape(ext) for ext in CODE_FILE_EXTS))

def extract_commit_refs(text: str):
    s = text or ""
    refs = set()
    for m in HG_COMMIT_URL.finditer(s): refs.add(m.group(0))
    for m in GH_COMMIT_URL.finditer(s): refs.add(m.group(0))
    for m in CHANGESET_HASH.finditer(s): refs.add(m.group(1))
    return refs

def _nearby_message_lines(lines, idx, lookback=2, lookahead=2):
    msgs=[]
    for j in range(max(0, idx - lookback), idx):
        if NON_URL_LINE.match(lines[j]): msgs.append(lines[j].strip())
    for j in range(idx+1, min(len(lines), idx + lookahead + 1)):
        if NON_URL_LINE.match(lines[j]): msgs.append(lines[j].strip())
    return msgs

def extract_commit_messages(text: str):
    s = text or ""
    msgs = set()

    for m in SUBJECT_LINE.finditer(s):
        val = m.group(1).strip()
        if val: msgs.add(val)

    for m in BUG_TITLE_LINE.finditer(s):
        val = m.group(1).strip()
        if val: msgs.add(val)

    for m in COMMIT_BLOCK.finditer(s):
        para = m.group(1) or ""
        para = "\n".join([ln.lstrip() for ln in para.splitlines()]).strip()
        first = para.splitlines()[0].strip() if para else ""
        if first: msgs.add(first)

    if HG_COMMIT_URL.search(s) or GH_COMMIT_URL.search(s):
        lines = s.splitlines()
        for i, _ in enumerate(lines):
            if HG_COMMIT_URL.search(lines[i]) or GH_COMMIT_URL.search(lines[i]):
                for cand in _nearby_message_lines(lines, i):
                    if "http://" in cand or "https://" in cand: 
                        continue
                    if DIFF_GIT_FILE.match(cand) or MINUS_FILE.match(cand) or PLUS_FILE.match(cand) or INDEX_FILE.match(cand):
                        continue
                    if len(cand) >= 6:
                        msgs.add(cand.strip())

    out = []
    for m in msgs:
        t = re.sub(r"\s+", " ", m).strip()
        if t:
            out.append(t[:300])
    return set(out)

def extract_files_changed(text: str):
    s = text or ""
    files = set()
    for m in DIFF_GIT_FILE.finditer(s): files.add(m.group(1).strip())
    minus = [m.group(1).strip() for m in MINUS_FILE.finditer(s)]
    plus  = [m.group(1).strip() for m in PLUS_FILE.finditer(s)]
    for name in minus + plus:
        if name and name != "/dev/null":
            files.add(name)
    for m in INDEX_FILE.finditer(s): files.add(m.group(1).strip())
    for m in LIKELY_PATH.finditer(s): files.add(m.group(1).strip())
    norm = []
    for f in files:
        norm.append(f[2:] if (f.startswith("a/") or f.startswith("b/")) else f)
    return sorted(set(norm))

# ====== HTTP SAFE GET ======
RETRY_STATUS = {429, 502, 503, 504}
def _safe_get(url, params=None, timeout=120, stream=False, max_retry=1):
    params = params or {}
    tries = 0
    while True:
        tries += 1
        try:
            r = requests.get(url, params=params, timeout=timeout, stream=stream)
            if r.status_code == 400:
                return None
            if r.status_code in RETRY_STATUS and tries <= max_retry + 1:
                time.sleep(5)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException:
            return None

# ====== MONTH RANGE ======
def month_range(start_dt, end_dt):
    cur = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    while cur <= end_dt:
        if cur.month == 12:
            nxt = datetime(cur.year+1, 1, 1, tzinfo=timezone.utc)
        else:
            nxt = datetime(cur.year, cur.month+1, 1, tzinfo=timezone.utc)
        yield cur, min(nxt - timedelta(seconds=1), end_dt)
        cur = nxt

# ====== RESUME HELPERS ======
def iter_existing_ids(out_path):
    """Kumpulkan id yang sudah diproses di OUT_JSONL (untuk skip)."""
    ids = set()
    if not os.path.isfile(out_path): return ids
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                obj=json.loads(line)
                bid=obj.get("id")
                if isinstance(bid, int): ids.add(bid)
            except: pass
    return ids

def compute_resume_since(out_path, default_since: str):
    """
    Jika RESUME_SINCE_EXISTING=1: ambil max(creation_time) dari OUT_JSONL,
    lalu set SINCE = (max_creation_time - 1 hari) agar tidak fetch jauh ke belakang.
    Kalau file tidak ada/format kosong, kembalikan default.
    """
    if not (RESUME_SINCE_EXISTING and os.path.isfile(out_path)):
        return default_since
    max_ct = None
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                obj=json.loads(line)
                ct = obj.get("creation_time")
                if not ct: continue
                t = datetime.fromisoformat(ct.replace("Z","").replace("z",""))
                if t.tzinfo is None: t = t.replace(tzinfo=timezone.utc)
                else: t = t.astimezone(timezone.utc)
                if (max_ct is None) or (t > max_ct):
                    max_ct = t
            except: pass
    if max_ct is None:
        return default_since
    resume_since_dt = (max_ct - timedelta(days=1)).replace(tzinfo=timezone.utc)
    return resume_since_dt.strftime("%Y-%m-%d")

# ====== FETCH BUG LIST BY DATE ======
def fetch_bugs_by_date(since_str: str):
    base_url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug"
    include_fields = [
        "id","summary","status","resolution","product","component",
        "creation_time","last_change_time","creator","assigned_to",
        "keywords","url","depends_on","dupe_of"
    ]
    start_dt = datetime.fromisoformat(since_str).replace(tzinfo=timezone.utc)
    end_dt   = datetime.now(timezone.utc)

    all_rows = {}
    for w_start, w_end in month_range(start_dt, end_dt):
        params = {
            "include_fields": include_fields,
            "order": "creation_time asc",
            "limit": PER_PAGE,
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
            r = _safe_get(base_url, params=params, timeout=120)
            if r is None:
                # skip window page pada error 400/exception
                break
            try:
                data = r.json()
            except Exception:
                break
            page = data.get("bugs", []) or []
            if not page:
                break
            for b in page:
                all_rows[b["id"]] = b   # de-dup by id
                if len(all_rows) >= MAX_TOTAL:
                    break
            if len(all_rows) >= MAX_TOTAL: break
            if len(page) < params["limit"]:
                break
            offset += params["limit"]
        if len(all_rows) >= MAX_TOTAL: break

    return list(all_rows.values())

# ====== FETCH COMMENTS & ATTACHMENTS ======
def fetch_comments(bug_id: int):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/comment"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    r = _safe_get(url, params=params, timeout=120)
    if r is None:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out=[]
    bug_node=(data.get("bugs") or {}).get(str(bug_id), {})
    for c in bug_node.get("comments", []) or []:
        txt=c.get("text") or ""
        if txt: out.append(txt)
    return out

def fetch_attachments_meta(bug_id: int):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/attachment"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    r = _safe_get(url, params=params, timeout=120)
    if r is None:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    return data.get("attachments", []) or []

def fetch_attachment_data(attach_id: int):
    # JSON base64 dulu
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/attachment/{attach_id}"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    r = _safe_get(url, params=params, timeout=120)
    if r is not None:
        try:
            data=r.json()
            atts=data.get("attachments") or {}
            att=atts.get(str(attach_id)) or {}
            content=att.get("data")
            if isinstance(content, str) and content:
                import base64
                raw=base64.b64decode(content, validate=False)
                return raw[:MAX_ATTACH_BYTES]
        except Exception:
            pass
    # fallback: CGI (binary)
    cgi = f"{BUGZILLA_BASE.rstrip('/')}/attachment.cgi"
    r = _safe_get(cgi, params={"id": attach_id}, timeout=180, stream=True)
    if r is None:
        return b""
    buf=b""
    try:
        for chunk in r.iter_content(chunk_size=8192):
            if not chunk: break
            buf += chunk
            if len(buf) > MAX_ATTACH_BYTES: break
    except Exception:
        return b""
    return buf

# ====== CLEANING + ENRICH ======
def clean_bug(b, commit_messages=None, commit_refs=None, files_changed=None):
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
        "commit_messages": sorted(set(commit_messages or [])),
        "commit_refs": sorted(set(commit_refs or [])),
        "files_changed": sorted(set(files_changed or [])),
    }

def enrich_one_bug(b):
    bid = b.get("id")
    try:
        bid = int(bid)
    except:
        return None

    commit_msgs=set()
    commit_refs=set()
    files=set()

    if FETCH_COMMENTS:
        try:
            for txt in fetch_comments(bid):
                commit_msgs.update(extract_commit_messages(txt))
                commit_refs.update(extract_commit_refs(txt))
                files.update(extract_files_changed(txt))
        except Exception as e:
            print(f"[warn] comments {bid} -> {e}")

    if FETCH_ATTACHMENTS:
        try:
            metas=fetch_attachments_meta(bid)
            taken=0
            for att in metas:
                if taken >= MAX_ATTACH_PER_BUG: break
                if att.get("is_obsolete"): continue
                name = (att.get("file_name") or "").lower()
                ctype = (att.get("content_type") or "").lower()
                if not any(x in ctype for x in ("text","log","patch","diff","x-diff","x-patch","plain")) \
                   and not (name.endswith((".patch",".diff")) or any(name.endswith(ext) for ext in CODE_FILE_EXTS)):
                    continue
                raw=fetch_attachment_data(att["id"])
                if not raw: continue
                try:
                    txt=raw.decode("utf-8", errors="replace")
                except:
                    txt=""
                if not txt: continue
                commit_msgs.update(extract_commit_messages(txt))
                commit_refs.update(extract_commit_refs(txt))
                files.update(extract_files_changed(txt))
                taken += 1
        except Exception as e:
            print(f"[warn] attachments {bid} -> {e}")

    out = clean_bug(
        b,
        commit_messages=list(commit_msgs),
        commit_refs=list(commit_refs),
        files_changed=list(files)
    )
    if FILTER_REQUIRE_COMMIT and not (out["commit_messages"] or out["commit_refs"]):
        return None
    return out

# ====== SAVE (APPEND) ======
def append_jsonl(path, rows):
    if not rows: return
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ====== MAIN ======
if __name__ == "__main__":
    # 1) Tentukan SINCE untuk fetch
    since_effective = compute_resume_since(OUT_JSONL, SINCE) if RESUME else SINCE
    if RESUME and since_effective != SINCE:
        print(f"[resume] set SINCE -> {since_effective} (based on existing output)")

    # 2) Kumpulkan ID yang sudah diproses agar skip
    already = iter_existing_ids(OUT_JSONL) if RESUME else set()
    if RESUME:
        print(f"[resume] will skip {len(already)} existing IDs in {OUT_JSONL}")

    # 3) Fetch daftar bug semenjak SINCE efektif
    print(f"fetching since={since_effective} ...")
    raw = fetch_bugs_by_date(since_effective)
    print("raw:", len(raw))

    # 4) Urutkan by creation_time (asc) untuk stabilitas penulisan
    raw.sort(key=lambda x: x.get("creation_time",""))

    # 5) Enrich & tulis bertahap (APPEND) + skip ID existing
    buf = []
    total = 0
    written = 0
    for b in raw:
        total += 1
        try:
            bid = int(b.get("id"))
        except:
            continue
        if bid in already:
            continue

        enr = enrich_one_bug(b)
        if enr is not None:
            buf.append(enr)
            written += 1

        if len(buf) >= SAVE_EVERY:
            append_jsonl(OUT_JSONL, buf)
            print(f"[save] +{len(buf)} (total_written={written})")
            buf.clear()

    if buf:
        append_jsonl(OUT_JSONL, buf)
        print(f"[save] +{len(buf)} (total_written={written})")

    print(f"done. fetched={total}, written={written}, out={OUT_JSONL}")
