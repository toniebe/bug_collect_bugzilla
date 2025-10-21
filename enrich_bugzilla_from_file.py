import os, re, json, time, requests
from datetime import datetime, timezone

# ========= KONFIGURASI TANPA ARGUMEN =========
BUGZILLA_BASE    = os.getenv("BUGZILLA_BASE", "https://bugzilla.mozilla.org")
BUGZILLA_API_KEY = os.getenv("BUGZILLA_API_KEY", "BlcgQ07cwYUdywCWCBqwQSuTH8Vq04yDEZ9XMzA7")

IN_PATH   = os.getenv("IN_PATH",  "datasource/bugs.jsonl")        
OUT_PATH  = os.getenv("OUT_PATH", "datasource/bug_enriched_commit_message.jsonl")
SAVE_EVERY = int(os.getenv("SAVE_EVERY", "25"))
RESUME     = os.getenv("RESUME", "1") not in ("0","false","False")

FETCH_COMMENTS        = os.getenv("FETCH_COMMENTS", "1") not in ("0","false","False")
FETCH_ATTACHMENTS     = os.getenv("FETCH_ATTACHMENTS","1") not in ("0","false","False")
MAX_ATTACH_PER_BUG    = int(os.getenv("MAX_ATTACH_PER_BUG", "2"))
MAX_ATTACH_BYTES      = int(os.getenv("MAX_ATTACH_BYTES", "200000"))
FILTER_REQUIRE_COMMIT = os.getenv("FILTER_REQUIRE_COMMIT","1") not in ("0","false","False")

# ========= REGEX EKSTRAKSI =========
# URL/hashes commit
HG_COMMIT_URL  = re.compile(r"https?://[\w\.\-]*hg\.mozilla\.org/\S*/rev/([0-9a-f]{8,40})", re.I)
GH_COMMIT_URL  = re.compile(r"https?://github\.com/\S+?/commit/([0-9a-f]{7,40})", re.I)
CHANGESET_HASH = re.compile(r"\bchangeset[: ]+([0-9a-f]{7,40})\b", re.I)

def extract_commit_refs(text: str):
    s = text or ""
    refs = set()
    for m in HG_COMMIT_URL.finditer(s): refs.add(m.group(0))
    for m in GH_COMMIT_URL.finditer(s): refs.add(m.group(0))
    for m in CHANGESET_HASH.finditer(s): refs.add(m.group(1))
    return refs

# Pola pesan commit yang umum
SUBJECT_LINE   = re.compile(r"^Subject:\s*(.+)$", re.I|re.M)
BUG_TITLE_LINE = re.compile(r"\bBug\s+\d+\s*[-:]\s*(.+)", re.I)
COMMIT_BLOCK   = re.compile(
    r"(?m)^commit\s+[0-9a-f]{7,40}\s*$[\s\S]*?(?:\n\n([ \t].+?)(?:\n{2,}|\Z))"
)
NON_URL_LINE   = re.compile(r"^\s*(?!https?://)\S.*$")

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

# ========= I/O =========
def load_input(path):
    if path.lower().endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line: continue
                try:
                    obj=json.loads(line)
                    if isinstance(obj, dict): yield obj
                except: pass
    else:
        with open(path, "r", encoding="utf-8") as f:
            data=json.load(f)
        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict): yield obj
        elif isinstance(data, dict):
            if "bugs" in data and isinstance(data["bugs"], list):
                for obj in data["bugs"]:
                    if isinstance(obj, dict): yield obj
            else:
                for _, obj in data.items():
                    if isinstance(obj, dict): yield obj

def iter_existing_ids(out_path):
    ids=set()
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

def append_jsonl(path, rows):
    if not rows: return
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# ========= Bugzilla detail (komentar/attachment) =========
def fetch_comments(bug_id: int):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/comment"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    r = requests.get(url, params=params, timeout=120)
    if r.status_code in (429,502,503,504):
        time.sleep(5); r=requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data=r.json()
    out=[]
    bug_node=(data.get("bugs") or {}).get(str(bug_id), {})
    for c in bug_node.get("comments", []) or []:
        txt=c.get("text") or ""
        if txt: out.append(txt)
    return out

def fetch_attachments_meta(bug_id: int):
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/{bug_id}/attachment"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    r = requests.get(url, params=params, timeout=120)
    if r.status_code in (429,502,503,504):
        time.sleep(5); r=requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data=r.json()
    return data.get("attachments", []) or []

def fetch_attachment_data(attach_id: int):
    # coba API JSON base64 dulu
    url = f"{BUGZILLA_BASE.rstrip('/')}/rest/bug/attachment/{attach_id}"
    params = {"api_key": BUGZILLA_API_KEY} if BUGZILLA_API_KEY else {}
    try:
        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        data=r.json()
        atts=data.get("attachments") or {}
        att=atts.get(str(attach_id)) or {}
        content=att.get("data")
        if isinstance(content, str) and content:
            import base64
            raw=base64.b64decode(content, validate=False)
            return raw[:MAX_ATTACH_BYTES]
    except: pass
    # fallback: CGI (binary)
    cgi = f"{BUGZILLA_BASE.rstrip('/')}/attachment.cgi"
    r = requests.get(cgi, params={"id": attach_id}, timeout=180, stream=True)
    r.raise_for_status()
    buf=b""
    for chunk in r.iter_content(chunk_size=8192):
        if not chunk: break
        buf += chunk
        if len(buf) > MAX_ATTACH_BYTES: break
    return buf

# ========= Ekstraksi FILES =========
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

def looks_like_code_attachment(att):
    name = (att.get("file_name") or "").lower()
    ctype = (att.get("content_type") or "").lower()
    if any(x in ctype for x in ("text","log","patch","diff","x-diff","x-patch","plain")): return True
    if name.endswith((".patch",".diff")): return True
    if name.endswith(CODE_FILE_EXTS): return True
    return False

# ========= Ekstraksi COMMIT MESSAGES =========
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
        for i, ln in enumerate(lines):
            if HG_COMMIT_URL.search(ln) or GH_COMMIT_URL.search(ln):
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

# ========= Cleaning =========
def clean_text(x):
    if x is None: return ""
    if not isinstance(x, str): x=str(x)
    x = re.sub(r"https?://\S+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def as_list(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]

def as_int_or_none(x):
    try:
        i=int(x)
        return i if i!=0 else None
    except: return None

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

# ========= ENRICH =========
def enrich_one(bug):
    bid = bug.get("id")
    try: bid=int(bid)
    except: return None

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
                   and not (name.endswith((".patch",".diff")) or name.endswith(CODE_FILE_EXTS)):
                    continue

                raw=fetch_attachment_data(att["id"])
                if not raw: continue
                try:
                    txt=raw.decode("utf-8", errors="replace")
                except: txt=""
                if not txt: continue
                commit_msgs.update(extract_commit_messages(txt))
                commit_refs.update(extract_commit_refs(txt))
                files.update(extract_files_changed(txt))
                taken += 1
        except Exception as e:
            print(f"[warn] attachments {bid} -> {e}")

    out = clean_bug(
        bug,
        commit_messages=list(commit_msgs),
        commit_refs=list(commit_refs),
        files_changed=list(files)
    )
    if FILTER_REQUIRE_COMMIT and not (out["commit_messages"] or out["commit_refs"]):
        return None
    return out

# ========= MAIN =========
if __name__ == "__main__":
    if not os.path.exists(IN_PATH):
        raise SystemExit(f"input missing: {IN_PATH}")

    # resume: skip id yang sudah ada di OUT_PATH
    already = iter_existing_ids(OUT_PATH) if RESUME else set()
    if RESUME:
        print(f"[resume] skipping {len(already)} existing IDs in {OUT_PATH}")

    buf=[]; total=0; written=0
    for bug in load_input(IN_PATH):
        total += 1
        bid = bug.get("id")
        try: bid_int=int(bid)
        except: continue
        if bid_int in already: continue

        out = enrich_one(bug)
        if out is None:
            pass
        else:
            buf.append(out); written += 1

        if len(buf) >= SAVE_EVERY:
            append_jsonl(OUT_PATH, buf)
            print(f"[save] +{len(buf)} (total_written={written})")
            buf.clear()

    if buf:
        append_jsonl(OUT_PATH, buf)
        print(f"[save] +{len(buf)} (total_written={written})")

    print(f"done. input={total}, written={written}, out={OUT_PATH}")
