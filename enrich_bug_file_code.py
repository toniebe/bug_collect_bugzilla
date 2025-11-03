import os, sys, json, re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import requests


BUGS_IN_PATH        = Path("bugs2.jsonl")
BUGS_OUT_PATH       = Path("datasource/bugs.with_file_code.jsonl")
PROGRESS_PATH       = Path("datasource/.progress.json")
AUTOSAVE_EVERY      = 25

GITHUB_API = "https://api.github.com"
RAW_BASE   = "https://raw.githubusercontent.com"

def log(msg: str):
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()

def gh_headers() -> Dict[str, str]:
    hdr = {"Accept": "application/vnd.github+json", "User-Agent": "bug-filecode/1.2"}
    tok = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if tok:
        hdr["Authorization"] = f"Bearer {tok}"
    return hdr

# ---------- Parser commit refs ----------
def parse_github_commit(u: str) -> Optional[Tuple[str,str,str]]:
    m = re.match(r"^https?://github\.com/([^/]+)/([^/]+)/commit/([0-9a-f]{7,40})$", u.strip())
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)

def parse_hg_commit(u: str) -> Optional[Tuple[str,str,str]]:
    m = re.match(r"^https?://hg(?:-edge)?\.mozilla\.org/([^/]+(?:/[^/]+)*)/rev/([0-9a-f]+)$", u.strip())
    if not m:
        return None
    base = m.group(1)   
    node = m.group(2)
    return "https://hg.mozilla.org/" + base, base, node

# ---------- GitHub ----------
def gh_commit_files(owner: str, repo: str, sha: str) -> List[Dict[str, Any]]:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/commits/{sha}"
    r = requests.get(url, headers=gh_headers(), timeout=60)
    r.raise_for_status()
    j = r.json()
    return j.get("files") or []

def gh_raw_url(owner: str, repo: str, sha: str, path: str) -> str:
    return f"{RAW_BASE}/{owner}/{repo}/{sha}/{path}"

def gh_repo_url(owner: str, repo: str) -> str:
    return f"https://github.com/{owner}/{repo}"

# ---------- Mercurial (hg.mozilla.org) ----------
def normalize_hg_base_url(base_url: str) -> str:
    return base_url.replace("hg-edge.mozilla.org", "hg.mozilla.org")

def hg_json_rev(base_url: str, node: str) -> Dict[str, Any]:
    url = f"{base_url}/json-rev/{node}"
    r = requests.get(url, headers={"User-Agent": "bug-filecode/1.2"}, timeout=60)
    r.raise_for_status()
    return r.json()

def hg_raw_file_url(base_url: str, node: str, path: str) -> str:
    return f"{base_url}/raw-file/{node}/{path}"

def hg_raw_rev_url(base_url: str, node: str) -> str:
    return f"{base_url}/raw-rev/{node}"

def hg_repo_url(base_name: str) -> str:
    return f"https://hg.mozilla.org/{base_name}"

def hg_fetch_files_meta(base_url: str, base_name: str, node: str) -> Tuple[List[Dict[str, Any]], str, str, str]:
    base_url = normalize_hg_base_url(base_url)
    changeset_patch_url = hg_raw_rev_url(base_url, node)

    candidates = []
    node_short = node[:12] if len(node) > 12 else node
    for nd in (node, node_short):
        candidates.append((base_url, nd))

    for bu, nd in candidates:
        try:
            j = hg_json_rev(bu, nd)
            files = j.get("files") or []
            if files:
                return files, bu, nd, changeset_patch_url
        except requests.HTTPError as he:
            # 404 → coba kandidat lain
            if he.response is not None and he.response.status_code == 404:
                continue
            # error lain → lanjut kandidat lain
            continue
        except requests.RequestException:
            continue

    # Fallback: parse raw-rev untuk ambil file list
    try:
        diff_resp = requests.get(changeset_patch_url, headers={"User-Agent": "bug-filecode/1.2"}, timeout=60)
        if diff_resp.status_code == 200:
            diff = diff_resp.text
            files = [{"file": m.group(1)} for m in re.finditer(r"^\+\+\+ b/(.+)$", diff, re.M)]
            if files:
                return files, base_url, node, changeset_patch_url
    except Exception:
        pass

    return [], base_url, node, changeset_patch_url

# ---------- Util struktur path ----------
def split_path_info(path: str) -> Tuple[str, str, Optional[str]]:
    parts = path.split("/")
    filename = parts[-1] if parts else path
    directory = "/".join(parts[:-1]) if len(parts) > 1 else ""
    ext = None
    if "." in filename and not filename.startswith("."):
        ext = filename.rsplit(".", 1)[-1]
    return directory, filename, ext

# ---------- Progress ----------
def load_progress() -> Dict[str, Any]:
    if PROGRESS_PATH.exists():
        try:
            return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_progress(state: Dict[str, Any]):
    PROGRESS_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# ====================================================
#                      MAIN
# ====================================================
def main():
    if not BUGS_IN_PATH.exists():
        log(f"[ERROR] Input tidak ditemukan: {BUGS_IN_PATH}")
        sys.exit(1)

    state = load_progress()
    resume_line = int(state.get("next_line", 0))
    processed_since_save = 0
    total_processed = 0

    BUGS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)

    with BUGS_IN_PATH.open("r", encoding="utf-8") as f_in, \
         BUGS_OUT_PATH.open("a", encoding="utf-8") as f_out:

        for i, raw in enumerate(f_in):
            if i < resume_line:
                continue

            raw = raw.strip()
            if not raw:
                state["next_line"] = i + 1
                continue

            try:
                bug = json.loads(raw)
            except Exception as e:
                log(f"[WARN] skip line {i}: bukan JSON ({e})")
                state["next_line"] = i + 1
                continue

            bug_id = bug.get("id")
            commit_refs: List[str] = bug.get("commit_refs") or []


            file_code: List[Dict[str, Any]] = []

            for cref in commit_refs:
                parsed = parse_github_commit(cref)
                if not parsed:
                    continue
                owner, repo, sha = parsed
                repo_id   = f"{owner}/{repo}"
                repo_url_ = gh_repo_url(owner, repo)

                try:
                    files = gh_commit_files(owner, repo, sha)
                except requests.HTTPError as he:
                    log(f"[ERROR] bug {bug_id}: gagal GitHub {repo_id}@{sha}: {he}")
                    continue
                except requests.RequestException as rexc:
                    log(f"[ERROR] bug {bug_id}: network GitHub {repo_id}@{sha}: {rexc}")
                    continue

                for finfo in files:
                    path = finfo.get("filename")
                    if not path:
                        continue
                    directory, filename, ext = split_path_info(path)
                    raw_url  = finfo.get("raw_url") or gh_raw_url(owner, repo, sha, path)
                    blob_url = finfo.get("blob_url")

                    file_code.append({
                        "system": "github",
                        "repo": repo_id,          
                        "repo_url": repo_url_,    
                        "rev": sha,              
                        "file_path": path,       
                        "dir": directory,        
                        "filename": filename,     
                        "ext": ext,              
                        "raw_url": raw_url,
                        "blob_url": blob_url,     
                        "status": finfo.get("status"),
                        "additions": finfo.get("additions"),
                        "deletions": finfo.get("deletions"),
                        "changes": finfo.get("changes"),
                    })

            # Mercurial
            for cref in commit_refs:
                parsed = parse_hg_commit(cref)
                if not parsed:
                    continue
                base_url, base_name, node = parsed
                repo_url_ = hg_repo_url(base_name)  


                files, used_base_url, used_node, changeset_patch_url = hg_fetch_files_meta(base_url, base_name, node)
                if not files:
                    log(f"[WARN] bug {bug_id}: tidak bisa dapat file list untuk {base_name}@{node}")
                    continue

                for finfo in files:
                    if not isinstance(finfo, dict):
                        continue
                    path = finfo.get("file")
                    if not path:
                        continue
                    directory, filename, ext = split_path_info(path)
                    raw_url = hg_raw_file_url(used_base_url, used_node, path)

                    file_code.append({
                        "system": "hg",
                        "repo": base_name,            
                        "repo_url": repo_url_,        
                        "rev": used_node,             
                        "file_path": path,
                        "dir": directory,
                        "filename": filename,
                        "ext": ext,
                        "raw_url": raw_url,            
                        "changeset_patch_url": changeset_patch_url 
                    })

            if "files_changed" in bug:
                del bug["files_changed"]
            bug["file_code"] = file_code

            f_out.write(json.dumps(bug, ensure_ascii=False) + "\n")
            f_out.flush()

            total_processed += 1
            processed_since_save += 1
            state["next_line"] = i + 1

            if processed_since_save >= AUTOSAVE_EVERY:
                save_progress(state)
                log(f"[INFO] Autosave: next_line={state['next_line']} total={total_processed}")
                processed_since_save = 0

        save_progress(state)
        log(f"[DONE] Selesai. next_line={state.get('next_line')} total={total_processed}")

if __name__ == "__main__":
    main()
