#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py
Run EasyFix step-by-step IN-PROCESS:
  1) 01_nlp_preprocess.py  (skipped if out_nlp/bugs_clean.csv exists, unless --force_nlp)
  2) 02_lda_topics.py

Logging: auto to log_YYYY-MM-DD.txt in this folder.
"""

import os, sys, argparse, datetime, importlib.util, contextlib

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------- logging ----------
def log_write(log_fh, msg):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line)
    if log_fh:
        try:
            log_fh.write(line + "\n"); log_fh.flush()
        except Exception:
            pass

# ---------- utils ----------
def file_nonempty(path: str) -> bool:
    try:
        return os.path.exists(path) and os.path.getsize(path) > 0
    except Exception:
        return False

@contextlib.contextmanager
def temp_argv(new_argv):
    old = sys.argv[:]
    sys.argv = new_argv
    try:
        yield
    finally:
        sys.argv = old

def load_module_from(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Run EasyFix NLP+LDA pipeline (in-process)")
    parser.add_argument("--input", type=str, default="datasource/bugs.jsonl", help="Path to Bugzilla JSONL")
    parser.add_argument("--nlp_out", type=str, default="out_nlp", help="Output dir for NLP step")
    parser.add_argument("--lda_out", type=str, default="out_lda", help="Output dir for LDA step")
    parser.add_argument("--num_topics", type=int, default=8, help="Number of topics (ignored if --auto_k)")
    parser.add_argument("--passes", type=int, default=8, help="LDA passes / max_iter")
    parser.add_argument("--auto_k", action="store_true", help="Choose K automatically")
    parser.add_argument("--sim_threshold", type=float, default=0.60, help="Similarity threshold for 'similar'")
    parser.add_argument("--dup_threshold", type=float, default=0.80, help="Similarity threshold for 'duplicate'")
    parser.add_argument("--force_nlp", action="store_true", help="Force re-run NLP even if bugs_clean.csv exists")
    args = parser.parse_args()

    # log file (UTC date)
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(HERE, f"log_{date_str}.txt")
    try:
        log_fh = open(log_path, "a", encoding="utf-8")
        log_write(log_fh, f"=== Pipeline started (log: {os.path.basename(log_path)}) ===")
    except Exception as e:
        log_fh = None
        print(f"[WARN] Could not open log file {log_path}: {e}")

    nlp_path = os.path.join(HERE, "01_nlp_preprocess.py")
    lda_path = os.path.join(HERE, "02_lda_topics.py")
    if not os.path.exists(nlp_path) or not os.path.exists(lda_path):
        log_write(log_fh, "Missing scripts (01_nlp_preprocess.py / 02_lda_topics.py)."); sys.exit(1)

    os.makedirs(args.nlp_out, exist_ok=True)
    os.makedirs(args.lda_out, exist_ok=True)
    bugs_clean_path = os.path.join(args.nlp_out, "bugs_clean.csv")

    # --- STEP 1: NLP (in-process) ---
    if not args.force_nlp and file_nonempty(bugs_clean_path):
        log_write(log_fh, f"[NLP] Skipped: found {bugs_clean_path}")
    else:
        log_write(log_fh, "[NLP] Running 01_nlp_preprocess.py in-process…")
        nlp_mod = load_module_from(nlp_path, "nlp_step")
        if not hasattr(nlp_mod, "main"):
            log_write(log_fh, "[NLP][ERROR] 01_nlp_preprocess.py must define main()"); sys.exit(1)
        # build argv for that module
        nlp_argv = [nlp_path, "--input", args.input, "--outdir", args.nlp_out]
        with temp_argv(nlp_argv):
            nlp_mod.main()
        if not file_nonempty(bugs_clean_path):
            log_write(log_fh, f"[NLP][ERROR] Missing or empty {bugs_clean_path} after NLP."); sys.exit(1)

    # --- STEP 2: LDA (in-process) ---
    
    lda_models = os.path.join(args.lda_out, "lda_sklearn_model_meta.npz")
    log_write(log_fh, "[LDA] Running 02_lda_topics.py in-process…")
    if file_nonempty(lda_models):
        log_write(log_fh, f"[LDA] Skipped: found {lda_models}")
    else:
        lda_mod = load_module_from(lda_path, "lda_step")
        if not hasattr(lda_mod, "main"):
            log_write(log_fh, "[LDA][ERROR] 02_lda_topics.py must define main()"); sys.exit(1)
        lda_argv = [
            lda_path, "--input", bugs_clean_path, "--outdir", args.lda_out,
            "--num_topics", str(args.num_topics), "--passes", str(args.passes),
            "--sim_threshold", str(args.sim_threshold), "--dup_threshold", str(args.dup_threshold)
        ]
        if args.auto_k:
            lda_argv.append("--auto_k")
        with temp_argv(lda_argv):
            lda_mod.main()
        
    # Step 3: Topic Cleaning
    log_write(log_fh, "[CLEAN] Running 03_clean_topics.py in-process…")
    from importlib import util
    clean_script = os.path.join(HERE, "03_clean_topics.py")
    log_write(log_fh, f"[DEBUG] HERE={HERE}, clean_script={clean_script}, exists={os.path.exists(clean_script)}")
    if os.path.exists(clean_script):
        spec = util.spec_from_file_location("clean_module", clean_script)
        mod = util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        # Emulate command-line args
        import sys
        sys.argv = [
            "03_clean_topics.py",
            "--topics", os.path.join(args.lda_out, "topics.csv"),
            "--bugs", os.path.join(args.lda_out, "bugs_with_topics.csv"),
            "--outdir", args.lda_out
        ]
        try:
            mod.main()
        except Exception as e:
            log_write(log_fh, f"[ERROR] Cleaning step failed: {e}")
        else:
            log_write(log_fh, "[WARN] Missing 03_clean_topics.py — skipping cleaning step")

        log_write(log_fh, "=== Pipeline finished successfully ===")
        if log_fh:
            try: log_fh.close()
            except Exception: pass

if __name__ == "__main__":
    main()
