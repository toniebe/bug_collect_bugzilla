#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
01_nlp_preprocess.py
- Load Bugzilla JSONL (e.g., datasource/bugs.jsonl)
- Clean & normalize text (summary/description)
- Output a CSV with clean_text and essential metadata for later modeling.
"""

import os, re, string, json, argparse, warnings
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------- NLP Utilities ----------

def ensure_nltk():
    import nltk
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt")
    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        nltk.download("stopwords")

def build_stopwords():
    from nltk.corpus import stopwords
    sw = set()
    # English & Indonesian
    try:
        sw |= set(stopwords.words("english"))
    except:
        pass
    try:
        sw |= set(stopwords.words("indonesian"))
    except:
        pass
    # Technical/common words to de-emphasize topics
    sw |= {
        "error","issue","bug","fix","fixed","problem","invalid","message","crash",
        "firefox","mozilla","general","component","please","thanks",
        "step","steps","reproduce","expected","actual"
    }
    # single letters
    for ch in "abcdefghijklmnopqrstuvwxyz":
        sw.add(ch)
    return sw

def clean_text(text, sw):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # remove URLs, git hashes, explicit bug ids, keep short path markers minimally
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"[a-f0-9]{7,40}", " ", text)           # git hashes and similar
    text = re.sub(r"bug\s*#?\s*\d+", " ", text)           # explicit Bug IDs
    text = re.sub(r"[^\w\s\./-]+", " ", text)             # keep / . - to preserve short paths
    tokens = re.split(r"\s+", text)

    def ok(tok):
        if not tok: return False
        if tok in sw: return False
        if tok.isdigit(): return False
        if len(tok) < 3: return False
        # overly long path-ish tokens → drop
        if tok.count(".") > 3 or tok.count("/") > 3: return False
        return True

    tokens = [t.strip(string.punctuation) for t in tokens]
    tokens = [t for t in tokens if ok(t)]
    return " ".join(tokens)

# ---------- IO ----------

def load_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            rows.append(json.loads(line))
    return pd.DataFrame(rows)

def main():
    parser = argparse.ArgumentParser(description="NLP preprocessing for EasyFix bug reports")
    parser.add_argument("--input", type=str, default="datasource/bugs.jsonl", help="Path to Bugzilla JSONL")
    parser.add_argument("--outdir", type=str, default="out_nlp", help="Output directory")
    parser.add_argument("--text-cols", type=str, default="summary,description", help="Comma-separated text columns to merge & clean")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print(f"[NLP] Loading: {args.input}")
    df = load_jsonl(args.input)

    # Ensure key cols exist
    for col in ["id","summary","creator","assigned_to","status","resolution","creation_time","last_change_time"]:
        if col not in df.columns:
            df[col] = None

    ensure_nltk()
    sw = build_stopwords()

    text_cols = [c.strip() for c in args.text_cols.split(",") if c.strip()]
    if not text_cols:
        text_cols = ["summary"]

    clean_texts = []
    for _, row in df.iterrows():
        chunks = []
        for c in text_cols:
            if c in df.columns and isinstance(row.get(c), str) and row.get(c):
                chunks.append(row.get(c))
        if not chunks and isinstance(row.get("summary"), str):
            chunks = [row.get("summary")]
        raw = " ".join(chunks)
        clean_texts.append(clean_text(raw, sw))

    out = df.copy()
    out["clean_text"] = clean_texts

    # Save a compact modeling table
    cols = ["id", "clean_text", "summary", "creator", "assigned_to", "status", "resolution", "creation_time", "last_change_time"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out[cols].to_csv(os.path.join(args.outdir, "bugs_clean.csv"), index=False)
    print(f"[NLP] Wrote {os.path.join(args.outdir,'bugs_clean.csv')}")

if __name__ == "__main__":
    main()
