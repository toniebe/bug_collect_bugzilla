#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_lda_topics.py  (scikit-learn, memory-safe relations)
- Read CSV from 01_nlp_preprocess.py (bugs_clean.csv)
- Train LDA (sklearn.decomposition.LatentDirichletAllocation)
- Export topics, per-bug topic assignment, bug relations (similar/duplicate) via sparse radius neighbors
- Export developer-topic profile
"""

import os, argparse, warnings
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.model_selection import train_test_split
from sklearn.neighbors import NearestNeighbors

warnings.filterwarnings("ignore", category=FutureWarning)

DEFAULT_SIM_THRESHOLD = 0.60
DEFAULT_DUP_THRESHOLD = 0.80

# ---------------------------- Core training ---------------------------- #

def _build_vectorizer():
    # clean_text sudah dipreproses; tokenisasi per kata
    return CountVectorizer(
        max_df=0.5,
        min_df=3,
        token_pattern=r"(?u)\b\w+\b"
    )

def _fit_lda(X, n_components=10, max_iter=12, random_state=42):
    lda = LatentDirichletAllocation(
        n_components=n_components,
        max_iter=max_iter,
        learning_method="batch",
        random_state=random_state,
        evaluate_every=-1,  # no internal eval
    )
    lda.fit(X)
    return lda

def _choose_k_auto(X, base_k=10, max_iter=12, random_state=42):
    """
    Pilih K dengan perplexity pada hold-out set (lebih rendah lebih baik).
    Kandidat: [max(3, base_k-4) .. base_k+4]
    """
    X_train, X_val = train_test_split(X, test_size=0.2, random_state=random_state, shuffle=True)
    ks = list(range(max(3, base_k - 4), base_k + 5))
    best_k, best_ppx, best_model = None, float("inf"), None
    for k in ks:
        lda = _fit_lda(X_train, n_components=k, max_iter=max_iter, random_state=random_state)
        # sklearn: higher score => better; perplexity = exp(-score / words)
        total_words = X_val.sum()
        ppx = np.exp(-lda.score(X_val) / total_words) if total_words > 0 else np.inf
        if ppx < best_ppx:
            best_k, best_ppx, best_model = k, ppx, lda
    return best_model, best_k

def train_lda_sklearn(texts, num_topics=10, passes=12, auto_k=False, random_state=42):
    """
    Returns:
      lda_model: fitted LatentDirichletAllocation
      vocab: np.array of str
      doc_topic: np.ndarray (n_docs, n_topics)
      chosen_k: int
    """
    vectorizer = _build_vectorizer()
    X = vectorizer.fit_transform(texts)

    if auto_k:
        lda_model, chosen_k = _choose_k_auto(X, base_k=num_topics, max_iter=passes, random_state=random_state)
    else:
        lda_model = _fit_lda(X, n_components=num_topics, max_iter=passes, random_state=random_state)
        chosen_k = num_topics

    doc_topic = lda_model.transform(X).astype(np.float32)
    vocab = vectorizer.get_feature_names_out()
    return lda_model, vocab, doc_topic, chosen_k

# ---------------------------- Exports ---------------------------- #

def export_topics_sklearn(lda_model, vocab, outdir, topn=12):
    rows = []
    comps = lda_model.components_  # (n_topics, n_terms)
    for k, topic_vec in enumerate(comps):
        top_idx = topic_vec.argsort()[:-topn-1:-1]
        terms = [str(vocab[i]) for i in top_idx]
        rows.append({"topic_id": k, "terms": ", ".join(terms)})
    pd.DataFrame(rows).to_csv(os.path.join(outdir, "topics.csv"), index=False)

def export_bug_table(df, topic_mat, outdir):
    dom_topic = topic_mat.argmax(axis=1)
    dom_score = topic_mat.max(axis=1)
    out = df.copy()
    out["dominant_topic"] = dom_topic
    out["topic_score"] = np.round(dom_score, 4)
    out.to_csv(os.path.join(outdir, "bugs_with_topics.csv"), index=False)

def export_developer_topics(df, topic_mat, outdir):
    if "assigned_to" not in df.columns:
        return
    tmp = df[["assigned_to"]].copy()
    for k in range(topic_mat.shape[1]):
        tmp[f"t{k}"] = topic_mat[:, k]
    dev_group = tmp.groupby("assigned_to").mean(numeric_only=True).reset_index()
    topic_cols = [c for c in dev_group.columns if c.startswith("t")]
    dev_group["dominant_topic"] = dev_group[topic_cols].values.argmax(axis=1)
    dev_group.to_csv(os.path.join(outdir, "developer_topic_profile.csv"), index=False)

def export_relations_sparse(df, topic_mat, sim_th, dup_th, outdir, chunk_flush=100_000):
    """
    Memory-safe edge generation:
    - Finds neighbors with cosine distance <= 1 - sim_th
    - Streams edges to CSV in chunks
    """
    topic_mat = np.asarray(topic_mat, dtype=np.float32)
    radius = 1.0 - float(sim_th)  # cosine distance radius
    nbrs = NearestNeighbors(metric="cosine", radius=radius, algorithm="brute", n_jobs=-1)
    nbrs.fit(topic_mat)
    G = nbrs.radius_neighbors_graph(topic_mat, mode="distance").tocsr()  # sparse CSR
    # Convert distances -> similarities
    G.data = 1.0 - G.data

    ids = df["id"].to_numpy() if "id" in df.columns else np.arange(len(df))
    out_path = os.path.join(outdir, "bug_relations.csv")
    # header
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("bug_a,bug_b,similarity,relation,method\n")

    rows, cols = G.nonzero()
    data = G.data
    buf = []
    for i, j, s in zip(rows, cols, data):
        if j <= i:  # keep upper triangle only (no dup/self-loop)
            continue
        relation = "duplicate" if s >= dup_th else "similar"
        buf.append(f"{int(ids[i])},{int(ids[j])},{s:.4f},{relation},sklearn_lda_radius")
        if len(buf) >= chunk_flush:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write("\n".join(buf) + "\n")
            buf.clear()
    if buf:
        with open(out_path, "a", encoding="utf-8") as f:
            f.write("\n".join(buf) + "\n")

    print(f"[LDA] Saved relations to {out_path}")

# ---------------------------- CLI ---------------------------- #

def main():
    parser = argparse.ArgumentParser(description="LDA topic modeling for EasyFix (scikit-learn, memory-safe)")
    parser.add_argument("--input", type=str, default="out_nlp/bugs_clean.csv", help="CSV from 01_nlp_preprocess.py")
    parser.add_argument("--outdir", type=str, default="out_lda", help="Output directory")
    parser.add_argument("--num_topics", type=int, default=10, help="Number of topics (ignored if --auto_k)")
    parser.add_argument("--passes", type=int, default=12, help="Max iterations for LDA")
    parser.add_argument("--auto_k", action="store_true", help="Select K via hold-out perplexity")
    parser.add_argument("--topn_terms", type=int, default=12, help="Top-N terms per topic")
    parser.add_argument("--sim_threshold", type=float, default=DEFAULT_SIM_THRESHOLD, help="Similarity threshold for 'similar'")
    parser.add_argument("--dup_threshold", type=float, default=DEFAULT_DUP_THRESHOLD, help="Similarity threshold for 'duplicate'")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print(f"[LDA] Loading: {args.input}")
    df = pd.read_csv(args.input)
    if "clean_text" not in df.columns:
        raise ValueError("Input CSV must contain 'clean_text'. Run 01_nlp_preprocess.py first.")
    texts = df["clean_text"].fillna("").astype(str).tolist()

    print("[LDA] Training (scikit-learn)…")
    lda_model, vocab, topic_mat, chosen_k = train_lda_sklearn(
        texts,
        num_topics=args.num_topics,
        passes=args.passes,
        auto_k=args.auto_k,
        random_state=42
    )
    print(f"[LDA] chosen num_topics = {chosen_k}")

    print("[LDA] Exporting artifacts…")
    export_topics_sklearn(lda_model, vocab, args.outdir, topn=args.topn_terms)
    export_bug_table(df, topic_mat, args.outdir)
    export_relations_sparse(df, topic_mat, args.sim_threshold, args.dup_threshold, args.outdir)
    export_developer_topics(df, topic_mat, args.outdir)

    # optional: save minimal model meta
    np.savez(os.path.join(args.outdir, "lda_sklearn_model_meta.npz"),
             components=lda_model.components_,
             vocab=vocab,
             doc_topic=topic_mat)

    print("[LDA] Done. Outputs in", args.outdir)

if __name__ == "__main__":
    main()
