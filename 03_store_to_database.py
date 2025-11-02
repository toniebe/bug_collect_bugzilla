#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
03_store_to_database.py
- Store hasil LDA ke Neo4j
- Skip kalau data sudah ada di Neo4j
- Robust baca CSV (bug-commit & commit-commit) kalau ada koma di tengah
"""

import os, sys, argparse, importlib.util, csv
import datetime
import pandas as pd


# ---------- helper ambil log dari main.py ----------
def get_main_module():
    here = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.join(here, "main.py")
    if not os.path.exists(main_path):
        return None
    spec = importlib.util.spec_from_file_location("main_module", main_path)
    main_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(main_mod)
    return main_mod


# ---------- koneksi ----------
def neo4j_connect(uri: str, user: str, password: str, db_name: str | None = None):
    try:
        from neo4j import GraphDatabase
    except ImportError as e:
        raise RuntimeError(f"neo4j driver not installed: {e}")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        if db_name:
            with driver.session(database=db_name) as session:
                session.run("RETURN 1 AS ok")
        else:
            with driver.session() as session:
                session.run("RETURN 1 AS ok")
    except Exception as e:
        raise RuntimeError(f"cannot connect to neo4j at {uri} as {user}: {e}")
    return driver


# ---------- util ----------
def _to_int_or_str(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    return s


# ---------- cek relasi TANPA warning ----------
def neo4j_has_bug_bug(session) -> bool:
    # cek dulu apakah tipe relasinya ada
    q = """
    CALL db.relationshipTypes() YIELD relationshipType
    WITH collect(relationshipType) AS rels
    RETURN any(r IN ['SIMILAR_TO','DUPLICATE_OF','DEPENDS_ON'] WHERE r IN rels) AS exists
    """
    exists = session.run(q).single()["exists"]
    if not exists:
        return False
    q2 = """
    MATCH ()-[r:SIMILAR_TO|DUPLICATE_OF|DEPENDS_ON]->()
    RETURN count(r) AS c
    """
    c = session.run(q2).single()["c"]
    return bool(c and c > 0)


def neo4j_has_bug_developer(session) -> bool:
    q = """
    CALL db.relationshipTypes() YIELD relationshipType
    WITH collect(relationshipType) AS rels
    RETURN any(r IN ['CREATED_BY','ASSIGNED_TO'] WHERE r IN rels) AS exists
    """
    exists = session.run(q).single()["exists"]
    if not exists:
        return False
    q2 = """
    MATCH (:Bug)-[r:CREATED_BY|ASSIGNED_TO]->(:Developer)
    RETURN count(r) AS c
    """
    c = session.run(q2).single()["c"]
    return bool(c and c > 0)


def neo4j_has_bug_commit(session) -> bool:
    q = """
    CALL db.relationshipTypes() YIELD relationshipType
    WITH collect(relationshipType) AS rels
    RETURN 'RELATED_COMMIT' IN rels AS exists
    """
    exists = session.run(q).single()["exists"]
    if not exists:
        return False
    q2 = """
    MATCH (:Bug)-[r:RELATED_COMMIT]->(:Commit)
    RETURN count(r) AS c
    """
    c = session.run(q2).single()["c"]
    return bool(c and c > 0)


def neo4j_has_commit_commit(session) -> bool:
    q = """
    CALL db.relationshipTypes() YIELD relationshipType
    WITH collect(relationshipType) AS rels
    RETURN 'CO_OCCURS' IN rels AS exists
    """
    exists = session.run(q).single()["exists"]
    if not exists:
        return False
    q2 = """
    MATCH (:Commit)-[r:CO_OCCURS]->(:Commit)
    RETURN count(r) AS c
    """
    c = session.run(q2).single()["c"]
    return bool(c and c > 0)


# ---------- reader longgar ----------
def read_bug_commit_csv_loose(path: str):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # boleh diabaikan
        for row in reader:
            if not row:
                continue
            if len(row) < 3:
                continue
            bug_id = row[0].strip()
            commit_id = row[1].strip()
            source = row[2].strip()
            # gabung sisanya jadi raw_value
            raw_value = ",".join(row[3:]).strip() if len(row) > 3 else ""
            rows.append({
                "bug_id": bug_id,
                "commit_id": commit_id,
                "source": source,
                "raw_value": raw_value,
            })
    return rows


def read_commit_commit_csv_loose(path: str):
    """
    Baca commit_commit_relations.csv yang bisa punya bentuk:
    1) c1,c2,co_occurs,1.0,bug_row      (normal, dari 02)
    2) c1,c2,1.0,co_occurs,bug_row      (score & relation ketukar)
    3) c1,c2,co_occurs                  (tanpa score & source)
    4) c1,c2,1.0                        (tanpa relation, kita isi default)
    5) c1,c2,co_occurs,1.0,bug,with,comma (source banyak koma)
    plus: c1 / c2 bisa mengandung koma → kita cuma ambil 2 kolom pertama apa adanya
    """
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # boleh ada / boleh tidak
        for line in reader:
            if not line:
                continue
            # minimal harus ada c1,c2
            if len(line) < 2:
                continue

            c1 = line[0].strip()
            c2 = line[1].strip()
            rest = line[2:]  # sisanya yang akan kita tebak

            relation = None
            score = None
            source_parts = []

            for item in rest:
                item = item.strip()
                if not item:
                    continue
                # coba float
                try:
                    val = float(item)
                    # kalau kita belum punya score → ini score
                    if score is None:
                        score = val
                        continue
                    else:
                        # kalau sudah ada score, ini kita taruh ke source
                        source_parts.append(item)
                        continue
                except ValueError:
                    # bukan angka
                    if relation is None and item.lower() in ("co_occurs", "co-occurs", "co", "cooccur", "cooccurred"):
                        relation = "co_occurs"
                    elif relation is None and item.isupper():
                        # kadang dari tool lain
                        relation = item
                    else:
                        source_parts.append(item)

            # isi default
            if relation is None:
                relation = "co_occurs"
            if score is None:
                score = 1.0
            source = ",".join(source_parts).strip() if source_parts else "bug_row"

            rows.append({
                "c1": c1,
                "c2": c2,
                "relation": relation,
                "score": float(score),
                "source": source,
            })
    return rows

# ---------- importers (batched) ----------
def import_bug_bug(session, path, log_write, log_fh, batch_size=1000):
    log_write(log_fh, f"[NEO4J] importing bug-bug from {path}")
    total = 0
    for chunk in pd.read_csv(path, chunksize=batch_size):
        rows = []
        for _, r in chunk.iterrows():
            src = _to_int_or_str(r["bug_id_source"])
            tgt = _to_int_or_str(r["bug_id_target"])
            if src is None or tgt is None:
                continue
            rows.append({
                "s": src,
                "t": tgt,
                "relation": r["relation"],
                "score": float(r["score"]),
                "source": r.get("source", "lda"),
            })
        if not rows:
            continue

        cypher = """
        UNWIND $rows AS row
        MERGE (s:Bug {bug_id: row.s})
        MERGE (t:Bug {bug_id: row.t})
        FOREACH (_ IN CASE WHEN row.relation = 'similar' THEN [1] ELSE [] END |
            MERGE (s)-[r:SIMILAR_TO]->(t)
            SET r.score = row.score, r.source = row.source
        )
        FOREACH (_ IN CASE WHEN row.relation = 'duplicate' THEN [1] ELSE [] END |
            MERGE (s)-[r:DUPLICATE_OF]->(t)
            SET r.score = row.score, r.source = row.source
        )
        FOREACH (_ IN CASE WHEN row.relation = 'depends_on' THEN [1] ELSE [] END |
            MERGE (s)-[r:DEPENDS_ON]->(t)
            SET r.score = row.score, r.source = row.source
        )
        """
        session.run(cypher, rows=rows)
        total += len(rows)
        log_write(log_fh, f"[NEO4J] bug-bug progress: {total}")
    log_write(log_fh, f"[NEO4J] bug-bug imported total={total}")


def import_bug_developer(session, path, log_write, log_fh, batch_size=1000):
    log_write(log_fh, f"[NEO4J] importing bug-developer from {path}")
    total = 0
    for chunk in pd.read_csv(path, chunksize=batch_size):
        rows = []
        for _, r in chunk.iterrows():
            bug_id = _to_int_or_str(r["bug_id"])
            if bug_id is None:
                continue
            rows.append({
                "bug_id": bug_id,
                "dev_id": r["developer_id"],
                "role": r["role"],
                "source": r.get("source", "bug_fields"),
            })
        if not rows:
            continue

        cypher = """
        UNWIND $rows AS row
        MERGE (b:Bug {bug_id: row.bug_id})
        MERGE (d:Developer {dev_id: row.dev_id})
        FOREACH (_ IN CASE WHEN row.role = 'creator' THEN [1] ELSE [] END |
            MERGE (b)-[r:CREATED_BY]->(d)
            SET r.source = row.source
        )
        FOREACH (_ IN CASE WHEN row.role = 'assigned_to' THEN [1] ELSE [] END |
            MERGE (b)-[r:ASSIGNED_TO]->(d)
            SET r.source = row.source
        )
        FOREACH (_ IN CASE WHEN row.role <> 'creator' AND row.role <> 'assigned_to' THEN [1] ELSE [] END |
            MERGE (b)-[r:RELATED_TO]->(d)
            SET r.source = row.source
        )
        """
        session.run(cypher, rows=rows)
        total += len(rows)
        log_write(log_fh, f"[NEO4J] bug-developer progress: {total}")
    log_write(log_fh, f"[NEO4J] bug-developer imported total={total}")


def import_bug_commit(session, path, log_write, log_fh, batch_size=1000):
    """
    SELALU pakai reader longgar karena file ini sering punya koma di raw_value.
    Format ideal: bug_id,commit_id,source,raw_value
    Tapi kalau ada koma di raw_value → kolom 4+ kita gabung.
    """
    log_write(log_fh, f"[NEO4J] importing bug-commit from {path} (loose parser)")
    rows = read_bug_commit_csv_loose(path)   # <--- SELALU pakai ini
    total = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        cypher = """
        UNWIND $rows AS row
        MERGE (b:Bug {bug_id: row.bug_id})
        MERGE (c:Commit {commit_id: row.commit_id})
        MERGE (b)-[r:RELATED_COMMIT]->(c)
        SET r.source = row.source, r.raw = row.raw_value
        """
        session.run(cypher, rows=batch)
        total += len(batch)
        log_write(log_fh, f"[NEO4J] bug-commit progress: {total}")

    log_write(log_fh, f"[NEO4J] bug-commit imported total={total}")


def import_commit_commit(session, path, log_write, log_fh, batch_size=1000):
    log_write(log_fh, f"[NEO4J] importing commit-commit from {path} (loose parser)")
    rows = read_commit_commit_csv_loose(path)
    total = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        cypher = """
        UNWIND $rows AS row
        MERGE (c1:Commit {commit_id: row.c1})
        MERGE (c2:Commit {commit_id: row.c2})
        MERGE (c1)-[r:CO_OCCURS]->(c2)
        SET r.score = row.score, r.source = row.source, r.relation = row.relation
        """
        session.run(cypher, rows=batch)
        total += len(batch)
        log_write(log_fh, f"[NEO4J] commit-commit progress: {total}")

    log_write(log_fh, f"[NEO4J] commit-commit imported total={total}")


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Store LDA relations to Neo4j (robust)")
    parser.add_argument("--in_lda", type=str, default=os.getenv("PATH_LDA_OUT", "out_lda"))
    parser.add_argument("--neo4j-uri", type=str, default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", type=str, default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-pass", type=str, default=os.getenv("NEO4J_PASS", "password"))
    parser.add_argument("--neo4j-db", type=str, default=None)
    parser.add_argument("--log_path", type=str, default=None)
    args = parser.parse_args()

    main_mod = get_main_module()
    log_fh = None
    log_write = print
    if main_mod and hasattr(main_mod, "log_write"):
        log_write = main_mod.log_write

    if args.log_path:
        try:
            log_fh = open(args.log_path, "a", encoding="utf-8")
        except Exception:
            log_fh = None

    db_name = args.neo4j_db or os.getenv("NEO4J_DB") or "neo4j"
    log_write(log_fh, "[NEO4J] === Store to database started ===")
    log_write(log_fh, f"[NEO4J] using database: {db_name}")

    if not os.path.isdir(args.in_lda):
        log_write(log_fh, "[NEO4J][ERROR] LDA output directory not found")
        sys.exit(1)

    # connect
    try:
        driver = neo4j_connect(args.neo4j_uri, args.neo4j_user, args.neo4j_pass, db_name=db_name)
    except RuntimeError as e:
        log_write(log_fh, f"[NEO4J][ERROR] {e}")
        sys.exit(1)

    # constraints
    with driver.session(database=db_name) as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:Bug) REQUIRE b.bug_id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Developer) REQUIRE d.dev_id IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Commit) REQUIRE c.commit_id IS UNIQUE")
    log_write(log_fh, "[NEO4J] constraints ensured")

    # imports
    with driver.session(database=db_name) as session:
        # 1) bug-bug
        p = os.path.join(args.in_lda, "bug_bug_relations.csv")
        if neo4j_has_bug_bug(session):
            log_write(log_fh, "[NEO4J] bug-bug relations already exist — skip.")
        elif os.path.exists(p):
            import_bug_bug(session, p, log_write, log_fh)
        else:
            log_write(log_fh, "[NEO4J] bug_bug_relations.csv not found — skip.")

        # 2) bug-developer
        p = os.path.join(args.in_lda, "bug_developer_relations.csv")
        if neo4j_has_bug_developer(session):
            log_write(log_fh, "[NEO4J] bug-developer relations already exist — skip.")
        elif os.path.exists(p):
            import_bug_developer(session, p, log_write, log_fh)
        else:
            log_write(log_fh, "[NEO4J] bug_developer_relations.csv not found — skip.")

        # 3) bug-commit
        p = os.path.join(args.in_lda, "bug_commit_relations.csv")
        if neo4j_has_bug_commit(session):
            log_write(log_fh, "[NEO4J] bug-commit relations already exist — skip.")
        elif os.path.exists(p):
            import_bug_commit(session, p, log_write, log_fh)
        else:
            log_write(log_fh, "[NEO4J] bug_commit_relations.csv not found — skip.")

        # 4) commit-commit
        p = os.path.join(args.in_lda, "commit_commit_relations.csv")
        if neo4j_has_commit_commit(session):
            log_write(log_fh, "[NEO4J] commit-commit relations already exist — skip.")
        elif os.path.exists(p):
            import_commit_commit(session, p, log_write, log_fh)
        else:
            log_write(log_fh, "[NEO4J] commit_commit_relations.csv not found — skip.")

    driver.close()
    log_write(log_fh, "[NEO4J] === Store to database finished ===")

    if args.log_path and log_fh:
        try:
            log_fh.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
