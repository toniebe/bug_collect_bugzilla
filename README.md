### EASY FIX COLLECT DATA

## json (Drive)
dikarenakan json beberapa repository memiliki file yang besar tidak dapat di push silahkan akses disini [drive](https://drive.google.com/drive/folders/1PjER9uZCddyRLEw0mM-5joLgpwEcprV9?usp=sharing)

## run
`python bug_collect_{provide}`
## provide
- Bugzilla
- Github
- Redmine

#### addition by DARA

### EasyFix Bugzilla → NLP → LDA → Neo4j

EasyFix merupakan pipeline AI yang mengubah laporan bug mentah menjadi Knowledge Graph berbasis AI,
melalui proses NLP, topic modeling (LDA), dan penyimpanan relasi ke Neo4j.

````
Bugzilla JSONL 
→ [01_nlp_preprocess.py] → Clean CSV
→ [02_lda_topics.py] → Topic Modeling & Relation Extraction
→ [03_store_to_database.py] → Neo4j Graph Storage
````

Semua konfigurasi diatur menggunakan file .env.


#### 01_nlp_preprocess.py — Text Cleaning & Normalization

- Input
  
  datasource/bugs.jsonl — Bugzilla export (1 JSON per baris)

- Output
  
  out_nlp/bugs_clean.csv dengan kolom:
  `id, clean_text, summary, creator, assigned_to, status, resolution,creation_time, last_change_time`

- Proses
  - Membersihkan teks (lowercase, hapus URL, hash, simbol)
  - Hilangkan stopwords (English + Indonesian + kata teknis bug)
  - Gabungkan kolom teks penting (summary, component, commit_messages, dsb.)
  - Simpan metadata bug untuk tahap modeling

#### 02_lda_topics.py — Topic Modeling & Relation Extraction
- Input : out_nlp/bugs_clean.csv
- Output (out_lda/)
- File	Deskripsi
  - `topics.csv`,	Daftar top terms per topic
  - `bugs_with_topics.csv`,	Topic dominan & skor tiap bug
  - `bug_bug_relations.csv`,	Relasi bug (similar / duplicate / depends_on)
  - `bug_developer_relations.csv`,Relasi bug–developer (creator / assignee)
  - `bug_commit_relations.csv`,	Relasi bug–commit (commit messages, files, refs)
  - `commit_commit_relations.csv`,	Relasi antar commit (co-occurrence)
  - `lda_sklearn_model_meta.npz`,	Model LDA tersimpan (vocab, komponen, doc-topic)
- Fungsi Utama
  - Vectorisasi teks menggunakan CountVectorizer
  - Latih model LDA (Latent Dirichlet Allocation)
  - Pilih jumlah topik otomatis (AUTO_K) atau sesuai .env
  - Hitung kemiripan bug via cosine similarity
- Ekstrak relasi antar entitas:
  - Bug ↔ Bug
  - Bug ↔ Developer
  - Bug ↔ Commit
  - Commit ↔ Commit

#### 03_store_to_database.py — Store Relations to Neo4j
- Input : Semua file hasil LDA (out_lda/*.csv)
- Fungsi
  - Menyambung ke Neo4j Database
  - Membuat constraints unik (Bug, Developer, Commit)
  - Impor data relasi dalam batch
  - Melewati data yang sudah ada (skip duplicate imports)
  - Log aktivitas dengan log_write() dari main.py

- Graph Schema
```
(:Bug)-[:SIMILAR_TO|DUPLICATE_OF|DEPENDS_ON]->(:Bug)
(:Bug)-[:CREATED_BY|ASSIGNED_TO]->(:Developer)
(:Bug)-[:RELATED_COMMIT]->(:Commit)
(:Commit)-[:CO_OCCURS]->(:Commit)
```

- Konfigurasi (via .env)
```
NEO4J_ENABLE=true
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASS=neo4j2025
NEO4J_DB=easyfix
```



### Setup
```bash
python -m venv easyfix_env
source easyfix_env/bin/activate  
pip install -r requirements.txt
```

### Run  main.py
Run the Full Pipeline
```
python main.py
```

main.py akan menjalankan tahapan berikut secara berurutan:
1. `01_nlp_preprocess.py`
2. `02_lda_topics.py`
3. `03_clean_topics.py` 
4. `03_store_to_database.py` (jika NEO4J_ENABLE=true)

## Output Summary
| Folder       | File / Deskripsi                                                                                                                                       |
| :----------- | :----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **out_nlp/** | `bugs_clean.csv` – hasil preprocessing                                                                                                                 |
| **out_lda/** | `topics.csv`, `bugs_with_topics.csv`, `bug_bug_relations.csv`, `bug_commit_relations.csv`, `commit_commit_relations.csv`, `lda_sklearn_model_meta.npz` |
| **logs/**    | `log_YYYY-MM-DD.txt` – log proses dan status pipeline                                                                                                  |

## Notes

NLP Cleaner mendukung English & Indonesian stopwords
serta menghapus kata umum bug seperti error, issue, bug, fix, firefox, mozilla.

Semua konfigurasi dapat diatur dari .env:

`NUM_TOPICS` → jumlah topik (default: 8, rekomendasi: 100)

`SIM_THRESHOLD` → ambang similar (default: 0.60)

`DUP_THRESHOLD` → ambang duplicate (default: 0.80)

File CSV hasil akhir bisa digunakan untuk analisis lanjutan atau divisualisasikan di Neo4j Bloom.
