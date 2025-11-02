### EASY FIX COLLECT DATA

## json (Drive)
dikarenakan json beberapa repository memiliki file yang besar tidak dapat di push silahkan akses disini [drive](https://drive.google.com/drive/folders/1PjER9uZCddyRLEw0mM-5joLgpwEcprV9?usp=sharing)

## run
`python bug_collect_{provide}`
## provide
- Bugzilla
- Github
- Redmine

## addition by DARA
# EasyFix Bugzilla → NLP → LDA (Separated)

1) **01_nlp_preprocess.py**  
   - Input: `datasource/bugs.jsonl` (Bugzilla export; one JSON per line)  
   - Output: `out_nlp/bugs_clean.csv` with columns:
     - `id, clean_text, summary, creator, assigned_to, status, resolution, creation_time, last_change_time`

2) **02_lda_topics.py**  
   - Input: `out_nlp/bugs_clean.csv`  
   - Output (in `out_lda/`):
     - `topics.csv` (top terms per topic)
     - `bugs_with_topics.csv` (dominant topic/score per bug)
     - `bug_relations.csv` (edges: similar/duplicate via cosine on topic vectors)
     - `developer_topic_profile.csv` (average topic distribution per developer)
     - `dictionary.gensim`, `lda_model.gensim`

## Setup
```bash
python -m venv easyfix_env
source easyfix_env/bin/activate  
pip install -r requirements.txt
```

## Run  main.py
this code will run 01_nlp_preprocess.py, 02_lda_topics.py and 03_clean_topics.py
we'll get follwoing data:
- out_nlp/bugs_cleaned.csv
- out_lda/bug_relations.csv
- out_lda/bugs_with_labels.csv
- out_lda/bugs_with_topics.csv
- out_lda/developer_topic_profile.csv
- out_lda/topics_cleaned.csv
- out_lda/topics.csv
- out_lda/lda_sklearn_model_meta.npz

### Notes
- The NLP cleaner supports English + Indonesian stopwords and removes generic bug words (e.g., "error", "bug", "firefox") to prevent topic domination.
- Tune thresholds in `02_lda_topics.py`:
  - num-topics = 8
  - `--sim_threshold` (default 0.60) for **similar** edges.
  - `--dup_threshold` (default 0.80) for **duplicate** edges.
- You can add a `description` column to the JSONL; `01_nlp_preprocess.py` will automatically concatenate it with `summary`.
