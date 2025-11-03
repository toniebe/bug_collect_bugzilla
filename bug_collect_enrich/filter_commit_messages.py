# filter_commit_messages.py
import json
import os

IN_PATH  = "datasource/bug_enriched_commit_message.jsonl"
OUT_WITH = "datasource/bug_with_commit_messages.jsonl"   # hanya yang punya commit_messages
OUT_NONE = "datasource/bug_no_commit_messages.jsonl"     # yang commit_messages kosong / tidak ada

def has_commit_messages(obj):
    msgs = obj.get("commit_messages")
    return isinstance(msgs, list) and len([m for m in msgs if str(m).strip()]) > 0

def main():
    os.makedirs(os.path.dirname(OUT_WITH), exist_ok=True)

    cnt_in = cnt_with = cnt_none = 0
    with open(IN_PATH, "r", encoding="utf-8") as fin, \
         open(OUT_WITH, "w", encoding="utf-8") as fwith, \
         open(OUT_NONE, "w", encoding="utf-8") as fnone:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            cnt_in += 1
            if has_commit_messages(obj):
                fwith.write(json.dumps(obj, ensure_ascii=False) + "\n")
                cnt_with += 1
            else:
                fnone.write(json.dumps(obj, ensure_ascii=False) + "\n")
                cnt_none += 1

    print(f"Total in      : {cnt_in}")
    print(f"With commits  : {cnt_with} -> {OUT_WITH}")
    print(f"No commits    : {cnt_none} -> {OUT_NONE}")

if __name__ == "__main__":
    main()
