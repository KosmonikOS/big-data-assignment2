import sys
import re


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 2)
    # Line is corrupted, skip it
    if len(parts) < 3:
        continue

    doc_id, title, text = parts[0], parts[1], parts[2]

    # Extract only alphanumeric tokens
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    length = len(tokens)

    # Per-document length record
    print(f"{doc_id}\t{length}")
    # For the global average calculation in the reducer
    print(f"__GLOBAL__\t{length}")
