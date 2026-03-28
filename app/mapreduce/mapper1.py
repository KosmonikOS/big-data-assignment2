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

    # Lowercase and extract only alphanumeric tokens
    tokens = re.findall(r"[a-z0-9]+", text.lower())

    for token in tokens:
        # Key is term, so Hadoop groups all occurrences of the same term together.
        # Value is doc_id so the reducer knows which doc this came from.
        print(f"{token}\t{doc_id}\t1")
