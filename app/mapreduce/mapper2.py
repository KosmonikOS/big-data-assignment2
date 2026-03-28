from __future__ import annotations

import sys
import re


def tokenize(text: str) -> list[str]:
    """Lowercase and extract alphanumeric tokens

    Args:
        text: The text to tokenize.

    Returns:
        A list of alphanumeric tokens.
    """
    return re.findall(r"[a-z0-9]+", text.lower())


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 2)
    # Line is corrupted, skip it
    if len(parts) < 3:
        continue

    doc_id, title, text = parts[0], parts[1], parts[2]

    tokens = tokenize(text)
    length = len(tokens)

    # Per-document length record (title preserved for Cassandra storage)
    print(f"{doc_id}\t{title}\t{length}")
    # For the global average calculation in the reducer
    print(f"__GLOBAL__\t{length}")
