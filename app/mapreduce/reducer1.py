import sys


def flush_term(term: str, doc_counts: dict[str, int]) -> None:
    """Emit one index line per (term, doc_id) pair, including document frequency.

    Args:
        term: The term to flush.
        doc_counts: A dictionary of document ids and their accumulated counts for the current term.
    """
    df = len(doc_counts)
    for doc_id, tf in doc_counts.items():
        print(f"{term}\t{doc_id}\t{tf}\t{df}")


current_term = None
doc_counts: dict[str, int] = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    # Line is corrupted, skip it
    if len(parts) < 3:
        continue

    term, doc_id, count = parts[0], parts[1], int(parts[2])

    if term != current_term:
        # New term encountered — flush the previous term's data
        if current_term is not None:
            flush_term(current_term, doc_counts)
        current_term = term
        doc_counts = {}

    doc_counts[doc_id] = doc_counts.get(doc_id, 0) + count

# Flush the final term
if current_term is not None:
    flush_term(current_term, doc_counts)
