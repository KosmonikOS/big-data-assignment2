import sys


current_term = None

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 2:
        continue

    term, df = parts[0], parts[1]

    # Only emit the first line for each term (all have the same df)
    if term != current_term:
        print(f"{term}\t{df}")
        current_term = term
