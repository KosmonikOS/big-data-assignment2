import sys


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    # Line is corrupted, skip it
    if len(parts) < 4:
        continue

    term, df = parts[0], parts[3]
    print(f"{term}\t{df}")
