import sys


total_len = 0
total_docs = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 2:
        continue

    key, value = parts[0], int(parts[1])

    if key == "__GLOBAL__":
        # Accumulate corpus totals;
        total_len += value
        total_docs += 1
    else:
        # Each doc_id appears exactly once from the mapper
        print(f"{key}\t{value}")

# Emit global corpus statistics after processing all input
if total_docs > 0:
    dlavg = total_len / total_docs
    print(f"__GLOBAL__\t{total_docs}\t{dlavg:.4f}")
