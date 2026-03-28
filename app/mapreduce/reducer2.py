import sys


total_len = 0
total_docs = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    # Line could contain 2 (__GLOBAL__) or 3 (doc_id\ttitle\tlength) parts
    # Otherwise, it's corrupted, skip it
    if len(parts) < 2:
        continue

    if parts[0] == "__GLOBAL__":
        # Accumulate corpus totals
        total_len += int(parts[1])
        total_docs += 1
    elif len(parts) == 3:
        # doc_id\ttitle\tlength — pass title through
        title, length = parts[1], int(parts[2])
        print(f"{parts[0]}\t{title}\t{length}")

# Emit global corpus statistics after processing all input
if total_docs > 0:
    dlavg = total_len / total_docs
    print(f"__GLOBAL__\t{total_docs}\t{dlavg:.4f}")
