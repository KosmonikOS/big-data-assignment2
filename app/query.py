import math
import re
import sys

from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"
K1 = 1.0
B = 0.75
TOP_N = 10


def tokenize(text: str) -> list[str]:
    """Lowercase and extract alphanumeric tokens

    Args:
        text: The text to tokenize.

    Returns:
        A list of alphanumeric tokens.
    """
    return re.findall(r"[a-z0-9]+", text.lower())


def fetch_index_data(
    query_terms: list[str],
) -> tuple[int, float, list[tuple[str, str, int, int]], dict[str, int], dict[str, str]]:
    """Fetch all data needed for BM25.

    Returns:
        Tuple containing:
        - total document count
        - average document length in tokens
        - list of (term, doc_id, tf, df) tuples for every query term
        - {doc_id: token_count} for docs appearing in postings
        - {doc_id: title} for docs appearing in postings
    """
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    # 1. Corpus-level constants
    row = session.execute(
        "SELECT num_docs, avg_doc_len FROM corpus_stats WHERE id = 'global'"
    ).one()
    if row is None:
        cluster.shutdown()
        raise RuntimeError("corpus_stats table is empty — run index.sh first.")
    num_docs = int(row.num_docs)
    avg_doc_len = float(row.avg_doc_len)

    # 2. Postings for every query term from the inverted index
    postings: list[tuple[str, str, int, int]] = []
    for term in query_terms:
        rows = session.execute(
            "SELECT term, doc_id, tf, df FROM inverted_index WHERE term = %s",
            [term],
        )
        for r in rows:
            postings.append((r.term, r.doc_id, int(r.tf), int(r.df)))

    # 3. Per-document lengths and titles for every doc that matched
    matched_doc_ids = list({p[1] for p in postings})
    doc_lengths: dict[str, int] = {}
    doc_titles: dict[str, str] = {}

    for doc_id in matched_doc_ids:
        r = session.execute(
            "SELECT length, title FROM doc_stats WHERE doc_id = %s", [doc_id]
        ).one()
        if r:
            doc_lengths[doc_id] = int(r.length)
            doc_titles[doc_id] = r.title or doc_id

    cluster.shutdown()
    return num_docs, avg_doc_len, postings, doc_lengths, doc_titles


def compute_bm25(
    sc: SparkContext,
    postings: list[tuple[str, str, int, int]],
    num_docs: int,
    avg_doc_len: float,
    doc_lengths: dict[str, int],
    top_n: int = TOP_N,
) -> list[tuple[str, float]]:
    """Return top-10 (doc_id, bm25_score) pairs using PySpark RDD API.

    Args:
        sc: active SparkContext
        postings: list of (term, doc_id, tf, df)
        num_docs: N
        avg_doc_len: dlavg
        doc_lengths: {doc_id: length}

    Returns:
        List of up to top_n (doc_id, score) tuples sorted descending by score.
    """
    bc_N = sc.broadcast(num_docs)
    bc_dlavg = sc.broadcast(avg_doc_len)
    bc_dl = sc.broadcast(doc_lengths)

    postings_rdd = sc.parallelize(postings)

    def bm25_score(record: tuple) -> tuple[str, float]:
        _, doc_id, tf, df = record
        N_val = bc_N.value
        dlavg_val = bc_dlavg.value
        dl = bc_dl.value.get(doc_id, dlavg_val)

        idf = math.log(N_val / df) if df > 0 else 0.0
        numerator = (K1 + 1) * tf
        denominator = K1 * ((1 - B) + B * dl / dlavg_val) + tf
        score = idf * numerator / denominator if denominator > 0 else 0.0
        return (doc_id, score)

    # Calculate BM25 score for each document and reduce by key to get the total score
    scores_rdd = postings_rdd.map(bm25_score).reduceByKey(lambda a, b: a + b)

    return scores_rdd.top(top_n, key=lambda x: x[1])


def main() -> None:
    # Read query: prefer first CLI argument, fall back to stdin
    if len(sys.argv) > 1:
        query_text = " ".join(sys.argv[1:])
    else:
        query_text = sys.stdin.readline().strip()

    query_terms = list(set(tokenize(query_text)))

    if not query_terms:
        print("No valid query terms found. Please enter query with at least one word.")
        sys.exit(0)

    num_docs, avg_doc_len, postings, doc_lengths, doc_titles = fetch_index_data(
        query_terms
    )

    if not postings:
        print("No matching documents found for the given query.")
        sys.exit(0)

    conf = SparkConf().setAppName("BM25Search")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    try:
        top_n = compute_bm25(sc, postings, num_docs, avg_doc_len, doc_lengths, TOP_N)
    finally:
        sc.stop()

    print(f"\nTop {len(top_n)} relevant documents:")
    for rank, (doc_id, score) in enumerate(top_n, 1):
        title = doc_titles.get(doc_id, doc_id)
        print(f"{rank:2}. [{doc_id}] {title}  (score={score:.4f})")


if __name__ == "__main__":
    main()
