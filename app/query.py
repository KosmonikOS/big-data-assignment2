from __future__ import annotations

import math
import re
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_engine"
K1 = 1.0
B = 0.75
TOP_N = 10


def tokenize(text: str) -> list[str]:
    """Lowercase and extract alphanumeric tokens.

    Args:
        text: The text to tokenize.

    Returns:
        A list of alphanumeric tokens in lowercase.
    """
    return list(set(re.findall(r"[a-z0-9]+", text.lower())))


def build_spark_session() -> SparkSession:
    """Create a SparkSession pre-configured for the Spark-Cassandra Connector.

    Returns:
        A configured SparkSession with BM25Search as the app name.
    """
    return (
        SparkSession.builder
        .appName("BM25Search")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )


def _cassandra_read(spark: SparkSession, table: str) -> DataFrame:
    """Load an entire Cassandra table as a Spark DataFrame.

    Args:
        spark: Active SparkSession with Cassandra connector configured.
        table: The Cassandra table name within KEYSPACE.

    Returns:
        A DataFrame representing the full table contents.
    """
    return (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .load()
    )


def read_corpus_stats(spark: SparkSession) -> tuple[int, float]:
    """Fetch corpus-level BM25 constants from Cassandra.

    Args:
        spark: Active SparkSession with Cassandra connector configured.

    Returns:
        A tuple of (num_docs, avg_doc_len).

    Raises:
        RuntimeError: If the corpus_stats table is empty.
    """
    rows = (
        _cassandra_read(spark, "corpus_stats")
        .filter(F.col("id") == "global")
        .select("num_docs", "avg_doc_len")
        .collect()
    )
    if not rows:
        raise RuntimeError("corpus_stats table is empty — run index.sh first.")
    return int(rows[0]["num_docs"]), float(rows[0]["avg_doc_len"])


def read_postings(spark: SparkSession, query_terms: list[str]) -> DataFrame:
    """Read inverted index rows matching any of the query terms.

    The Spark-Cassandra Connector pushes the ``isin`` predicate down to
    Cassandra so only matching partitions are scanned.

    Args:
        spark: Active SparkSession with Cassandra connector configured.
        query_terms: Tokenised query terms to look up.

    Returns:
        A DataFrame with columns [term, doc_id, tf, df].
    """
    return (
        _cassandra_read(spark, "inverted_index")
        .filter(F.col("term").isin(query_terms))
        .select("term", "doc_id", "tf", "df")
    )


def read_doc_stats(spark: SparkSession) -> DataFrame:
    """Read per-document length and title from Cassandra.

    Args:
        spark: Active SparkSession with Cassandra connector configured.

    Returns:
        A DataFrame with columns [doc_id, length, title].
    """
    return (
        _cassandra_read(spark, "doc_stats")
        .select("doc_id", "length", "title")
    )


def compute_bm25(
    postings_df: DataFrame,
    doc_stats_df: DataFrame,
    num_docs: int,
    avg_doc_len: float,
    top_n: int = TOP_N,
) -> DataFrame:
    """Compute BM25 scores using the Spark DataFrame API.

    BM25 score per (term, doc) pair:

        idf   = log(N / df)
        score = idf * (K1+1)*tf / (K1 * ((1-B) + B * dl/dlavg) + tf)

    Scores are summed across all query terms per document, then the top-N
    documents by total score are returned.

    Args:
        postings_df: DataFrame with columns [term, doc_id, tf, df].
        doc_stats_df: DataFrame with columns [doc_id, length, title].
        num_docs: Total number of documents in the corpus (N).
        avg_doc_len: Average document length in tokens (dlavg).
        top_n: Maximum number of top-scoring documents to return.

    Returns:
        A DataFrame with columns [doc_id, title, total_score] sorted
        descending by total_score, limited to top_n rows.
    """
    n_lit = F.lit(num_docs).cast("double")
    dlavg_lit = F.lit(avg_doc_len)
    k1_lit = F.lit(K1)
    b_lit = F.lit(B)

    scored = (
        postings_df
        .join(doc_stats_df, on="doc_id", how="inner")
        .withColumn(
            "idf",
            F.log(n_lit / F.col("df").cast("double")),
        )
        .withColumn(
            "norm_tf",
            (k1_lit + F.lit(1.0)) * F.col("tf").cast("double")
            / (
                k1_lit * (
                    (F.lit(1.0) - b_lit)
                    + b_lit * F.col("length").cast("double") / dlavg_lit
                )
                + F.col("tf").cast("double")
            ),
        )
        .withColumn("score", F.col("idf") * F.col("norm_tf"))
    )

    return (
        scored
        .groupBy("doc_id", "title")
        .agg(F.sum("score").alias("total_score"))
        .orderBy(F.desc("total_score"))
        .limit(top_n)
    )


def main() -> None:
    """Parse the query, fetch index data, compute BM25, and print ranked results."""
    if len(sys.argv) > 1:
        query_text = " ".join(sys.argv[1:])
    else:
        query_text = sys.stdin.readline().strip()

    query_terms = list(set(tokenize(query_text)))

    if not query_terms:
        print("No valid query terms found. Please enter query with at least one word.")
        sys.exit(0)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        num_docs, avg_doc_len = read_corpus_stats(spark)
        postings_df = read_postings(spark, query_terms)
        if postings_df.rdd.isEmpty():
            print("No matching documents found for the given query.")
            sys.exit(0)

        doc_stats_df = read_doc_stats(spark)
        results_df = compute_bm25(
            postings_df, doc_stats_df, num_docs, avg_doc_len, TOP_N
        )

        results = results_df.collect()
    finally:
        spark.stop()

    if not results:
        print("No matching documents found for the given query.")
        sys.exit(0)

    print(f"\nTop {len(results)} relevant documents:")
    for rank, row in enumerate(results, 1):
        title = row["title"] or row["doc_id"]
        print(f"{rank:2}. [{row['doc_id']}] {title}  (score={row['total_score']:.4f})")


if __name__ == "__main__":
    main()
