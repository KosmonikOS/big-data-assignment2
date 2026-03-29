from __future__ import annotations

import logging
import time

from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import RetryPolicy
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

HDFS_BASE = "hdfs://cluster-master:9000/indexer"
CASSANDRA_HOST = "cassandra-server"

KEYSPACE = "search_engine"
CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
"""
CREATE_INVERTED_INDEX = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.inverted_index (
    term    text,
    doc_id  text,
    tf      int,
    df      int,
    PRIMARY KEY (term, doc_id)
);
"""
CREATE_DOC_STATS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.doc_stats (
    doc_id  text PRIMARY KEY,
    title   text,
    length  int
);
"""
CREATE_CORPUS_STATS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.corpus_stats (
    id          text PRIMARY KEY,
    num_docs    bigint,
    avg_doc_len double
);
"""
TABLES = ["inverted_index", "doc_stats", "corpus_stats"]

_INVERTED_INDEX_SCHEMA = StructType([
    StructField("term", StringType(), nullable=False),
    StructField("doc_id", StringType(), nullable=False),
    StructField("tf", IntegerType(), nullable=False),
    StructField("df", IntegerType(), nullable=False),
])

_STATS_RAW_SCHEMA = StructType([
    StructField("col0", StringType(), nullable=True),
    StructField("col1", StringType(), nullable=True),
    StructField("col2", StringType(), nullable=True),
])

_DOC_STATS_SCHEMA = StructType([
    StructField("doc_id", StringType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("length", IntegerType(), nullable=False),
])

_CORPUS_STATS_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("num_docs", LongType(), nullable=False),
    StructField("avg_doc_len", DoubleType(), nullable=False),
])


def wait_for_cassandra(
    contact_points: list[str], timeout: int = 180, interval: int = 5
) -> None:
    """Block until Cassandra accepts CQL connections or timeout expires.

    Args:
        contact_points: A list of contact points to connect to Cassandra.
        timeout: The timeout in seconds.
        interval: The interval in seconds to check for Cassandra availability.

    Raises:
        RuntimeError: If Cassandra is not available after the timeout.
    """
    logger.info("Waiting for Cassandra at %s", contact_points)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cluster = Cluster(contact_points)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            cluster.shutdown()
            logger.info("Cassandra is ready.")
            return
        except NoHostAvailable as exc:
            logger.debug("Cassandra not yet reachable: %s", exc)
            time.sleep(interval)
    raise RuntimeError(f"Cassandra not available after {timeout}s")


def _cql_session(contact_points: list[str]) -> tuple[Cluster, Session]:
    """Open a Cassandra cluster connection for DDL execution.

    Args:
        contact_points: A list of contact points to connect to Cassandra.

    Returns:
        A tuple of (Cluster, Session) for the caller to close when done.
    """
    cluster = Cluster(contact_points, default_retry_policy=RetryPolicy())
    session = cluster.connect()
    return cluster, session


def setup_schema(session: Session) -> None:
    """Create the keyspace and all three tables if they do not already exist.

    Args:
        session: An open CQL session (no keyspace selected required).
    """
    for stmt in [
        CREATE_KEYSPACE,
        CREATE_INVERTED_INDEX,
        CREATE_DOC_STATS,
        CREATE_CORPUS_STATS,
    ]:
        session.execute(stmt)
    logger.info("Schema created.")


def truncate_tables(session: Session) -> None:
    """Clear all tables before loading so re-runs are idempotent.

    Args:
        session: An open CQL session (no keyspace selected required).
    """
    for table in TABLES:
        session.execute(f"TRUNCATE {KEYSPACE}.{table}")
    logger.info("Tables truncated.")


def _cassandra_write(df: DataFrame, table: str) -> None:
    """Write a DataFrame to a Cassandra table via the Spark-Cassandra Connector.

    Args:
        df: The DataFrame to write. Column names must exactly match the target table.
        table: The Cassandra table name within KEYSPACE.
    """
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=KEYSPACE) \
        .mode("append") \
        .save()


def load_inverted_index(spark: SparkSession, base_path: str) -> None:
    """Read inverted index TSV files from HDFS and write to Cassandra.

    Each line has the format: term\\tdoc_id\\ttf\\tdf

    Args:
        spark: Active SparkSession with Cassandra connector configured.
        base_path: HDFS root path that contains the ``index/part-*`` files.
    """
    logger.info("Loading inverted_index from %s/index", base_path)
    raw: DataFrame = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .schema(_INVERTED_INDEX_SCHEMA)
        .csv(f"{base_path}/index/part-*")
    )
    df = raw.dropna(subset=["term", "doc_id"])
    _cassandra_write(df, "inverted_index")
    logger.info("inverted_index loaded.")


def load_stats(spark: SparkSession, base_path: str) -> None:
    """Read stats TSV files from HDFS and write to doc_stats and corpus_stats.

    Two line formats are interleaved in the same part files:
        - ``__GLOBAL__\\t<num_docs>\\t<avg_doc_len>`` — one corpus-level row
        - ``<doc_id>\\t<title>\\t<length>``            — one row per document

    Args:
        spark: Active SparkSession with Cassandra connector configured.
        base_path: HDFS root path that contains the ``stats/part-*`` files.
    """
    logger.info("Loading doc_stats and corpus_stats from %s/stats", base_path)
    raw: DataFrame = (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .schema(_STATS_RAW_SCHEMA)
        .csv(f"{base_path}/stats/part-*")
    )

    doc_df: DataFrame = (
        raw.filter(F.col("col0") != "__GLOBAL__")
        .dropna(subset=["col0", "col2"])
        .select(
            F.col("col0").alias("doc_id"),
            F.col("col1").alias("title"),
            F.col("col2").cast(IntegerType()).alias("length"),
        )
    )
    _cassandra_write(doc_df, "doc_stats")
    logger.info("doc_stats loaded.")

    corpus_df: DataFrame = (
        raw.filter(F.col("col0") == "__GLOBAL__")
        .dropna(subset=["col1", "col2"])
        .select(
            F.lit("global").alias("id"),
            F.col("col1").cast(LongType()).alias("num_docs"),
            F.col("col2").cast(DoubleType()).alias("avg_doc_len"),
        )
    )
    _cassandra_write(corpus_df, "corpus_stats")
    logger.info("corpus_stats loaded.")


def build_spark_session() -> SparkSession:
    """Create a SparkSession pre-configured for the Spark-Cassandra Connector.

    Returns:
        A configured SparkSession.
    """
    return (
        SparkSession.builder
        .appName("LoadIndexToCassandra")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )


def main() -> None:
    """Orchestrate schema setup and full data load from HDFS into Cassandra."""
    logging.basicConfig(level=logging.INFO)

    wait_for_cassandra([CASSANDRA_HOST])

    cluster, cql_session = _cql_session([CASSANDRA_HOST])
    try:
        setup_schema(cql_session)
        truncate_tables(cql_session)
    finally:
        cluster.shutdown()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        load_inverted_index(spark, HDFS_BASE)
        load_stats(spark, HDFS_BASE)
    finally:
        spark.stop()

    logger.info("All tables loaded successfully.")


if __name__ == "__main__":
    main()
