from __future__ import annotations

import logging
import subprocess
import time
from typing import Generator
from cassandra.cluster import Cluster, NoHostAvailable, PreparedStatement, Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import RetryPolicy

logger = logging.getLogger(__name__)


HDFS_BASE = "/indexer"
CASSANDRA_HOST = "cassandra-server"
BATCH = 500

# DDL
KEYSPACE = "search_engine"
CREATE_KEYSPACE = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
"""
# Partition key = term so a query fetches all postings for a term in one read.
CREATE_INVERTED_INDEX = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.inverted_index (
    term    text,
    doc_id  text,
    tf      int,
    df      int,
    PRIMARY KEY (term, doc_id)
);
"""
# Per-document token count and title - implements dl(d) in BM25.
CREATE_DOC_STATS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.doc_stats (
    doc_id  text PRIMARY KEY,
    title   text,
    length  int
);
"""
# Single-row table (id = 'global') for corpus-level BM25 constants N and dlavg.
CREATE_CORPUS_STATS = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.corpus_stats (
    id          text PRIMARY KEY,
    num_docs    bigint,
    avg_doc_len double
);
"""
TABLES = ["inverted_index", "doc_stats", "corpus_stats"]


def wait_for_cassandra(
    contact_points: list[str], timeout: int = 180, interval: int = 5
) -> None:
    """Block until Cassandra accepts CQL connections or timeout expires.

    Args:
        contact_points: A list of contact points to connect to Cassandra.
        timeout: The timeout in seconds.
        interval: The interval in seconds to check for Cassandra availability.
    """
    print(f"Waiting for Cassandra at {contact_points}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cluster = Cluster(contact_points)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            cluster.shutdown()
            print("Cassandra is ready.")
            return
        except NoHostAvailable as exc:
            logger.debug("Cassandra not yet reachable: %s", exc)
            time.sleep(interval)
    raise RuntimeError(f"Cassandra not available after {timeout}s")


def connect(contact_points: list[str]) -> tuple[Cluster, Session]:
    """Connect to Cassandra and return a cluster and session.

    Args:
        contact_points: A list of contact points to connect to Cassandra.

    Returns:
        A tuple containing a cluster and session.
    """
    cluster = Cluster(contact_points, default_retry_policy=RetryPolicy())
    session = cluster.connect()
    return cluster, session


def setup_schema(session: Session) -> None:
    """Setup the schema for the Cassandra database.

    Args:
        session: The session to use to setup the schema.
    """
    # Execute all DDL statements
    for stmt in [
        CREATE_KEYSPACE,
        CREATE_INVERTED_INDEX,
        CREATE_DOC_STATS,
        CREATE_CORPUS_STATS,
    ]:
        session.execute(stmt)
    print("Schema created.")


def truncate_tables(session: Session) -> None:
    """Clear all tables before loading so re-runs are idempotent.

    Args:
        session: The session to use to truncate the tables.
    """
    for table in TABLES:
        session.execute(f"TRUNCATE {KEYSPACE}.{table}")
    print("Tables truncated.")


def hdfs_lines(hdfs_glob: str) -> Generator[str, None, None]:
    """Yield decoded lines from `hdfs dfs -cat <glob>`, skipping blank lines.

    Args:
        hdfs_glob: The HDFS glob to read from.

    Returns:
        A generator of decoded lines.
    """
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-cat", hdfs_glob],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for raw in proc.stdout:
        line = raw.decode("utf-8", errors="replace").rstrip("\n")
        if line:
            yield line
    proc.wait()
    if proc.returncode != 0:
        err = proc.stderr.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"hdfs dfs -cat {hdfs_glob} failed:\n{err}")


def _flush(session: Session, prepared: PreparedStatement, rows: list[tuple]) -> None:
    """Flush rows to Cassandra.

    Args:
        session: The session to use to flush the rows.
        prepared: The prepared statement to use to flush the rows.
        rows: The rows to flush.
    """
    if rows:
        execute_concurrent_with_args(session, prepared, rows, concurrency=50)


def load_inverted_index(session: Session, base_path: str) -> None:
    """Load the inverted index into the Cassandra database.

    Args:
        session: The session to use to load the inverted index.
        base_path: The base path to the inverted index.
    """
    print("Loading inverted_index")
    stmt = session.prepare(
        f"INSERT INTO {KEYSPACE}.inverted_index (term, doc_id, tf, df) VALUES (?, ?, ?, ?)"
    )
    rows: list[tuple[str, str, int, int]] = []
    for line in hdfs_lines(f"{base_path}/index/part-*"):
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        term, doc_id, tf, df = parts[0], parts[1], int(parts[2]), int(parts[3])
        rows.append((term, doc_id, tf, df))
        if len(rows) >= BATCH:
            _flush(session, stmt, rows)
            rows = []
    _flush(session, stmt, rows)


def load_stats(session: Session, base_path: str) -> None:
    """Load doc_stats (one row per doc) and corpus_stats (__GLOBAL__ line).

    Args:
        session: The session to use to load the stats.
        base_path: The base path to the stats.
    """
    print("Loading doc_stats and corpus_stats")
    doc_stmt = session.prepare(
        f"INSERT INTO {KEYSPACE}.doc_stats (doc_id, title, length) VALUES (?, ?, ?)"
    )
    corpus_stmt = session.prepare(
        f"INSERT INTO {KEYSPACE}.corpus_stats (id, num_docs, avg_doc_len) VALUES (?, ?, ?)"
    )
    rows: list[tuple[str, str, int]] = []
    for line in hdfs_lines(f"{base_path}/stats/part-*"):
        parts = line.split("\t")
        if parts[0] == "__GLOBAL__":
            if len(parts) < 3:
                continue
            num_docs, avg_doc_len = int(parts[1]), float(parts[2])
            session.execute(corpus_stmt, ("global", num_docs, avg_doc_len))
        else:
            # Line is corrupted, skip it
            if len(parts) < 3:
                continue
            doc_id, title, length = parts[0], parts[1], int(parts[2])
            rows.append((doc_id, title, length))
            if len(rows) >= BATCH:
                _flush(session, doc_stmt, rows)
                rows = []
    _flush(session, doc_stmt, rows)


def main():
    wait_for_cassandra([CASSANDRA_HOST])
    cluster, session = connect([CASSANDRA_HOST])

    try:
        setup_schema(session)
        truncate_tables(session)
        load_inverted_index(session, HDFS_BASE)
        load_stats(session, HDFS_BASE)
    finally:
        cluster.shutdown()

    print("All tables loaded successfully.")


if __name__ == "__main__":
    main()
