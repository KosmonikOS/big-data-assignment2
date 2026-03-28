from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from typing import Union


from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, Row


def is_acceptable_plain_text(text: Union[str, None]) -> bool:
    """Check if text is non-empty plain text, not JSON/XML/HTML-like markup.

    Args:
        text: A string containing the text to check.

    Returns:
        A boolean indicating if the text is acceptable.
    """
    if text is None:
        return False
    text = text.strip()
    if len(text) == 0:
        return False
    head = text.lstrip()[:200].lower()
    # XML
    if head.startswith("<?xml"):
        return False
    # HTML
    if head.startswith("<!doctype html") or head.startswith("<html"):
        return False
    if head.startswith("<!--"):
        return False
    # JSON
    stripped = text.lstrip()
    if stripped.startswith("<") and len(stripped) > 1:
        if stripped[1].isalpha() or stripped[1] == "/":
            return False
    if text[0] in "{[":
        try:
            json.loads(text)
            return False
        except json.JSONDecodeError:
            pass
    return True


def write_docs_partition(rows: Iterator[Row]) -> None:
    """Write each row in a partition to a local ``data/*.txt`` file (RDD foreachPartition).

    Args:
        rows: Iterator of Spark SQL ``Row`` objects with id, title, and text.
    """
    for row in rows:
        filename = (
            "data/"
            + sanitize_filename(str(row["id"]) + "_" + row["title"]).replace(" ", "_")
            + ".txt"
        )
        text = (row["text"] or "").strip()
        with open(filename, "w", encoding="utf-8") as f:
            f.write(text)


def parse_doc(filepath_content: tuple[str, str]) -> str:
    """Parse doc_id and title from filenames, and write consolidated TSV file for MapReduce input.

    Args:
        filepath_content: A tuple containing the file path and content.

    Returns:
        A string containing the document id, title, and text.
    """
    path, content = filepath_content
    filename = path.split("/")[-1].replace(".txt", "")
    doc_id, title = filename.split("_", 1) if "_" in filename else (filename, "")
    text = content.replace("\t", " ").replace("\n", " ")
    return f"{doc_id}\t{title}\t{text}"


def main() -> None:
    spark = (
    SparkSession.builder.appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
    )
    # UDF to check if text is acceptable plain text
    acceptable_plain_text_udf = udf(is_acceptable_plain_text, BooleanType())

    df = spark.read.parquet("/a.parquet").select(["id", "title", "text"])
    # To avoid processing the whole dataset (check if it texts are acceptable),
    # we have to sample first and filter only after
    # Potentially, this could result in less than 1000 documents, but this is
    # a compromise we accept to avoid processing the whole dataset
    df = df.filter(acceptable_plain_text_udf(col("text"))).limit(1000)

    # Load data to local filesystem
    os.makedirs("data", exist_ok=True)
    df.rdd.foreachPartition(write_docs_partition)

    # Move data to HDFS from local filesystem
    subprocess.run(["hdfs", "dfs", "-put", "data", "/"], check=True)

    # Finalize data for indexing
    rdd = spark.sparkContext.wholeTextFiles("/data")
    rdd.map(parse_doc).coalesce(1).saveAsTextFile("/input/data")
    
    # Cleanup local filesystem (still contains .txt files after hdfs dfs -put)
    shutil.rmtree("data", ignore_errors=True)


if __name__ == "__main__":
    main()