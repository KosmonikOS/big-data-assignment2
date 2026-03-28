import json
import subprocess, os


from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, Row


def is_acceptable_plain_text(text: str | None) -> bool:
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


def create_doc(row: Row) -> None:
    """Create a text file for each document in the data folder.

    The file name is the document id and title.
    The file content is the document text.

    Args:
        row: A Row object containing the document id, title, and text.
    """
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


spark = (
    SparkSession.builder.appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)
# UDF to check if text is acceptable plain text
acceptable_plain_text_udf = udf(is_acceptable_plain_text, BooleanType())


df = spark.read.parquet("/a.parquet").select(["id", "title", "text"])
df = df.filter(acceptable_plain_text_udf(col("text")))
n = 1000
total = df.count()
fraction = min(1.0, (100 * n) / max(total, 1))
df = df.sample(fraction=fraction, seed=0).limit(n)

os.makedirs("data", exist_ok=True)
df.foreach(create_doc)

# Remove existing data from HDFS (to avoid errors on re-runs)
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=False)
subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)

# We need to move the data to HDFS before parsing it
subprocess.run(["hdfs", "dfs", "-put", "data", "/"], check=True)

rdd = spark.sparkContext.wholeTextFiles("/data")
rdd.map(parse_doc).coalesce(1).saveAsTextFile("/input/data")
