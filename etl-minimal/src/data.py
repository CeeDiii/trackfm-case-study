"""
Shared data-loading utilities for the lastfm-dataset-1K exercises.
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from sessions import SESSION_GAP_IN_MIN, add_session_ids, add_sessions

BASE_DIR = Path(__file__).parent.parent
DATA_FILE = str(BASE_DIR / "data/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")

SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp_str", StringType()),
    StructField("artist_id", StringType()),
    StructField("artist_name", StringType()),
    StructField("track_id", StringType()),
    StructField("track_name", StringType()),
])


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_plays(spark: SparkSession, gap_minutes: int = SESSION_GAP_IN_MIN) -> DataFrame:
    """Read the raw TSV and return a DataFrame with session columns attached."""
    df = spark.read.csv(DATA_FILE, sep="\t", header=False, schema=SCHEMA)
    df = add_sessions(df, gap_minutes)
    return add_session_ids(df)
