"""
Reusable session detection built on Spark's session_window.

A session is a contiguous run of plays per user where no gap between
consecutive tracks exceeds SESSION_GAP_IN_MIN minutes.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SESSION_GAP_IN_MIN = 20


def add_sessions(df: DataFrame, gap_minutes: int = SESSION_GAP_IN_MIN) -> DataFrame:
    """Parse timestamps and attach a `session` struct {start, end} to each row."""
    return (
        df
        .withColumn("started_at", F.to_timestamp("timestamp_str"))
        .withColumn("session", F.session_window("started_at", F.lit(f"{gap_minutes} minutes")))
    )


def add_session_ids(df: DataFrame) -> DataFrame:
    """Add a `session_id` string column that uniquely identifies each user session.

    Format: <user_id>_<session_window_start>
    Requires the `session` struct column produced by add_sessions().
    """
    return df.withColumn(
        "session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session.start").cast("string")),
    )
