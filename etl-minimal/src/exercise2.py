"""
Exercise 2 — Top 10 songs played in the top 50 longest sessions (by duration).

A session is a contiguous run of plays per user where no gap between
consecutive tracks exceeds 20 minutes.
"""
import logging
from pathlib import Path

from pyspark.sql import functions as F

from data import BASE_DIR, create_spark_session, load_plays
from logging_config import configure_logging

logger = logging.getLogger(__name__)

OUTPUT_FILE = str(BASE_DIR / "data/results/excercise2_top_10_songs_in_top_50_longest_sessions.tsv")


def main() -> None:
    spark = create_spark_session()
    df = load_plays(spark)

    top_50_longest_sessions = (
        df.groupBy("user_id", "session")
        .agg(
            F.min("started_at").alias("session_start"),
            F.max("started_at").alias("session_end"),
            (
                F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))
            ).alias("session_duration"),
        )
        .orderBy("session_duration", ascending=False)
        .limit(50)
    )

    # group by track_name because track_id can be empty
    top_10 = (
        df.alias("base")
        .join(
            top_50_longest_sessions.alias("top"),
            on=[
                F.col("base.user_id") == F.col("top.user_id"),
                F.col("base.started_at") >= F.col("top.session_start"),
                F.col("base.started_at") <= F.col("top.session_end"),
            ],
            how="inner",
        )
        .groupBy("track_name", "artist_name")
        .agg(F.count("*").alias("times_played"))
        .orderBy("times_played", ascending=False)
        .limit(10)
    )

    top_10.show(truncate=False)

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    top_10.toPandas().to_csv(OUTPUT_FILE, sep="\t", index=False)
    logger.info("Results written to %s", OUTPUT_FILE)

    spark.stop()


if __name__ == "__main__":
    configure_logging()
    main()
