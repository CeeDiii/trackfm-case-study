"""
Exercise 3 — Forecast session metrics for the top user by session count.

Metric options (set FORECAST_METRIC):
  "session_count"        — daily number of sessions
  "avg_duration_minutes" — daily average session duration in minutes

Forecast covers 90 days beyond the user's last recorded session.
"""
import logging
from pathlib import Path

from prophet import Prophet
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.constants import BASE_DIR, DATA_FILE, SCHEMA, SESSION_GAP_IN_MIN
from utils.helpers import create_spark_session, load_tsv
from utils.logging import configure_logging
from utils.sessions import add_session_ids, add_sessions

logger = logging.getLogger(__name__)

OUTPUT_FILE = str(BASE_DIR / "data/results/exercise3_forecast.tsv")

FORECAST_METRIC = "session_count"  # or "avg_duration_minutes"
FORECAST_HORIZON_DAYS = 90


def build_session_summary(df: DataFrame) -> DataFrame:
    """Aggregate per-session metrics: start/end timestamps, track count, and duration in minutes."""
    return df.groupBy("user_id", "session_id", "session").agg(
        F.min("started_at").alias("session_start"),
        F.max("started_at").alias("session_end"),
        F.count("*").alias("track_count"),
        (
            (F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60
        ).alias("duration_minutes"),
    )


def get_top_user(session_summary: DataFrame) -> str:
    """Return the user_id with the highest number of sessions."""
    return (
        session_summary
        .groupBy("user_id")
        .agg(F.count("session_id").alias("num_sessions"))
        .orderBy("num_sessions", ascending=False)
        .first()["user_id"]
    )


def build_daily_metric(session_summary: DataFrame, user_id: str, metric: str):
    """Build a daily time series for a single user, ready for Prophet (columns: ds, y)."""
    user_sessions = (
        session_summary
        .filter(F.col("user_id") == user_id)
        .withColumn("date", F.to_date("session_start"))
    )

    if metric == "session_count":
        daily = user_sessions.groupBy("date").agg(F.count("session_id").alias("y"))
    else:
        daily = user_sessions.groupBy("date").agg(F.avg("duration_minutes").alias("y"))

    return daily.orderBy("date").toPandas().rename(columns={"date": "ds"})


def forecast(daily_df, horizon_days: int = FORECAST_HORIZON_DAYS):
    """Fit a Prophet model and return predictions for the next horizon_days days."""
    m = Prophet()
    m.fit(daily_df)
    future = m.make_future_dataframe(periods=horizon_days)
    return m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]


def main() -> None:
    """Forecast session metrics for the top user and write predictions to TSV."""
    spark = create_spark_session()
    df = load_tsv(spark, DATA_FILE, SCHEMA)
    df = add_sessions(df, SESSION_GAP_IN_MIN)
    df = add_session_ids(df)

    sessions = build_session_summary(df)

    top_user = get_top_user(sessions)
    logger.info("Top user by session count: %s", top_user)

    logger.info("Building daily '%s' series ...", FORECAST_METRIC)
    daily = build_daily_metric(sessions, top_user, FORECAST_METRIC)
    logger.info("Last 5 days:\n%s", daily.tail().to_string())

    logger.info("Forecasting %d days ahead ...", FORECAST_HORIZON_DAYS)
    predictions = forecast(daily, FORECAST_HORIZON_DAYS)

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT_FILE, sep="\t", index=False)
    logger.info("Forecast written to %s", OUTPUT_FILE)

    spark.stop()


if __name__ == "__main__":
    configure_logging()
    main()
