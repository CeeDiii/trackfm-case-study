"""
Exercise 3 — Forecast session metrics for the top user by session count.

Metric options (set FORECAST_METRIC):
  "session_count"        — daily number of sessions
  "avg_duration_minutes" — daily average session duration in minutes

AutoARIMA is trained on all users' daily data in a single call. Predictions
are then filtered to the top user, giving exactly FORECAST_HORIZON_DAYS rows.
"""
import logging
from pathlib import Path

import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA
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


def build_daily_metric(session_summary: DataFrame, metric: str) -> pd.DataFrame:
    """Build a per-user daily time series in statsforecast format (unique_id, ds, y)."""
    with_date = session_summary.withColumn("date", F.to_date("session_start"))

    if metric == "session_count":
        daily = with_date.groupBy("user_id", "date").agg(F.count("session_id").alias("y"))
    else:
        daily = with_date.groupBy("user_id", "date").agg(F.avg("duration_minutes").alias("y"))

    return (
        daily.orderBy("user_id", "date")
        .toPandas()
        .rename(columns={"user_id": "unique_id", "date": "ds"})
    )


def get_user_last_n_days(session_summary: DataFrame, user_id: str, n: int) -> pd.DataFrame:
    """Return a pandas DataFrame of the n most recent session-days for the given user."""
    return (
        session_summary
        .filter(F.col("user_id") == user_id)
        .withColumn("date", F.to_date("session_start"))
        .groupBy("date")
        .agg(F.count("session_id").alias("session_count"))
        .orderBy("date", ascending=False)
        .limit(n)
        .toPandas()
        .sort_values("date")
    )


def forecast(daily_df: pd.DataFrame, user_id: str, horizon_days: int = FORECAST_HORIZON_DAYS) -> pd.DataFrame:
    """Train AutoARIMA on all users and return horizon_days predictions for user_id."""
    sf = StatsForecast(models=[AutoARIMA(season_length=7)], freq="D", n_jobs=-1)
    sf.fit(daily_df)
    predictions = sf.predict(h=horizon_days, level=[90])
    result = predictions[predictions["unique_id"] == user_id].copy()
    for col in ["AutoARIMA", "AutoARIMA-lo-90", "AutoARIMA-hi-90"]:
        result[col] = result[col].clip(lower=0)
    return result[["ds", "AutoARIMA", "AutoARIMA-lo-90", "AutoARIMA-hi-90"]].rename(columns={
        "ds": "date",
        "AutoARIMA": FORECAST_METRIC,
        "AutoARIMA-lo-90": f"{FORECAST_METRIC}_lower",
        "AutoARIMA-hi-90": f"{FORECAST_METRIC}_upper",
    })


def main() -> None:
    """Forecast session metrics for the top user and write predictions to TSV."""
    spark = create_spark_session()
    df = load_tsv(spark, DATA_FILE, SCHEMA)
    df = add_sessions(df, SESSION_GAP_IN_MIN)
    df = add_session_ids(df)

    sessions = build_session_summary(df)

    top_user = get_top_user(sessions)
    logger.info("Top user by session count: %s", top_user)

    last_15 = get_user_last_n_days(sessions, top_user, 15)
    logger.info("Last 15 session-days for %s:\n%s", top_user, last_15.to_string(index=False))

    logger.info("Building daily '%s' series for all users ...", FORECAST_METRIC)
    daily = build_daily_metric(sessions, FORECAST_METRIC)

    logger.info("Fitting AutoARIMA on %d users, forecasting %d days ...", daily["unique_id"].nunique(), FORECAST_HORIZON_DAYS)
    predictions = forecast(daily, top_user, FORECAST_HORIZON_DAYS)

    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT_FILE, sep="\t", index=False)
    logger.info("Forecast written to %s", OUTPUT_FILE)

    spark.stop()


if __name__ == "__main__":
    configure_logging()
    main()
