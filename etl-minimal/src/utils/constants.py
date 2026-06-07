from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = str(BASE_DIR / "data/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv")
SESSION_GAP_IN_MIN = 20

SCHEMA = StructType([
    StructField("user_id", StringType()),
    StructField("timestamp_str", StringType()),
    StructField("artist_id", StringType()),
    StructField("artist_name", StringType()),
    StructField("track_id", StringType()),
    StructField("track_name", StringType()),
])
