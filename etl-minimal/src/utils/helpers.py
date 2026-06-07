from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def create_spark_session() -> SparkSession:
    """Create a local Spark session with log level set to WARN."""
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_tsv(spark: SparkSession, path: str, schema: StructType, header: bool = False) -> DataFrame:
    """Read a TSV file into a DataFrame using the given schema."""
    return spark.read.csv(path, sep="\t", header=header, schema=schema)
