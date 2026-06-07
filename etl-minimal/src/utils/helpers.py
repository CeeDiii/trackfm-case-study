from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_tsv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.csv(path, sep="\t", header=False, schema=schema)
