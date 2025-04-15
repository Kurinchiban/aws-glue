from pyspark.sql import DataFrame


def read_csv_with_header(spark_session, path: str) -> DataFrame:
    """Reads a CSV from the given S3 path with header option enabled."""
    return spark_session.read.option("header", "true").csv(path)

def write_csv_with_header(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Writes a DataFrame to a given path as CSV with header."""
    df.write.mode(mode).option("header", "true").csv(path)