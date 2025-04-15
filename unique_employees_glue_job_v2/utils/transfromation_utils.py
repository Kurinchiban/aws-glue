from pyspark.sql import DataFrame

def deduplicate_by_columns(df: DataFrame, cols: list) -> DataFrame:
    """Returns a DataFrame with duplicates removed based on specified columns."""
    return df.dropDuplicates(cols)