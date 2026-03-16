import logging
from pyspark.sql import functions as F

# Setup logger for the utils file
logger = logging.getLogger(__name__)


def split_location(df, colName, idx, exceptions):
    return df.withColumn(
        colName,
        F.when(
            (F.get(F.col("split_parts"), idx).isNull()) |
            (F.get(F.col("split_parts"), idx) == "") |
            (F.get(F.col("split_parts"), idx) == "n/a") |
            (F.get(F.col("split_parts"), idx) == ","),
            F.lit("Unknown")
        ).otherwise(
            F.when(
                F.get(F.col("split_parts"), idx).isin(exceptions),
                F.upper(F.get(F.col("split_parts"), idx))
            ).otherwise(
                # Handle hyphens and slashes properly
                    F.initcap(
                        F.regexp_replace(F.get(F.col("split_parts"), idx), "[-/]", " ")
                    )
                )
            )
        )

def null_check(df):
    """Checks and logs null counts for all columns."""
    for c in df.columns:
        null_counts = df.filter(df[c].isNull()).count()
        if null_counts > 0:
            logger.warning(f"Column '{c}': {null_counts} null values found")
        else:
            logger.info(f"Column '{c}': No nulls")
    return 