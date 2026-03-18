import logging
from pyspark.sql import functions as F
from constants import Config


# Setup logger for the utils file
logger = logging.getLogger(__name__)



def clean_value(col):
    return F.trim(
        F.lower(
            F.regexp_replace(
                F.regexp_replace(col, r'[,\"\\]', ''), r'\s+', ' '
            )
        )
    )

def clean_and_normalize(col):
    cleaned = clean_value(col)
    return (
        F.when(cleaned.isin(Config.NOISE_TOKENS), F.lit("Unknown"))
        .otherwise(
            F.when(cleaned.isin(Config.EXCEPTIONS_LIST), F.upper(cleaned))
            .otherwise(F.initcap(cleaned))
        )
    )


def split_location(df):
    df = df.withColumn(
        "country",
        F.when(
            (F.size(F.col("split_parts")) >= 1) &
            (clean_value(F.element_at(F.col("split_parts"), -1)).isin(Config.VALID_COUNTRIES)),
            clean_and_normalize(F.element_at(F.col("split_parts"), -1))
        ).otherwise(F.lit("Unknown"))
    )

    df = df.withColumn(
        "region",
        F.when(F.size(F.col("split_parts")) == 2,
               clean_and_normalize(F.element_at(F.col("split_parts"), -1))
        ).when(F.size(F.col("split_parts")) >= 3,
               clean_and_normalize(F.element_at(F.col("split_parts"), -2))
        ).otherwise(F.lit("Unknown"))
    )

    df = df.withColumn(
        "city",
        F.when(F.size(F.col("split_parts")) == 2,
               clean_and_normalize(F.element_at(F.col("split_parts"), -2))
        ).when(F.size(F.col("split_parts")) >= 3,
               clean_and_normalize(F.element_at(F.col("split_parts"), -3))
        ).otherwise(
            clean_and_normalize(F.element_at(F.col("split_parts"), 1))
        )
    )

    return df


def null_check(df):
    """Checks and logs null counts for all columns."""
    for c in df.columns:
        null_counts = df.filter(df[c].isNull()).count()
        if null_counts > 0:
            logger.warning(f"Column '{c}': {null_counts} null values found")
        else:
            logger.info(f"Column '{c}': No nulls")
    return 