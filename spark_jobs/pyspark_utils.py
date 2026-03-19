from pyspark.sql import functions as F
from constants import Config

def clean_value(col):
    """Basic string cleanup: lower, trim, remove quotes/slashes."""
    return F.trim(
        F.lower(
            F.regexp_replace(col, r'[,\"\\]', '')
        )
    )

def clean_simple(col):
    """Simple Title Case for non-mapped values."""
    cleaned = clean_value(col)
    return (
        F.when(cleaned.isNull() | (cleaned == "") | cleaned.isin(Config.NOISE_TOKENS), F.lit("Unknown"))
        .when(cleaned.isin(Config.EXCEPTIONS_LIST), F.upper(cleaned))
        .otherwise(F.initcap(cleaned))
    )

def split_location_raw(df):
    """Splits location safely by checking array size before accessing indices."""
    # Using regex to handle comma with or without space
    parts = F.split(F.col("location"), r",\s*")
    size = F.size(parts)
    
    return df.withColumn("loc_size", size) \
             .withColumn("raw_first_part", 
                         F.when(size >= 1, clean_value(parts.getItem(0)))) \
             .withColumn("raw_second_part", 
                         F.when(size >= 2, clean_value(parts.getItem(1)))) \
             .withColumn("raw_last_part", 
                         F.when(size >= 1, clean_value(parts.getItem(size - 1)))) \
             .withColumn("raw_pre_last_part", 
                         F.when(size >= 2, clean_value(parts.getItem(size - 2))))


def null_check(df, table_name="Data"):
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f'\nChecking nulls for {table_name}:')
    for c in df.columns:
        cnt = df.filter(df[c].isNull()).count()
        if cnt > 0:
            logger.warning(f"Column '{c}': {cnt} nulls")
        else:
            logger.info(f"Column '{c}': No nulls")