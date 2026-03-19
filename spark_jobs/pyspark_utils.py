import logging
from pyspark.sql import functions as F
# from constants import Config


# Setup logger for the utils file
logger = logging.getLogger(__name__)



from pyspark.sql import functions as F


# ================= CLEANING =================

def clean_value(col):

    """

    Basic cleaning:

    - lower

    - trim

    - remove quotes, commas, slashes

    - normalize spaces

    """

    return F.trim(

        F.lower(

            F.regexp_replace(

                F.regexp_replace(col, r'[,\"\\]', ''),

                r'\s+',

                ' '

            )

        )

    )


def clean_and_normalize(col):
    """
    Normalize values:
    - noise → "Unknown"
    - variants (USA, UK, UAE) → Standardized Acronyms
    - exceptions → UPPER (ny, la, etc.)
    - others → Title Case
    """
    cleaned = clean_value(col)

    # 1. Apply normalization map first to a temporary column expression
    normalized = (
        F.when(cleaned.isin(Config.USA_VARIANTS), F.lit("USA"))
        .when(cleaned.isin(Config.UK_VARIANTS), F.lit("UK"))
        .when(cleaned.isin(Config.UAE_VARIANTS), F.lit("UAE"))
        .otherwise(cleaned)
    )

    # 2. Use the 'normalized' variable for the final logic
    return (
        F.when(normalized.isNull() | normalized.isin(Config.NOISE_TOKENS), F.lit("Unknown"))
        .when(normalized.isin(Config.EXCEPTIONS_LIST), F.upper(normalized))
        # Add a check to prevent Initcap from turning "USA" into "Usa"
        .when(normalized.isin(["USA", "UK", "UAE"]), normalized) 
        .otherwise(F.initcap(normalized))
    )


# ================= SPLIT LOCATION =================

def split_location(df):

    parts = F.split(F.col("location"), ", ")
    size = F.size(parts)

    # --- safe access ---

    first = F.when(size >= 1, parts.getItem(0))
    second = F.when(size >= 2, parts.getItem(1))

    third = F.when(size >= 3, parts.getItem(2))

    last = F.when(size >= 1, parts.getItem(size - 1))

    pre_last = F.when(size >= 2, parts.getItem(size - 2))

    # --- detect address ---

    first_has_digits = first.rlike(r"\d")

    # --- cleaned values for logic ---

    last_clean = clean_value(last)

    df = (

        df

        # ================= COUNTRY =================

        .withColumn(

            "country",

            F.when(

                (size >= 1) & (last_clean.isin(Config.VALID_COUNTRIES)),

                clean_and_normalize(last)

            ).otherwise(F.lit("Unknown"))

        )

        # ================= CITY =================

        .withColumn(

            "city",

            F.when(

                (size >= 3) & first_has_digits,

                clean_and_normalize(second)  # skip address

            ).when(

                size >= 2,

                clean_and_normalize(first)

            ).when(

                size == 1,

                clean_and_normalize(first)

            ).otherwise(F.lit("Unknown"))

        )

        # ================= REGION =================

        .withColumn(

            "region",

            F.when(

                (size >= 4) & first_has_digits,

                clean_and_normalize(third)  # shifted because of address

            ).when(

                size >= 3,

                clean_and_normalize(pre_last)

            ).when(size >=2, clean_and_normalize(last)).otherwise(F.lit("Unknown"))
        )
    )
    return df
 


def null_check(df):
    """Checks and logs null counts for all columns."""
    logger.info(f'\nCheck missing values for USERS data:\n')
    for c in df.columns:
        null_counts = df.filter(df[c].isNull()).count()
        if null_counts > 0:
            logger.warning(f"Column '{c}': {null_counts} null values found")
        else:
            logger.info(f"Column '{c}': No nulls")