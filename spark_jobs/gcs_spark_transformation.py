import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from constants import Config, Schemas
from pyspark_utils import split_location_raw, null_check, clean_simple

# --- 1. CONFIGURE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.info(f"# --- 1. CONFIGURE LOGGING ---\n")

# --- 2. PATH LOGIC ---
start = time.time()
key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")

if not key_path or not os.path.exists(key_path):
    if os.path.exists("../service-account.json"):
        key_path = "../service-account.json"
    else:
        key_path = "gcp-key.json"

logger.info(f"\n\n# --- 2. PATH LOGIC GCP_KEY_PATH ---\n")      
logger.info(f"Using GCP Key from: {key_path}\n")



# --- 3. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .getOrCreate())

logger.info(f"\n\n# --- 3. SPARK SESSION ---")
logger.info(f"Spark version: {spark.version}")
# Reduce Spark internal noise
spark.sparkContext.setLogLevel("WARN")


# --- 4. PROCESS BOOKS DATA ---

def process_books(spark, input_path, output_path):
    logger.info(f"\n\n# --- 4. PROCESS BOOKS DATA ---")
    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.BOOKS_SCHEMA).csv(input_path)
    logger.info("Schema loaded. Printing schema to stdout:")
    df.printSchema()

    row_count = df.count()
    logger.info(f"CSV Loaded successfully. Total rows: {row_count}")
    
    # df.show() outputs to stdout by default, which is fine for visual logs
    df.show(5)

    isbn_distinct = df.select('ISBN').distinct().count()
    logger.info(f"Distinct ISBN count: {isbn_distinct}")
    
    # Null checks
    null_check(df, table_name='BOOKS')
    

    # Handling shifted rows
    df = df.withColumn("split_parts", F.split(F.col("title"), r'\";'))
    shifted_rows = df.filter(F.size(F.col("split_parts")) > 1).count()
    logger.info(f"Found {shifted_rows} rows with title splitting issues")

    author_condition = (F.col("author").rlike(r'^\d{4}$'))

    df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
                        .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
                        .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
                        .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
                        .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author")))          
                                
    # Encoding fixes
    logger.info("Apply ENCODING_FIXES from constants")
    for bad_str, good_str in Config.ENCODING_FIXES.items():
        df = df.withColumn("title", F.regexp_replace(F.col("title"), bad_str, good_str))

    # Regex Cleanup
    df = df.withColumn("title", F.translate(F.col("title"), '\\"', '')) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "&amp;", "&")) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "/", ", ")) \
                        .withColumn("title", F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
                    )

    df = df.drop("split_parts").withColumn("year", F.col("year").cast("int"))
    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info(f"Writing transformed BOOKS data from {input_path} source to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")




# --- 5. PROCESS USERS DATA ---


def process_users(spark, input_path, output_path):
    logger.info("# --- PROCESS USERS DATA ---")
    
    # 1. CREATE LOOKUP DATAFRAME (The Secret Sauce)
    # We combine standard countries and specific variants (U.s.a., etc.)
    lookup_list = []
    for c in Config.VALID_COUNTRIES:
        lookup_list.append((c.lower(), c))
    for raw, clean in Config.COUNTRY_MAPPING.items():
        lookup_list.append((raw.lower(), clean))
    
    # Remove duplicates and create DF
    lookup_df = spark.createDataFrame(list(set(lookup_list)), ["raw_name", "clean_country"])

    # 2. READ DATA
    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.USERS_SCHEMA).csv(input_path)
    
    # 3. FIX AGE (Decimal String -> Float -> Int)
    df = df.withColumn("age", F.expr("try_cast(age AS float)").cast("int"))

    # 4. SPLIT LOCATION
    df = split_location_raw(df)
    
    # 5. BROADCAST JOIN FOR COUNTRY (This replaces the massive 'INSET' logic)
    # We join our main data against the small lookup table
    df = df.join(F.broadcast(lookup_df), df.raw_last_part == lookup_df.raw_name, "left")
    
    # Assign the result of the join to 'country'
    df = df.withColumn("country", F.coalesce(F.col("clean_country"), F.lit("Unknown")))

    # 6. CITY & REGION LOGIC (Simplified to avoid 64KB error)
    # Check if first part contains digits (address)
    first_has_digits = F.col("raw_first_part").rlike(r"\d")

    df = df.withColumn("city", 
        F.when((F.col("loc_size") >= 3) & first_has_digits, clean_simple(F.col("raw_second_part")))
        .otherwise(clean_simple(F.col("raw_first_part")))
    )

    df = df.withColumn("region", 
        # If address exists (size 4), region is usually the 3rd part
        F.when((F.col("loc_size") >= 4) & first_has_digits, clean_simple(F.col("raw_pre_last_part")))
        # Standard case: region is the part before the country
        .when(F.col("loc_size") >= 2, clean_simple(F.col("raw_pre_last_part")))
        .otherwise(F.lit("Unknown"))
    )


    # 7. CLEANUP & FINALIZE
    df = df.withColumn("age_anomaly", F.when((F.col("age") < 5) | (F.col("age") > 100), 1).otherwise(0))
    df = df.select(
        "user_id", "location", "age", "age_anomaly", "country", "city", "region",
        F.current_timestamp().alias("ingested_at")
    )

    null_check(df, "USERS")
    df.show(10)

    logger.info(f"Writing transformed USERS to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")




# --- 6. PROCESS RATING DATA ---


def process_rating(spark, input_path, output_path):

    logger.info(f"\n\n\n# --- 5. PROCESS RATINGS DATA ---")
    logger.info(f"Reading RATINGS Data from: {input_path}")

    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.RATINGS_SCHEMA).csv(input_path)

    logger.info(f"Schema for RATINGS data loaded")

    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for RATINGS data: {row_count}")

    logger.info("Sample rows RATINGS data:")
    df.show(5)
    
    # Null checks
    null_amount = null_check(df, table_name='RATINGS')

    df = df.withColumn("ingested_at", F.current_timestamp())

    logger.info(f"Writing transformed RATINGS data from {input_path} source to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")


# --- 7. EXECUTION ---
try:
    process_books(spark, input_path = Config.INPUT_PATH_BOOKS, output_path = Config.OUTPUT_PATH_BOOKS)
except Exception as e:
    logger.exception("An error occurred during the BOOKS transformation:")


try:
    process_users(spark, input_path = Config.INPUT_PATH_USERS, output_path = Config.OUTPUT_PATH_USERS)
except Exception as e:
    logger.exception("An error occurred during the USERS transformation:")


try:
    process_rating(spark, input_path = Config.INPUT_PATH_RATING, output_path = Config.OUTPUT_PATH_RATING)
except Exception as e:
    logger.exception("An error occurred during the RATING transformation:")
        
        
end = time.time()
logger.info(f"Total time elapsed: {round(end - start, 2)} seconds")


spark.stop()