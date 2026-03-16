import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from constants import Config
from pyspark_utils import split_location, null_check

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

logger.info(f"\n\n\n# --- 2. PATH LOGIC GCP_KEY_PATH ---\n")      
logger.info(f"Using GCP Key from: {key_path}\n")



# --- 3. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .config("spark.hadoop.fs.gs.project.id", "books-pipeline-490008")
    .config("spark.hadoop.fs.gs.region", "europe-west10")
    .getOrCreate())

logger.info(f"\n\n\n# --- 3. SPARK SESSION ---")
logger.info(f"Spark version: {spark.version}")
# Reduce Spark internal noise
spark.sparkContext.setLogLevel("WARN")


# --- 4. PROCESS BOOKS DATA ---

def process_books(spark, input_path, output_path):
    logger.info(f"\n\n\n# --- 4. PROCESS BOOKS DATA ---")
    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.options(**Config.CSV_OPTIONS).csv(input_path)

    logger.info("Schema loaded. Printing schema to stdout:")
    df.printSchema()

    row_count = df.count()
    logger.info(f"CSV Loaded successfully. Total rows: {row_count}")
    
    # df.show() outputs to stdout by default, which is fine for visual logs
    #df.show(5)

    # Rename columns
    df = (df.withColumnRenamed("Book-Title", "title")
        .withColumnRenamed("Book-Author", "author")
        .withColumnRenamed("Year-Of-Publication", "year")
        .withColumnRenamed("Publisher", "publisher")
        .withColumnRenamed("Image-URL-S", "image_url_small")
        .withColumnRenamed("Image-URL-M", "image_url_medium")
        .withColumnRenamed("Image-URL-L", "image_url_large")
        )

    isbn_distinct = df.select('ISBN').distinct().count()
    logger.info(f"Distinct ISBN count: {isbn_distinct}")
    
    # Null checks
    null_amount = null_check(df)
    logger.info(f'Check missing values for BOOKS data: {null_amount}')

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

    logger.info(f"Writing transformed BOOKS data from {input_path} source to: {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")




# --- 5. PROCESS USERS DATA ---


def process_users(spark, input_path, output_path):
    logger.info(f"\n\n\n# --- 5. PROCESS USERS DATA ---")
    logger.info(f"Reading Users Data from: {input_path}")

    df = spark.read.options(**Config.CSV_OPTIONS).csv(input_path)

    logger.info(f"Schema for USERS data loaded")

    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for USERS: {row_count}")

    # logger.info("Sample rows USERS data:")
    # df.show(5)

    df = df.withColumnRenamed('User-ID', 'user_id') \
            .withColumnRenamed('Location', 'location') \
            .withColumnRenamed('Age', 'age')

    df = df.withColumn("age", F.col("age").cast("int"))

    df = df.withColumn('split_parts', F.split(F.col("location"), ", "))

    # Null checks
    null_amount = null_check(df)
    logger.info(f'Check missing values for USERS data: {null_amount}\n')
     

    df = split_location(df, "city", 0, Config.EXCEPTIONS_LIST)
    df = split_location(df, "region", 1, Config.EXCEPTIONS_LIST)
    df = split_location(df, "country", 2, Config.EXCEPTIONS_LIST)

    df = df.drop(F.col("split_parts"))

    #df.show()

    logger.info(f"Writing transformed USERS data from {input_path} source to: {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")




# --- 6. PROCESS RATING DATA ---


def process_rating(spark, input_path, output_path):

    logger.info(f"\n\n\n# --- 5. PROCESS RATING DATA ---")
    logger.info(f"Reading RATING Data from: {input_path}")

    df = spark.read.options(**Config.CSV_OPTIONS).csv(input_path)

    logger.info(f"Schema for RATING data loaded")

    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for RATING data: {row_count}")

    # logger.info("Sample rows RATING data:")
    # df.show(5)

    df = df.withColumnRenamed("User-ID", "user_id") \
            .withColumnRenamed("Book-Rating", "book_rating")
    
    # Null checks
    null_amount = null_check(df)
    logger.info(f'Check missing values for USERS data: {null_amount}\n')

    logger.info(f"Writing transformed RATING data from {input_path} source to: {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
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