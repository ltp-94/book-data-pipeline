import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from constants import Config, Schemas
from pyspark_utils import split_location, null_check

# --- Logging setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Spark session ---
spark = (
    SparkSession.builder
    .appName("GCS Test")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- Paths ---
input_path = "/workspaces/book-data-pipeline/Users.csv"
output_path = "/workspaces/book-data-pipeline/users_output"

def process_users(spark, input_path, output_path):
    logger.info("# --- PROCESS USERS DATA ---")
    logger.info(f"Reading Users Data from: {input_path}")

    df = (
        spark.read.options(**Config.CSV_OPTIONS)
        .schema(Schemas.USERS_SCHEMA)
        .csv(input_path)
    )

    logger.info("Schema for USERS data loaded")
    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for USERS: {row_count}")

    logger.info("Sample rows USERS data:")
    df.show(5)

    df = df.withColumn("age", F.col("age").cast("int"))
    df = df.withColumn("split_parts", F.split(F.col("location"), ", "))

    # Null checks
    null_amount = null_check(df)
    logger.info(f"Check missing values for USERS data: {null_amount}")

    # Split location into city, region, country
    df = split_location(df)

    df = df.drop(F.col("split_parts"))
    df = df.withColumn("ingested_at", F.current_timestamp())

    df.show(5)

    logger.info(f"Writing transformed USERS data to: {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")

# --- Run job ---
process_users(spark, input_path, output_path)

# --- Stop Spark ---
spark.stop()
