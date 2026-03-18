import os
import time
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, year, month
from pyspark.sql import functions as F
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from constants import Config, Schemas
from pyspark_utils import split_location, null_check


start = time.time()
spark = (
   SparkSession.builder \
   .appName("GCS Test")\
   .getOrCreate()
)


# Add this line here:
spark.sparkContext.setLogLevel("WARN")


# --- 2. Paths ---
input_path = "/workspaces/books-data-pipeline/data/Users.csv"
output_path = "/workspaces/books-data-pipeline/data/users_output"

def process_users(spark, input_path, output_path):
    logger.info(f"\n\n\n# --- 5. PROCESS USERS DATA ---")
    logger.info(f"Reading Users Data from: {input_path}")

    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.USERS_SCHEMA).csv(input_path)

    logger.info(f"Schema for USERS data loaded")

    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for USERS: {row_count}")

    logger.info("Sample rows USERS data:")
    df.show(5)

    df = df.withColumn("age", F.col("age").cast("int"))

    df = df.withColumn('split_parts', F.split(F.col("location"), ", "))

    # Null checks
    null_amount = null_check(df)
    logger.info(f'Check missing values for USERS data: {null_amount}\n')
     

    # df = split_location(df, "city", 0, Config.EXCEPTIONS_LIST)
    # df = split_location(df, "region", 1, Config.EXCEPTIONS_LIST)
    # df = split_location(df, "country", 2, Config.EXCEPTIONS_LIST)
    df = split_location(df)

    df = df.drop(F.col("split_parts"))
    df = df.withColumn("ingested_at", F.current_timestamp())

    df.show()

    logger.info(f"Writing transformed USERS data from {input_path} source to: {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")

    process_users(spark, input_path, output_path)

# --- 6. Stop Spark ---
spark.stop()



# --- 5. Manipulations ---
# Example: select only a few columns
# books = df.select("Book-Title", "Book-Author", "Year-Of-Publication")

# # Filter: books published after 2000
# recent_books = books.filter(col("Year-Of-Publication") > 2000)

# # Group: count books per author
# author_counts = books.groupBy("Book-Author").count().orderBy(col("count").desc())

# # --- 6. Save Results ---
# recent_books.write.mode("overwrite").csv(output_path + "/recent_books", header=True)
# author_counts.write.mode("overwrite").csv(output_path + "/author_counts", header=True)
