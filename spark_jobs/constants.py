from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class Config:
    ENCODING_FIXES = {
                        "í²": "ò", "í¨": "è", "í¡": "à",
                        "Ã\\?Â©": "é", "Ã\\?Â": "à", "Ã\\?Â¨": "è",
                        "Ã\\?Âª": "ê", "Ã\\?Â«": "ë", "Ã\\?Â´": "ô",
                        "Ã\\?Â®": "î", "Ã\\?Â¯": "ï", "Ã\\?Â¹": "ù",
                        "Ã\\?Â§": "ç", "Ã³": "ó", "Ã±": "ñ",
                        "Ã¡": "á", "Ã©": "é", "Ã": "í"                    
                    }

    EXCEPTIONS_LIST = ["ny", "nyc", "la", "dc", "sf", "usa", "uk", "uae", "eu", "u.a.e"]

    # --- PATHS ---
    # Ensure these names match EXACTLY what is in your GCS bucket
    INPUT_PATH_BOOKS = "gs://kestra-books-bucket-latypov/raw/Books.csv"
    OUTPUT_PATH_BOOKS = "gs://kestra-books-bucket-latypov/pyspark_transformed/books" 

    INPUT_PATH_USERS = "gs://kestra-books-bucket-latypov/raw/Users.csv"
    OUTPUT_PATH_USERS = "gs://kestra-books-bucket-latypov/pyspark_transformed/users" 

    # FIXED: Changed Rating.csv to Ratings.csv
    INPUT_PATH_RATING = "gs://kestra-books-bucket-latypov/raw/Ratings.csv"
    OUTPUT_PATH_RATING = "gs://kestra-books-bucket-latypov/pyspark_transformed/ratings"

    CSV_OPTIONS = {
        "header": True,
        "inferSchema": False,
        "multiLine": True,
        "quote": '"',
        "escape": '"'
    }




class Schemas:
    BOOKS_SCHEMA = StructType([
        StructField("ISBN", StringType(), True),
        StructField("title", StringType(), True), # We name it 'title' immediately
        StructField("author", StringType(), True),
        StructField("year", StringType(), True),   # Read as string first because of messy data
        StructField("publisher", StringType(), True),
        StructField("image_url_small", StringType(), True),
        StructField("image_url_medium", StringType(), True),
        StructField("image_url_large", StringType(), True)
    ])

    USERS_SCHEMA = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    RATINGS_SCHEMA = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("ISBN", StringType(), True),
        StructField("book_rating", IntegerType(), True)
    ])