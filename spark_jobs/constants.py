from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

class Config:
    ENCODING_FIXES = {
                        "í²": "ò", "í¨": "è", "í¡": "à",
                        "Ã\\?Â©": "é", "Ã\\?Â": "à", "Ã\\?Â¨": "è",
                        "Ã\\?Âª": "ê", "Ã\\?Â«": "ë", "Ã\\?Â´": "ô",
                        "Ã\\?Â®": "î", "Ã\\?Â¯": "ï", "Ã\\?Â¹": "ù",
                        "Ã\\?Â§": "ç", "Ã³": "ó", "Ã±": "ñ",
                        "Ã¡": "á", "Ã©": "é", "Ã": "í"                    
                    }

    # Basic Noise
    NOISE_TOKENS = ["n/a", "null", "none", "unknown", "", " ", "n/a,"]
    EXCEPTIONS_LIST = ["ny", "nyc", "la", "dc", "sf", "usa", "uk", "uae", "eu"]

    # This mapping ensures U.s.a. and U.k. (with periods) are caught!
    # We lowercase them here because we will clean the data to lowercase before joining.
    COUNTRY_MAPPING = {
        "usa": "USA", "united states": "USA", "united states of america": "USA", 
        "u.s.a.": "USA", "u.s.a": "USA", "us": "USA",
        "united kingdom": "UK", "uk": "UK", "u.k.": "UK", "u.k": "UK",
        "england": "UK", "scotland": "UK", "wales": "UK", "northern ireland": "UK",
        "united arab emirates": "UAE", "uae": "UAE", "u.a.e.": "UAE", "u.a.e": "UAE"
    }

    # The list of standard countries you provided
    VALID_COUNTRIES = [
        "Canada", "Germany", "Spain", "Australia", "Italy", "France", "Portugal", 
        "New Zealand", "Netherlands", "Switzerland", "Brazil", "China", "Sweden", 
        "India", "Austria", "Malaysia", "Argentina", "Finland", "Singapore", 
        "Denmark", "Mexico", "Belgium", "Ireland", "Philippines", "Turkey", 
        "Poland", "Pakistan", "Greece", "Iran", "Romania", "Chile", "Israel", 
        "South Africa", "Indonesia", "Norway", "Japan", "Croatia", "Nigeria", 
        "South Korea", "Slovakia", "Czech Republic", "Russia", "Hong Kong", 
        "Costa Rica", "Taiwan", "Slovenia", "Egypt", "Vietnam", "Peru", 
        "Venezuela", "Colombia", "Bulgaria", "Luxembourg", "Hungary", "Thailand", 
        "Ghana", "Saudi Arabia", "Bosnia And Herzegovina", "Sri Lanka", "Iceland", 
        "Bangladesh", "Paraguay", "Guatemala", "Andorra", "Ukraine", "Lithuania", 
        "Latvia", "Bahamas", "Bolivia", "Jamaica", "Kuwait", "Panama", "Ecuador", 
        "Cuba", "Lebanon", "Trinidad And Tobago", "Morocco", "Malta", "Albania", 
        "Afghanistan", "Dominican Republic", "Algeria", "Puerto Rico", "Honduras", 
        "Cyprus", "Kenya", "El Salvador", "Bermuda", "Oman", "Uzbekistan", 
        "Zimbabwe", "Jordan", "Georgia", "Belize", "Estonia", "Mauritius", 
        "Nepal", "Uruguay", "Brunei", "Qatar", "Barbados", "Burma", "Nicaragua", 
        "Bahrain", "Iraq", "Belarus", "Benin", "Ethiopia", "Syria", "Fiji", 
        "Kazakhstan", "Mozambique", "Sudan", "Guyana", "Azerbaijan", "Eritrea", 
        "Armenia", "Uganda", "Moldova", "Papua New Guinea", "Yemen", "Tunisia", 
        "Monaco", "Cameroon", "Botswana", "Gabon", "Cambodia", "Laos", "Tanzania", 
        "Rwanda", "Burkina Faso", "Niger", "Togo", "Samoa", "Serbia", "Angola", 
        "Zambia", "Senegal", "Lesotho", "Libya", "Mongolia", "Suriname", "Kosovo", 
        "Congo", "North Korea", "Kyrgyzstan", "Namibia", "Macau", "Bhutan", 
        "Maldives", "Tajikistan", "Vanuatu", "San Marino", "Liberia", "Djibouti", 
        "Tonga", "Guinea", "Swaziland", "Malawi", "Haiti", "Solomon Islands", 
        "Myanmar", "Turkmenistan", "Micronesia", "Marshall Islands", "Palau", 
        "Chad", "Mali", "Vatican City", "Madagascar", "Faroe Islands", 
        "Palestine", "Somalia", "Kiribati"
    ]

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
        StructField("user_id", LongType(), True),
        StructField("location", StringType(), True),
        StructField("age", StringType(), True)
    ])

    RATINGS_SCHEMA = StructType([
        StructField("user_id", LongType(), True),
        StructField("ISBN", StringType(), True),
        StructField("book_rating", IntegerType(), True)
    ])