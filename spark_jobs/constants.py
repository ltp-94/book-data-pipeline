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

    EXCEPTIONS_LIST = ["ny", "nyc", "la", "dc", "sf", "usa", "uk", "uae", "eu", "u.a.e"]

    NOISE_TOKENS = ["n/a", "null", "none", "unknown", "", "n/a,"]

    USA_VARIANTS = ["united states", "united states of america", "united staets of america", "us", "u.s.a", "usa"]

    UK_VARIANTS  = ["united kingdom", "great britain", "england", "uk"]

    UAE_VARIANTS = ["uae", "u.a.e", "united arab emirates"]

    VALID_COUNTRIES = [
    # North America & Caribbean
    "usa", "united states", "u.s.a.", "united states of america", "canada", "mexico", 
    "cuba", "jamaica", "bahamas", "barbados", "dominican republic", "haiti", "puerto rico", 
    "trinidad and tobago", "costa rica", "guatemala", "honduras", "el salvador", "nicaragua", 
    "panama", "belize", "greenland", "bermuda",

    # South America
    "brazil", "argentina", "chile", "colombia", "peru", "venezuela", "ecuador", 
    "bolivia", "paraguay", "uruguay", "guyana", "suriname",

    # Europe
    "uk", "united kingdom", "u.k.", "england", "scotland", "wales", "northern ireland", 
    "ireland", "germany", "france", "italy", "spain", "portugal", "netherlands", 
    "belgium", "austria", "switzerland", "sweden", "norway", "denmark", "finland", 
    "iceland", "poland", "czech republic", "slovakia", "hungary", "romania", "bulgaria", 
    "greece", "turkey", "russia", "ukraine", "belarus", "moldova", "estonia", "latvia", 
    "lithuania", "croatia", "serbia", "slovenia", "bosnia and herzegovina", "montenegro", 
    "albania", "north macedonia", "kosovo", "luxembourg", "malta", "cyprus", "monaco", 
    "andorra", "liechtenstein", "san marino", "vatican city", "faroe islands",

    # Asia
    "china", "japan", "south korea", "north korea", "india", "pakistan", "bangladesh", 
    "sri lanka", "nepal", "bhutan", "maldives", "taiwan", "hong kong", "macau", 
    "vietnam", "thailand", "philippines", "malaysia", "singapore", "indonesia", 
    "myanmar", "burma", "cambodia", "laos", "brunei", "east timor", "kazakhstan", 
    "uzbekistan", "turkmenistan", "kyrgyzstan", "tajikistan", "mongolia", "afghanistan",

    # Middle East
    "israel", "palestine", "iran", "iraq", "saudi arabia", "uae", "united arab emirates", 
    "u.a.e.", "qatar", "kuwait", "oman", "jordan", "lebanon", "syria", "yemen", 
    "bahrain", "armenia", "azerbaijan", "georgia",

    # Africa
    "egypt", "morocco", "algeria", "tunisia", "libya", "south africa", "nigeria", 
    "kenya", "ghana", "ethiopia", "tanzania", "uganda", "sudan", "south sudan", 
    "congo", "dr congo", "ivory coast", "cote d'ivoire", "senegal", "cameroon", 
    "angola", "zimbabwe", "zambia", "botswana", "namibia", "mozambique", "madagascar", 
    "mauritius", "seychelles", "rwanda", "burundi", "somalia", "eritrea", "djibouti", 
    "mali", "niger", "chad", "burkina faso", "guinea", "sierra loren", "liberia", 
    "togo", "benin", "gabon", "equatorial guinea", "gambia", "malawi", "lesotho", "swaziland", "eswatini",

    # Oceania
    "australia", "new zealand", "fiji", "papua new guinea", "samoa", "tonga", 
    "vanuatu", "solomon islands", "kiribati", "micronesia", "palau", "marshall islands", 
    "tuvalu", "nauru"
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