from pyspark.sql import DataFrame

def preprocess_data(spark, file_path: str) -> DataFrame:
    # Load CSV into a DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Perform basic cleaning
    df = df.dropDuplicates().na.fill("Unknown")  # Example preprocessing

    return df
