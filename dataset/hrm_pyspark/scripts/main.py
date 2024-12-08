from pyspark.sql import SparkSession
from scripts.config import DATA_PATH, OUTPUT_PATH
from scripts.preprocess import preprocess_data
from scripts.transformations import apply_transformations
from scripts.analysis import perform_analysis

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV File Management") \
        .getOrCreate()

    # Preprocess data
    file1 = preprocess_data(spark, f"{DATA_PATH}/Traffic_Collisions_5599987132563638107.csv")
    file2 = preprocess_data(spark, f"{DATA_PATH}/Traffic_Collisions_Involved_Pedestrian_-6326215530466530587.csv")
    file3 = preprocess_data(spark, f"{DATA_PATH}/Traffic_Collisions_Involved_Vehicle_-2623639782137125921.csv") 

    # Transform data
    transformed_data = apply_transformations(file1, file2, file3)

    # Perform analysis
    perform_analysis(transformed_data, OUTPUT_PATH)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
