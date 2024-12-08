from pyspark.sql import DataFrame

def apply_transformations(df1: DataFrame, df2: DataFrame, df3: DataFrame) -> DataFrame:
    # Example: Joining the datasets
    joined_df = df1.join(df2, on="common_column", how="inner") \
                   .join(df3, on="another_common_column", how="inner")

    # Add transformations (e.g., column computations)
    transformed_df = joined_df.withColumn("new_column", df1["column1"] + df2["column2"])

    return transformed_df
