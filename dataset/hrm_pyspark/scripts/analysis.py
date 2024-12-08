from pyspark.sql import DataFrame

def perform_analysis(df: DataFrame, output_path: str):
    # Example: Aggregation
    aggregated_df = df.groupBy("some_column").count()

    # Save results
    aggregated_df.write.csv(f"{output_path}/reports/aggregated_report.csv", header=True)
