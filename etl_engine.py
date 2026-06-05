from pyspark.sql import SparkSession
from SQLBuilder import SQLBuilder
import os
def run_metadata_etl(spark: SparkSession, metadata: dict):
    print(f"Starting ETL Job for: {metadata['table_name']}")
    
    try:
        # Extract: Load the source file dynamically based on file_type (csv, parquet, json, etc.)
        df = spark.read.format(metadata["file_type"]) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(metadata["source_path"])
        
        # Register the DataFrame as a temporary SQL view
        df.createOrReplaceTempView(metadata["table_name"])
        
        # SQL query builder 
        builder = (SQLBuilder()
                   .select(metadata["columns"])
                   .from_table(metadata["table_name"]))
        
        if metadata.get("where_clause"):
            builder = builder.where(metadata["where_clause"])
            
        generated_query = builder.get_query()
        
        print(f"Executing Generated Query: {generated_query}")
        result_df = spark.sql(generated_query)

        # 5. Load/Output: Show the results (or save them)
        result_df.show()
        
        destination_path = metadata["destination_path"]

        # Ensure it doesn't have a weird trailing slash or malformed prefix
        
        # Optional: Forcing a clean local file URI protocol often completely bypasses Windows path bugs
        
        result_df.write.format(metadata["destination_file_type"]) \
        .option("header", "true") \
        .mode("overwrite") \
        .save(destination_path)
    
       # 6. To return for further dev 
        return result_df

    except Exception as e:
        print(f"An error occurred during ETL: {str (e)}")
        return None
    