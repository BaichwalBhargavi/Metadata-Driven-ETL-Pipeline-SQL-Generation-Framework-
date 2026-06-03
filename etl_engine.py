from pyspark.sql import SparkSession
from SQLBuilder import SQLBuilder

def run_metadata_etl(spark: SparkSession, metadata: dict):
    # 1. Initialize Spark Session
    
    print(f"Starting ETL Job for: {metadata['table_name']}")
    
    try:
        # 2. Extract: Load the source file dynamically
       
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(metadata["source_path"])
        
        # 3. Register the DataFrame as a temporary SQL view
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
    
       # 6. To return for further dev 
        return result_df

    except Exception as e:
        print(f"An error occurred during ETL: {e}")
        return None
    