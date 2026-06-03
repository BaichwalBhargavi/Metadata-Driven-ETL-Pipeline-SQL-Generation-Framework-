import os 
import sys

# 1. Setup Java Environment Variables
os.environ["JAVA_HOME"] = r"C:\Users\HP\Downloads\openlogic-openjdk-17.0.17+10-windows-x64\openlogic-openjdk-17.0.17+10-windows-x64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from etl_engine import run_metadata_etl  # Import your ETL function here!

if __name__ == "__main__":
    # 2. Initialize Spark Session globally
    spark_session = SparkSession.builder \
        .appName("MetadataDrivenETL") \
        .getOrCreate()

    # 3. Define the configuration/metadata payload
    mock_database_row = {
        "source_path": "data/social_media_usage.csv",  
        "table_name": "social_media_usage",
        "columns": ["user_id", "gender", "daily_screen_time_hours", "addiction_risk_level"],
        "where_clause": "addiction_risk_level = 'Low'"
    }
    
    try:
        # 4. Run the engine by passing dependencies
        output_df = run_metadata_etl(spark_session, mock_database_row)
        
    finally:
        # 5. Make sure to safely close the Spark application session
        spark_session.stop()