import os 
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from etl_engine import * 

# Load secret environment variables from .env
load_dotenv()

# 1. Setup Java Environment Variables
os.environ["JAVA_HOME"] = r"C:\Users\HP\Downloads\openlogic-openjdk-17.0.17+10-windows-x64\openlogic-openjdk-17.0.17+10-windows-x64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["HADOOP_HOME"] = r"C:\Users\HP\Hadoop"

if __name__ == "__main__":

    # 1. Initialize Spark Session with the MySQL JDBC Driver path
    spark_session = SparkSession.builder \
        .appName("MetadataDrivenETL") \
        .config("spark.jars", os.getenv("JAR_PATH")) \
        .config("spark.speculation", "false") \
        .getOrCreate()

    # 2. Extract database connection parameters from environment
    jdbc_url = os.getenv("DB_URL")
    connection_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    
    try:
        # 3. Read the metadata matrix from MySQL
        print("Connecting to MySQL metadata repository...")
        metadata_df = spark_session.read.jdbc(
            url=jdbc_url, 
            table="etl_config", 
            properties=connection_properties
        )

        # Grab the first configuration row
        db_row = metadata_df.first()

        if db_row:
            # 4. Dynamically generate the source path using OS-safe rules
            source_file_name = f"{db_row['source_file_name']}.{db_row['source_file_type']}"
            destination_file_name = f"{db_row['output_file_name']}"
            dynamic_source_path = os.path.join(db_row['source_dir'], source_file_name)
            dynamic_destination_path = os.path.join(db_row['destination_dir'], destination_file_name)
            
            print(f"Dynamically generated file path: {dynamic_source_path}")
            print(f"Dynamically generated file path: {dynamic_destination_path}")

        # 5. Assemble the payload required by your engine
            metadata_payload = {
                "source_path": dynamic_source_path,
                "file_type": db_row["source_file_type"],
                "table_name": db_row["table_name"],
                "columns": [col.strip() for col in db_row["columns"].split(",")],
                "destination_path":dynamic_destination_path,
                "destination_file_type":db_row['output_file_type'],
                "where_clause": db_row["where_clause"] if db_row["where_clause"] else None
            }

        # 6. Run your ETL Engine
            print(f"Starting ETL engine execution for: {metadata_payload['table_name']}\n")
            output_df = run_metadata_etl(spark_session, metadata_payload)
            
        else:
            print("Execution halted: No records discovered inside 'etl_config' table.")
            
    except Exception as e:
        print(f"Critical error encountered in master pipeline: {e}")
        
    finally:
        # 7. Safely tear down the Spark session
        print("Closing Spark context application session safely.")
        spark_session.stop()