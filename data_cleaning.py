import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FlightDataProcessing") \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("flight_date", StringType(), True),
    StructField("flight_status", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("airline_iata", StringType(), True),
    StructField("airline_icao", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("flight_iata", StringType(), True),
    StructField("flight_icao", StringType(), True),
    StructField("flight_codeshared", StringType(), True),
    StructField("departure_airport", StringType(), True),
    StructField("departure_iata", StringType(), True),
    StructField("departure_icao", StringType(), True),
    StructField("departure_timezone", StringType(), True),
    StructField("departure_terminal", StringType(), True),
    StructField("departure_gate", StringType(), True),
    StructField("departure_delay", StringType(), True),
    StructField("departure_scheduled", TimestampType(), True),
    StructField("departure_estimated", StringType(), True),
    StructField("departure_actual", StringType(), True),
    StructField("departure_estimated_runway", StringType(), True),
    StructField("departure_actual_runway", StringType(), True),
    StructField("arrival_airport", StringType(), True),
    StructField("arrival_iata", StringType(), True),
    StructField("arrival_icao", StringType(), True),
    StructField("arrival_timezone", StringType(), True),
    StructField("arrival_terminal", StringType(), True),
    StructField("arrival_gate", StringType(), True),
    StructField("arrival_baggage", StringType(), True),
    StructField("arrival_scheduled", TimestampType(), True),
    StructField("arrival_estimated", StringType(), True),
    StructField("arrival_actual", StringType(), True),
    StructField("arrival_delay", StringType(), True),
    StructField("arrival_estimated_runway", StringType(), True),
    StructField("arrival_actual_runway", StringType(), True),
    StructField("aircraft_registration", StringType(), True),
    StructField("aircraft_iata", StringType(), True),
    StructField("aircraft_icao", StringType(), True),
    StructField("aircraft_icao24", StringType(), True),
    StructField("live_updated", StringType(), True),
    StructField("live_latitude", FloatType(), True),
    StructField("live_longitude", FloatType(), True),
    StructField("live_altitude", StringType(), True),
    StructField("live_direction", StringType(), True),
    StructField("live_speed_horizontal", StringType(), True),
    StructField("live_speed_vertical", StringType(), True),
    StructField("live_is_ground", StringType(), True)
])

# Clean live data using PySpark
def clean_live_data(live_data):
    # Create a PySpark DataFrame from the live data using the defined schema
    live_data_df = spark.createDataFrame(live_data, schema)

    # Remove rows where any column has a null value
    live_data_df = live_data_df.dropna()

    # Remove timezone part using regex and convert 'departure_scheduled' and 'arrival_scheduled' to timestamp
    live_data_df = live_data_df.withColumn(
        "departure_scheduled", 
        F.to_timestamp(F.regexp_replace(F.col("departure_scheduled"), r"\+.*", ""), "yyyy-MM-dd'T'HH:mm:ss")
    )
    live_data_df = live_data_df.withColumn(
        "arrival_scheduled", 
        F.to_timestamp(F.regexp_replace(F.col("arrival_scheduled"), r"\+.*", ""), "yyyy-MM-dd'T'HH:mm:ss")
    )

    # Cast latitude and longitude to float
    live_data_df = live_data_df.withColumn("live_latitude", live_data_df["live_latitude"].cast("float"))
    live_data_df = live_data_df.withColumn("live_longitude", live_data_df["live_longitude"].cast("float"))
    
    return live_data_df

# Write cleaned live data to Google Cloud Storage (GCS)
def write_to_gcs(cleaned_data_df, bucket_name, file_name):
    cleaned_data_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(f"gs://{bucket_name}/{file_name}")
    print(f"Data written to GCS: gs://{bucket_name}/{file_name}")

# Main function
def main():
    # Assuming live_data is passed as a JSON string
    if len(sys.argv) < 2:
        print("No data received from data_collection.py")
        return
    
    live_data = json.loads(sys.argv[1])  # Now we load the JSON string passed from subprocess
    
    # Clean live data using PySpark
    cleaned_live_data_df = clean_live_data(live_data)

    # Write the cleaned data to GCS
    bucket_name = "flights_live_data"  # Your GCS bucket name
    file_name = "cleaned_live_flight_data.parquet"
    write_to_gcs(cleaned_live_data_df, bucket_name, file_name)

if __name__ == "__main__":
    main()
