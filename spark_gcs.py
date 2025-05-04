from pyspark.sql import SparkSession

# Create a Spark session with the GCS connector and authentication
spark = SparkSession.builder \
    .appName('Clean Flight Data') \
    .config('spark.jars', 'gs://flights_live_data/gcs-connector-hadoop2-latest.jar') \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/vedanth/Desktop/data-management2-458610-6f1b53103d6b.json") \
    .getOrCreate()

# Now you can proceed with your Spark operations, such as reading from GCS or processing data
print("Spark session created successfully!")
