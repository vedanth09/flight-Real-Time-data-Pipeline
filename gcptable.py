from google.cloud import bigquery

def create_bigquery_table():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define the table ID
    dataset_id = "data-management2-458610.flight_data"  # Replace with your dataset ID
    table_id = f"{dataset_id}.flight_data_table"  # This will be the new table within the dataset

    # Define the schema for the table
    schema = [
        bigquery.SchemaField("flight_date", "STRING"),
        bigquery.SchemaField("flight_status", "STRING"),
        
        bigquery.SchemaField("airline_name", "STRING"),
        bigquery.SchemaField("airline_iata", "STRING"),
        bigquery.SchemaField("airline_icao", "STRING"),
        
        bigquery.SchemaField("flight_number", "STRING"),
        bigquery.SchemaField("flight_iata", "STRING"),
        bigquery.SchemaField("flight_icao", "STRING"),
        
        bigquery.SchemaField("departure_airport", "STRING"),
        bigquery.SchemaField("departure_iata", "STRING"),
        bigquery.SchemaField("departure_icao", "STRING"),
        bigquery.SchemaField("departure_timezone", "STRING"),
        bigquery.SchemaField("departure_terminal", "STRING"),
        bigquery.SchemaField("departure_gate", "STRING"),
        bigquery.SchemaField("departure_delay", "INTEGER"),
        bigquery.SchemaField("departure_scheduled", "TIMESTAMP"),
        bigquery.SchemaField("departure_estimated", "TIMESTAMP"),
        bigquery.SchemaField("departure_actual", "TIMESTAMP"),
        
        bigquery.SchemaField("arrival_airport", "STRING"),
        bigquery.SchemaField("arrival_iata", "STRING"),
        bigquery.SchemaField("arrival_icao", "STRING"),
        bigquery.SchemaField("arrival_timezone", "STRING"),
        bigquery.SchemaField("arrival_terminal", "STRING"),
        bigquery.SchemaField("arrival_gate", "STRING"),
        bigquery.SchemaField("arrival_baggage", "STRING"),
        bigquery.SchemaField("arrival_scheduled", "TIMESTAMP"),
        bigquery.SchemaField("arrival_estimated", "TIMESTAMP"),
        bigquery.SchemaField("arrival_actual", "TIMESTAMP"),
        bigquery.SchemaField("arrival_delay", "INTEGER"),
        
        bigquery.SchemaField("aircraft_registration", "STRING"),
        bigquery.SchemaField("aircraft_iata", "STRING"),
        bigquery.SchemaField("aircraft_icao", "STRING"),
        bigquery.SchemaField("aircraft_icao24", "STRING"),
        
        bigquery.SchemaField("live_updated", "TIMESTAMP"),
        bigquery.SchemaField("live_latitude", "FLOAT"),
        bigquery.SchemaField("live_longitude", "FLOAT"),
        bigquery.SchemaField("live_altitude", "FLOAT"),
        bigquery.SchemaField("live_direction", "FLOAT"),
        bigquery.SchemaField("live_speed_horizontal", "FLOAT"),
        bigquery.SchemaField("live_speed_vertical", "FLOAT"),
        bigquery.SchemaField("live_is_ground", "BOOLEAN")
    ]

    # Create the table schema
    table = bigquery.Table(table_id, schema=schema)

    # Create the table in BigQuery
    table = client.create_table(table)  # Make an API request.
    print(f"Table {table.table_id} created successfully.")

if __name__ == "__main__":
    create_bigquery_table()
