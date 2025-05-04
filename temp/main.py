import requests
import json
from google.cloud import storage  # To interact with GCS

# Initialize the GCS client. Google Cloud Functions will automatically authenticate using the IAM role.
storage_client = storage.Client()

def fetch_flight_data():
    # Define the API endpoint and parameters
    url = "http://api.aviationstack.com/v1/flights"
    params = {
        "access_key": "63c376e09b8f45e7b1ff843ab2b40fa8"  # Use your actual API key here
    }

    try:
        print("Collecting data... Please wait.")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()

        print("API Request Successful")

        # Prepare live data for processing
        flight_data = []
        for flight in data.get("data", []):
            departure = flight.get("departure", {})
            arrival = flight.get("arrival", {})
            airline = flight.get("airline", {})
            flight_info = flight.get("flight", {})
            aircraft = flight.get("aircraft", {})
            live = flight.get("live", {})

            flight_data.append({
                "flight_date": flight.get("flight_date", "N/A"),
                "flight_status": flight.get("flight_status", "N/A"),
                "airline_name": airline.get("name", "N/A"),
                "airline_iata": airline.get("iata", "N/A"),
                "airline_icao": airline.get("icao", "N/A"),
                "flight_number": flight_info.get("number", "N/A"),
                "flight_iata": flight_info.get("iata", "N/A"),
                "flight_icao": flight_info.get("icao", "N/A"),
                "flight_codeshared": flight_info.get("codeshared", "N/A"),
                # Departure information
                "departure_airport": departure.get("airport", "N/A"),
                "departure_iata": departure.get("iata", "N/A"),
                "departure_icao": departure.get("icao", "N/A"),
                "departure_timezone": departure.get("timezone", "N/A"),
                "departure_terminal": departure.get("terminal", "N/A"),
                "departure_gate": departure.get("gate", "N/A"),
                "departure_delay": departure.get("delay", "N/A"),
                "departure_scheduled": departure.get("scheduled", "N/A"),
                "departure_estimated": departure.get("estimated", "N/A"),
                "departure_actual": departure.get("actual", "N/A"),
                "departure_estimated_runway": departure.get("estimated_runway", "N/A"),
                "departure_actual_runway": departure.get("actual_runway", "N/A"),
                # Arrival information
                "arrival_airport": arrival.get("airport", "N/A"),
                "arrival_iata": arrival.get("iata", "N/A"),
                "arrival_icao": arrival.get("icao", "N/A"),
                "arrival_timezone": arrival.get("timezone", "N/A"),
                "arrival_terminal": arrival.get("terminal", "N/A"),
                "arrival_gate": arrival.get("gate", "N/A"),
                "arrival_baggage": arrival.get("baggage", "N/A"),
                "arrival_scheduled": arrival.get("scheduled", "N/A"),
                "arrival_estimated": arrival.get("estimated", "N/A"),
                "arrival_actual": arrival.get("actual", "N/A"),
                "arrival_delay": arrival.get("delay", "N/A"),
                "arrival_estimated_runway": arrival.get("estimated_runway", "N/A"),
                "arrival_actual_runway": arrival.get("actual_runway", "N/A"),
                # Aircraft information
                "aircraft_registration": aircraft["registration"] if aircraft else "N/A",
                "aircraft_iata": aircraft["iata"] if aircraft else "N/A",
                "aircraft_icao": aircraft["icao"] if aircraft else "N/A",
                "aircraft_icao24": aircraft["icao24"] if aircraft else "N/A",
                # Live flight data (if exists)
                "live_updated": live.get("updated", "N/A") if live else "N/A",
                "live_latitude": live.get("latitude", "N/A") if live else "N/A",
                "live_longitude": live.get("longitude", "N/A") if live else "N/A",
                "live_altitude": live.get("altitude", "N/A") if live else "N/A",
                "live_direction": live.get("direction", "N/A") if live else "N/A",
                "live_speed_horizontal": live.get("speed_horizontal", "N/A") if live else "N/A",
                "live_speed_vertical": live.get("speed_vertical", "N/A") if live else "N/A",
                "live_is_ground": live.get("is_ground", "N/A") if live else "N/A"
            })

        return flight_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def upload_to_gcs(data, bucket_name, file_name):
    # Upload flight data to GCS as JSON
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Check if the blob already exists
    if blob.exists():
        existing_data = json.loads(blob.download_as_text())
        existing_data.extend(data)  # Append the new data
    else:
        existing_data = data  # If file doesn't exist, initialize with new data

    # Upload the combined data to GCS
    blob.upload_from_string(json.dumps(existing_data), content_type='application/json')
    print(f"Data uploaded to GCS: gs://{bucket_name}/{file_name}")

def main():
    data = fetch_flight_data()
    
    if data:
        # Upload the fetched data to GCS
        bucket_name = "flights_live_data"
        file_name = "live_flight_data.json"
        upload_to_gcs(data, bucket_name, file_name)

if __name__ == "__main__":
    main()