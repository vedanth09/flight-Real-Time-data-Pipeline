# data_collection.py

import requests
import csv
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the API key from the .env file
api_key = os.getenv("AVIATIONSTACK_API_KEY")

# Base URL for AviationStack API
base_url = "http://api.aviationstack.com/v1/"

# Define the endpoint (example: current flights)
endpoint = "flights"

# Construct the full URL for the request
url = f"{base_url}{endpoint}?access_key={api_key}"

# Function to save data to a CSV file
def save_to_csv(data, filename="collected_flight_data.csv"):
    # Open the CSV file for writing
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())  # Use the keys of the first dictionary as headers
        writer.writeheader()  # Write the header (column names)
        writer.writerows(data)  # Write the data rows
    
    print(f"Data has been saved to {filename}")

# Function to fetch and collect flight data
def fetch_flight_data():
    try:
        print("Collecting data... Please wait.")
        
        # Send the GET request to the API
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            print("API Request Successful")
            data = response.json()  # Convert response to JSON
            
            # Check if data exists
            if "data" in data:
                flight_data = data["data"]  # Extract flight data from the response
                save_to_csv(flight_data)  # Save the flight data to CSV
            else:
                print("No flight data found in the response.")
        else:
            print(f"Error: {response.status_code} - {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

# Run the function to fetch and save data
fetch_flight_data()
