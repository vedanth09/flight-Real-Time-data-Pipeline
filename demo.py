import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replace with your actual API key (WITHOUT THE SPACE)
api_key = "5f169766214ddc030b539c0d76c3779a122047edaa4b7ae4af68463e"

# API endpoint for the test system
url = "https://www.vrn.de/service/entwickler/trias-test/"

# Request parameters (you might need to adjust these)
params = {
    "outputFormat": "application/json",
    "itdDate": "20250502",
    "itdTime": "090000",
    "type_origin": "stop",
    "name_origin": "Heidelberg Hbf",
    "type_destination": "stop",
    "name_destination": "Mannheim Hbf",
    "numberOfResults": "2"
}

# Set the Authorization header
headers = {
    "Authorization": f"Apikey {api_key}"
}

try:
    logging.info(f"Sending GET request to: {url}")
    logging.info(f"Request parameters: {params}")
    logging.info(f"Request headers: {headers}")

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    logging.info(f"Response Status Code: {response.status_code}")

    if response.status_code == 200 and params.get("outputFormat") == "application/json":
        try:
            data = json.loads(response.text)
            logging.info("Parsed JSON Response:")
            logging.info(json.dumps(data, indent=4))
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON response: {e}")
            logging.info(f"Raw response: {response.text}")
    else:
        logging.info("Response Content:")
        logging.info(response.text)

except requests.exceptions.RequestException as e:
    logging.error(f"An error occurred during the request: {e}")
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")