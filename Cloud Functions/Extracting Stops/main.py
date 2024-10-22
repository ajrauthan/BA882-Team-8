import requests
from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

# MBTA API endpoint for stops
stops_url = "https://api-v3.mbta.com/stops"
api_key = "844e75c1921b486aa93f856967ebe33c"

def fetch_and_insert_stops(request):
    """
    Cloud Function to fetch stops data from MBTA API and load into BigQuery.
    This function is triggered by an HTTP request.
    """
    # Construct the URL
    url = f"{stops_url}?api_key={api_key}"
    logging.info(f"Fetching data from URL: {url}")

    # Fetch data from the MBTA API
    try:
        response = requests.get(url)
        logging.info(f"API Status Code: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"Failed to fetch data from MBTA API. Status Code: {response.status_code}")
            logging.error(f"API Response: {response.text}")
            return f"API call failed with status code: {response.status_code}", 500
    except Exception as e:
        logging.error(f"Error during API request: {e}")
        return "API request failed", 500

    # Extract and log the raw API response
    try:
        data = response.json()
        logging.info(f"Raw API Data: {data}")
    except Exception as e:
        logging.error(f"Error parsing API response: {e}")
        return "Failed to parse API response", 500

    # Extract stops data
    stops_data = data.get('data', [])
    if not stops_data:
        logging.error("No stops data found in the API response.")
        return "No stops data found", 500

    # Prepare list for stops table
    stops = []
    try:
        for stop in stops_data:
            logging.info(f"Processing raw stop data: {stop}")  # Log the raw stop data

            # Use a try-except block for each stop to catch and handle missing fields
            try:
                stop_attributes = stop.get('attributes', {})
                relationships = stop.get('relationships', {})
                links = stop.get('links', {})

                stops.append({
                    'stop_id': stop.get('id'),
                    'name': stop_attributes.get('name') if stop_attributes.get('name') else 'Unknown',  # Handle missing names
                    'description': stop_attributes.get('description', ''),  # Default to empty string if missing
                    'latitude': stop_attributes.get('latitude') if stop_attributes.get('latitude') is not None else 0.0,  # Default to 0.0 if missing
                    'longitude': stop_attributes.get('longitude') if stop_attributes.get('longitude') is not None else 0.0,  # Default to 0.0 if missing
                    'municipality': stop_attributes.get('municipality', 'Unknown'),  # Default to 'Unknown' if missing
                    'location_type': stop_attributes.get('location_type') if stop_attributes.get('location_type') is not None else -1,  # Default to -1 if missing
                    'wheelchair_boarding': stop_attributes.get('wheelchair_boarding') if stop_attributes.get('wheelchair_boarding') is not None else -1,  # Default to -1 if missing
                    'parent_station_id': relationships.get('parent_station', {}).get('data', {}).get('id') if relationships.get('parent_station') else None,
                    'zone_id': relationships.get('zone', {}).get('data', {}).get('id') if relationships.get('zone') else None,
                    'self_link': links.get('self', '')  # Default to empty string if missing
                })
            except Exception as e:
                logging.error(f"Error processing stop ID {stop.get('id')}: {e}")
                continue  # Skip this stop and continue processing others

    except Exception as e:
        logging.error(f"Failed to process stop data: {e}")
        return "Failed to process stop data", 500

    logging.info(f"Processed Stops Data: {stops}")

    # Insert data into the stops table in BigQuery
    try:
        errors = client.insert_rows_json("ba882-team8-fall24.mbta_dataset.stops", stops)
        if errors:
            logging.error(f"Errors inserting into stops table: {errors}")
            return f"Insertion failed with errors: {errors}", 500
    except Exception as e:
        logging.error(f"BigQuery insert failed: {e}")
        return "BigQuery insertion failed", 500

    logging.info("Data successfully inserted into BigQuery stops table.")
    return "Stops data successfully inserted into BigQuery", 200
