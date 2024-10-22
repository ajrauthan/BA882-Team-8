import requests
from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

# MBTA API endpoint for vehicles
vehicles_url = "https://api-v3.mbta.com/vehicles"
api_key = "844e75c1921b486aa93f856967ebe33c"  # Replace with your actual MBTA API Key

def fetch_and_insert_vehicles(request):
    """
    Cloud Function to fetch vehicles data from MBTA API and load into BigQuery.
    This function is triggered by an HTTP request.
    """
    # Construct the URL
    url = f"{vehicles_url}?api_key={api_key}"
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

    # Extract vehicles data
    vehicles_data = data.get('data', [])
    if not vehicles_data:
        logging.error("No vehicles data found in the API response.")
        return "No vehicles data found", 500

    # Prepare list for vehicles table
    vehicles = []
    try:
        for vehicle in vehicles_data:
            logging.info(f"Processing raw vehicle data: {vehicle}")  # Log the raw vehicle data

            # Use a try-except block for each vehicle to catch and handle missing fields
            try:
                vehicle_attributes = vehicle.get('attributes', {})
                relationships = vehicle.get('relationships', {})

                vehicles.append({
                    'vehicle_id': vehicle.get('id'),
                    'label': vehicle_attributes.get('label', 'Unknown'),  # Handle missing labels
                    'type': vehicle.get('type', 'Unknown'),  # Handle missing types
                    'revenue': vehicle_attributes.get('revenue', 'Unknown'),  # Default to 'Unknown'
                    'direction_id': vehicle_attributes.get('direction_id') if vehicle_attributes.get('direction_id') is not None else -1,  # Default to -1 if missing
                    'bearing': vehicle_attributes.get('bearing') if vehicle_attributes.get('bearing') is not None else 0,  # Default to 0 if missing
                    'current_status': vehicle_attributes.get('current_status', 'Unknown'),  # Handle missing status
                    'current_stop_sequence': vehicle_attributes.get('current_stop_sequence') if vehicle_attributes.get('current_stop_sequence') is not None else -1,  # Default to -1
                    'latitude': vehicle_attributes.get('latitude') if vehicle_attributes.get('latitude') is not None else 0.0,  # Default to 0.0
                    'longitude': vehicle_attributes.get('longitude') if vehicle_attributes.get('longitude') is not None else 0.0,  # Default to 0.0
                    'occupancy_status': vehicle_attributes.get('occupancy_status', 'Unknown'),  # Default to 'Unknown'
                    'speed': vehicle_attributes.get('speed') if vehicle_attributes.get('speed') is not None else 0.0,  # Default to 0.0
                    'updated_at': vehicle_attributes.get('updated_at', '1970-01-01T00:00:00Z'),  # Default to epoch if missing
                    'route_id': relationships.get('route', {}).get('data', {}).get('id') if relationships.get('route') else None,
                    'stop_id': relationships.get('stop', {}).get('data', {}).get('id') if relationships.get('stop') else None,
                    'trip_id': relationships.get('trip', {}).get('data', {}).get('id') if relationships.get('trip') else None
                })
            except Exception as e:
                logging.error(f"Error processing vehicle ID {vehicle.get('id')}: {e}")
                continue  # Skip this vehicle and continue processing others

    except Exception as e:
        logging.error(f"Failed to process vehicle data: {e}")
        return "Failed to process vehicle data", 500

    logging.info(f"Processed Vehicles Data: {vehicles}")

    # Insert data into the vehicles table in BigQuery
    try:
        errors = client.insert_rows_json("ba882-team8-fall24.mbta_dataset.vehicles", vehicles)
        if errors:
            logging.error(f"Errors inserting into vehicles table: {errors}")
            return f"Insertion failed with errors: {errors}", 500
    except Exception as e:
        logging.error(f"BigQuery insert failed: {e}")
        return "BigQuery insertion failed", 500

    logging.info("Data successfully inserted into BigQuery vehicles table.")
    return "Vehicles data successfully inserted into BigQuery", 200
