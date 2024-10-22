import requests
from google.cloud import bigquery
import logging
import math

# Initialize BigQuery client
client = bigquery.Client()

# MBTA API endpoint for trips
trips_url = "https://api-v3.mbta.com/trips?filter[route]=Red,Mattapan,Orange,Green-B,Green-C,Green-D,Green-E,Blue,741,742,743,751,749,746,CR-Fairmount,CR-Fitchburg,CR-Worcester,CR-Franklin,CR-Greenbush,CR-Haverhill,CR-Kingston,CR-Lowell,CR-Middleborough,CR-Needham,CR-Newburyport,CR-Providence,CR-Foxboro,Boat-F4,Boat-F1,Boat-EastBoston,Boat-Lynn,Boat-F6,747,708,1,4,7,8,9,10,11,14,15,16,17,18,19,21,22,23,24,26,28,29,30,31,32,33,34,34E,35,36,37,38,39,40,41,42,43,44,45,47,50,51,52,55,57,59,60,61,62,627,64,65,66,67,68,69,70,71,73,74,75,76,77,78,80,83,85,86,87,88,89,90,91,92,93,94,95,96,97,99,100,101,104,105,106,108,109,110,111,112,114,116,117,119,120,121,131,132,134,137,171,201,202,210,211,215,216,217,220,222,225,226,230,236,238,240,245,350,351,354,411,424,426,428,429,430,435,436,439,441,442,450,451,455,456,501,504,505,553,554,556,558,712,713,714,716"
api_key = "844e75c1921b486aa93f856967ebe33c"  # Replace with your actual MBTA API Key

def fetch_and_insert_trips(request):
    """
    Cloud Function to fetch trips data from MBTA API and load into BigQuery in batches.
    """
    # Construct the correct URL with API key
    url = f"{trips_url}&api_key={api_key}"
    logging.info(f"Fetching data from URL: {url}")
    
    # Fetch data from the MBTA API
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad HTTP responses (4xx/5xx)
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from MBTA API: {e}")
        return "API call failed", 500

    # Log the API status code and response for debugging
    logging.info(f"API Status Code: {response.status_code}")
    logging.info(f"API Response: {response.text}")

    data = response.json()

    # Extract trips data
    trips_data = data.get('data', [])

    if not trips_data:
        logging.error("No trips data found in the API response.")
        return "No trips data found", 500

    # Prepare list for trips table
    trips = []

    # Extract and transform data for each trip
    for trip in trips_data:
        try:
            trip_attributes = trip.get('attributes', {})
            relationships = trip.get('relationships', {})
            links = trip.get('links', {})

            trips.append({
                'trip_id': trip.get('id'),  # STRING
                'bikes_allowed': int(trip_attributes.get('bikes_allowed', 0)),  # INT64
                'block_id': trip_attributes.get('block_id', 'Unknown'),  # STRING
                'direction_id': int(trip_attributes.get('direction_id', -1)),  # INT64
                'headsign': trip_attributes.get('headsign', 'Unknown'),  # STRING
                'revenue': trip_attributes.get('revenue', 'Unknown'),  # STRING
                'wheelchair_accessible': int(trip_attributes.get('wheelchair_accessible', 0)),  # INT64
                'route_id': relationships.get('route', {}).get('data', {}).get('id') if relationships.get('route') else None,  # STRING
                'route_pattern_id': relationships.get('route_pattern', {}).get('data', {}).get('id') if relationships.get('route_pattern') else None,  # STRING
                'service_id': relationships.get('service', {}).get('data', {}).get('id') if relationships.get('service') else None,  # STRING
                'shape_id': relationships.get('shape', {}).get('data', {}).get('id') if relationships.get('shape') else None,  # STRING
                'self_link': links.get('self', 'Unknown')  # STRING
            })
        except Exception as e:
            logging.error(f"Error processing trip data: {e}")
            continue

    # Log the processed trips data for debugging
    logging.info(f"Processed Trips Data: {trips}")

    # Define batch size to avoid sending too much data in one go
    batch_size = 1000  # You can adjust this number as needed
    total_batches = math.ceil(len(trips) / batch_size)

    # Insert data in batches
    for i in range(total_batches):
        start = i * batch_size
        end = start + batch_size
        batch = trips[start:end]

        try:
            logging.info(f"Inserting batch {i+1}/{total_batches} with {len(batch)} trips")
            errors = client.insert_rows_json("ba882-team8-fall24.mbta_dataset.trips", batch)
            if errors:
                logging.error(f"Errors inserting batch {i+1}: {errors}")
                return f"Insertion failed with errors in batch {i+1}", 500
        except Exception as e:
            logging.error(f"BigQuery insert failed in batch {i+1}: {e}")
            return f"BigQuery insertion failed in batch {i+1}", 500

    logging.info("All batches successfully inserted into BigQuery trips table.")
    return "Trips data successfully inserted into BigQuery", 200