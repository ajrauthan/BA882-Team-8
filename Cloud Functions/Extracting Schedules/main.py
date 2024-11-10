import requests
from google.cloud import bigquery
import logging
import math

# Initialize BigQuery client
client = bigquery.Client()

# List of all routes
routes = [
    "Red", "Mattapan", "Orange", "Green-B", "Green-C", "Green-D", "Green-E", "Blue",
    "741", "742", "743", "751", "749", "746", "CR-Fairmount", "CR-Fitchburg", "CR-Worcester",
    "CR-Franklin", "CR-Greenbush", "CR-Haverhill", "CR-Kingston", "CR-Lowell",
    "CR-Middleborough", "CR-Needham", "CR-Newburyport", "CR-Providence", "CR-Foxboro",
    "Boat-F4", "Boat-F1", "Boat-EastBoston", "Boat-Lynn", "Boat-F6", "747", "708", "1", "4",
    "7", "8", "9", "10", "11", "14", "15", "16", "17", "18", "19", "21", "22", "23", "24", "26",
    "28", "29", "30", "31", "32", "33", "34", "34E", "35", "36", "37", "38", "39", "40", "41",
    "42", "43", "44", "45", "47", "50", "51", "52", "55", "57", "59", "60", "61", "62", "627",
    "64", "65", "66", "67", "68", "69", "70", "71", "73", "74", "75", "76", "77", "78", "80",
    "83", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "99",
    "100", "101", "104", "105", "106", "108", "109", "110", "111", "112", "114", "116", "117",
    "119", "120", "121", "131", "132", "134", "137", "171", "201", "202", "210", "211", "215",
    "216", "217", "220", "222", "225", "226", "230", "236", "238", "240", "245", "350", "351",
    "354", "411", "424", "426", "428", "429", "430", "435", "436", "439", "441", "442", "450",
    "451", "455", "456", "501", "504", "505", "553", "554", "556", "558", "712", "713", "714",
    "716"
]

# Split the routes into smaller batches (e.g., 20 routes per batch)
batch_size = 10
route_batches = [routes[i:i + batch_size] for i in range(0, len(routes), batch_size)]

api_key = "844e75c1921b486aa93f856967ebe33c"  # Replace with your actual MBTA API Key

def fetch_and_insert_schedules(request):
    """
    Cloud Function to fetch schedules data from MBTA API in batches and load into BigQuery.
    """
    all_schedules = []
    
    for batch in route_batches:
        # Create a filter string for the batch
        filter_routes = ",".join(batch)
        url = f"https://api-v3.mbta.com/schedules?filter[route]={filter_routes}&api_key={api_key}"
        logging.info(f"Fetching data from URL: {url}")

        # Fetch data from the MBTA API
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad HTTP responses (4xx/5xx)
            data = response.json()
            schedules_data = data.get('data', [])

            if schedules_data:
                all_schedules.extend(schedules_data)
            else:
                logging.warning(f"No data found for routes: {batch}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data for routes: {batch}. Error: {e}")
            continue

    if not all_schedules:
        logging.error("No schedules data found in the API responses.")
        return "No schedules data found", 500

    # Process and insert data in batches to BigQuery
    schedules = []
    for schedule in all_schedules:
        try:
            attributes = schedule.get('attributes', {})
            relationships = schedule.get('relationships', {})

            schedules.append({
                'schedule_id': schedule.get('id'),  # STRING
                'arrival_time': attributes.get('arrival_time'),  # TIMESTAMP
                'departure_time': attributes.get('departure_time'),  # TIMESTAMP
                'direction_id': attributes.get('direction_id', -1),  # INT64
                'drop_off_type': attributes.get('drop_off_type', -1),  # INT64
                'pickup_type': attributes.get('pickup_type', -1),  # INT64
                'stop_headsign': attributes.get('stop_headsign', 'Unknown'),  # STRING
                'stop_sequence': attributes.get('stop_sequence', 0),  # INT64
                'timepoint': attributes.get('timepoint', False),  # BOOL
                'route_id': relationships.get('route', {}).get('data', {}).get('id') if relationships.get('route') else None,  # STRING
                'stop_id': relationships.get('stop', {}).get('data', {}).get('id') if relationships.get('stop') else None,  # STRING
                'trip_id': relationships.get('trip', {}).get('data', {}).get('id') if relationships.get('trip') else None  # STRING
            })
        except Exception as e:
            logging.error(f"Error processing schedule data: {e}")
            continue

    # Define batch size to avoid sending too much data in one go
    batch_size = 1000
    total_batches = math.ceil(len(schedules) / batch_size)

    for i in range(total_batches):
        start = i * batch_size
        end = start + batch_size
        batch = schedules[start:end]

        try:
            logging.info(f"Inserting batch {i+1}/{total_batches} with {len(batch)} schedules")
            errors = client.insert_rows_json("ba882-team8-fall24.mbta_dataset.schedules", batch)
            if errors:
                logging.error(f"Errors inserting batch {i+1}: {errors}")
                return f"Insertion failed with errors in batch {i+1}", 500
        except Exception as e:
            logging.error(f"BigQuery insert failed in batch {i+1}: {e}")
            return f"BigQuery insertion failed in batch {i+1}", 500

    logging.info("All batches successfully inserted into BigQuery schedules table.")
    return "Schedules data successfully inserted into BigQuery", 200
