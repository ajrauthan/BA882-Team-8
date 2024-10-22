import requests
from google.cloud import bigquery
import logging
import json

# Initialize BigQuery client
client = bigquery.Client()

# MBTA API endpoint for routes
routes_url = "https://api-v3.mbta.com/routes"
api_key = "844e75c1921b486aa93f856967ebe33c"  # Replace with your actual MBTA API Key

def fetch_and_insert_routes(request):
    """
    Cloud Function to fetch routes data from MBTA API and load into BigQuery.
    """
    # Construct the URL
    url = f"{routes_url}?api_key={api_key}"
    logging.info(f"Fetching data from URL: {url}")

    # Fetch data from the MBTA API
    response = requests.get(url)

    # Log the API status code and response for debugging
    logging.info(f"API Status Code: {response.status_code}")
    logging.info(f"API Response: {response.text}")

    if response.status_code != 200:
        logging.error(f"Failed to fetch data from MBTA API. Status Code: {response.status_code}")
        return "API call failed", 500

    data = response.json()
    routes_data = data.get('data', [])

    if not routes_data:
        logging.error("No routes data found in the API response.")
        return "No routes data found", 500

    # Prepare list for routes table
    routes = []
    for route in routes_data:
        route_attributes = route.get('attributes', {})
        relationships = route.get('relationships', {})

        routes.append({
            'route_id': route.get('id'),
            'color': route_attributes.get('color'),
            'description': route_attributes.get('description'),
            'direction_destinations': json.dumps(route_attributes.get('direction_destinations')),
            'direction_names': json.dumps(route_attributes.get('direction_names')),
            'fare_class': route_attributes.get('fare_class'),
            'long_name': route_attributes.get('long_name'),
            'short_name': route_attributes.get('short_name'),
            'sort_order': route_attributes.get('sort_order'),
            'text_color': route_attributes.get('text_color'),
            'type': route_attributes.get('type'),
            'line_id': relationships.get('line', {}).get('data', {}).get('id') if relationships.get('line') else None,
            'self_link': route.get('links', {}).get('self')
        })

    # Insert data into the routes table in BigQuery
    try:
        errors = client.insert_rows_json("ba882-team8-fall24.mbta_dataset.routes", routes)
        if errors:
            logging.error(f"Errors inserting into routes table: {errors}")
            return f"Insertion failed with errors: {errors}", 500
    except Exception as e:
        logging.error(f"BigQuery insert failed: {e}")
        return "BigQuery insertion failed", 500

    logging.info("Data successfully inserted into BigQuery routes table.")
    return "Routes data successfully inserted into BigQuery", 200
