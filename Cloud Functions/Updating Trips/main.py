from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def transfer_trips_data(request):
    """
    Cloud Function to transfer data from `trips` table to `updated_trips` table
    without 'revenue' and 'self_link' columns, using a SQL query.
    """
    try:
        # Define the table references
        project_id = "ba882-team8-fall24"
        dataset_id = "mbta_dataset"
        source_table = f"{project_id}.{dataset_id}.trips"
        target_table = f"{project_id}.{dataset_id}.updated_trips"

        # Log the source and target table names
        logging.info(f"Source table: {source_table}")
        logging.info(f"Target table: {target_table}")

        # Query to select specific columns from the source table
        query = f"""
        CREATE OR REPLACE TABLE `{target_table}` AS
        SELECT 
            trip_id, bikes_allowed, block_id, direction_id, headsign, 
            wheelchair_accessible, route_id, route_pattern_id, 
            service_id, shape_id
        FROM `{source_table}`
        """

        # Execute the query to transfer data directly
        query_job = client.query(query)
        query_job.result(timeout=300)  # Wait for up to 5 minutes

        logging.info(f"Data successfully transferred from `{source_table}` to `{target_table}`.")
        return "Trips data transferred successfully", 200

    except Exception as e:
        logging.error(f"Error transferring trips data: {e}")
        return f"Error: {e}", 500
