from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def clean_multiple_tables(request):
    """
    Cloud Function to remove exactly duplicate rows from multiple BigQuery tables.
    This function cleans the following tables:
    - updated_routes
    - updated_stops
    - updated_vehicles
    - updated_trips
    """
    try:
        # Define project, dataset, and tables to clean
        project_id = "ba882-team8-fall24"
        dataset_id = "mbta_dataset"
        tables = ["updated_routes", "updated_stops", "updated_vehicles", "updated_trips"]

        for table_id in tables:
            logging.info(f"Cleaning table {table_id}")

            # Define the source and temp table references
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            temp_table_ref = f"{project_id}.{dataset_id}.{table_id}_temp"

            # Query to select distinct rows from each table
            query = f"""
            CREATE OR REPLACE TABLE `{temp_table_ref}` AS
            SELECT DISTINCT * FROM `{table_ref}`
            """

            # Execute the query to create the temporary table with distinct rows
            query_job = client.query(query)
            query_job.result()  # Wait for the job to complete
            logging.info(f"Distinct rows written to {temp_table_ref}")

            # Query to replace the original table with the cleaned temp table
            query = f"""
            CREATE OR REPLACE TABLE `{table_ref}` AS
            SELECT * FROM `{temp_table_ref}`
            """

            # Execute the query to replace the original table
            query_job = client.query(query)
            query_job.result()  # Wait for the job to complete
            logging.info(f"Table {table_ref} cleaned of duplicate rows.")

            # Optionally, drop the temp table after cleaning
            client.delete_table(temp_table_ref, not_found_ok=True)
            logging.info(f"Temporary table {temp_table_ref} deleted.")

        return "All specified tables cleaned successfully", 200

    except Exception as e:
        logging.error(f"Error cleaning tables: {e}")
        return f"Error: {e}", 500
