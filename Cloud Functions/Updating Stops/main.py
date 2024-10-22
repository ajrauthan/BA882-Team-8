from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def update_stops_table(request):
    """
    Cloud Function to create a new `updated_stops` table without certain columns 
    and copy data from the existing `stops` table.
    """
    try:
        # Define project, dataset, and table names
        project_id = "ba882-team8-fall24"
        dataset_id = "mbta_dataset"
        old_table = f"{project_id}.{dataset_id}.stops"
        new_table = f"{project_id}.{dataset_id}.updated_stops"

        # Define the schema for the new table (without 'self_link' and 'location_type')
        new_schema = [
            bigquery.SchemaField("stop_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("municipality", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("wheelchair_boarding", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("parent_station_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zone_id", "STRING", mode="NULLABLE"),
        ]

        # Create the new table with the defined schema
        table_ref = client.dataset(dataset_id).table("updated_stops")
        new_table = bigquery.Table(table_ref, schema=new_schema)
        client.create_table(new_table, exists_ok=True)
        logging.info("New table `updated_stops` created successfully.")

        # SQL to copy data from the original `stops` table (excluding dropped columns)
        query = f"""
        INSERT INTO `{new_table}`
        SELECT 
            stop_id, name, description, latitude, longitude, 
            municipality, wheelchair_boarding, parent_station_id, zone_id
        FROM `{old_table}`
        """

        # Execute the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logging.info("Data copied successfully to `updated_stops` table.")

        return "Stops table updated successfully", 200

    except Exception as e:
        logging.error(f"Error updating stops table: {e}")
        return f"Error: {e}", 500
