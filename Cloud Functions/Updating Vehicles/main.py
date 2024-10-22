from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def update_vehicles_table(request):
    """
    Cloud Function to create a new `updated_vehicles` table without certain columns
    and copy data from the existing `vehicles` table.
    """
    try:
        # Define project, dataset, and table names
        project_id = "ba882-team8-fall24"
        dataset_id = "mbta_dataset"
        old_table = f"{project_id}.{dataset_id}.vehicles"
        new_table = f"{project_id}.{dataset_id}.updated_vehicles"

        # Define the schema for the new table (without 'type' and 'revenue')
        new_schema = [
            bigquery.SchemaField("vehicle_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("direction_id", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("bearing", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("current_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("current_stop_sequence", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("occupancy_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("speed", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("route_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("stop_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("trip_id", "STRING", mode="NULLABLE"),
        ]

        # Create the new table with the defined schema
        table_ref = client.dataset(dataset_id).table("updated_vehicles")
        new_table = bigquery.Table(table_ref, schema=new_schema)
        client.create_table(new_table, exists_ok=True)
        logging.info("New table `updated_vehicles` created successfully.")

        # SQL to copy data from the original `vehicles` table (excluding dropped columns)
        query = f"""
        INSERT INTO `{new_table}`
        SELECT 
            vehicle_id, label, direction_id, bearing, current_status, 
            current_stop_sequence, latitude, longitude, occupancy_status, 
            speed, updated_at, route_id, stop_id, trip_id
        FROM `{old_table}`
        """

        # Execute the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logging.info("Data copied successfully to `updated_vehicles` table.")

        return "Vehicles table updated successfully", 200

    except Exception as e:
        logging.error(f"Error updating vehicles table: {e}")
        return f"Error: {e}", 500