from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def update_routes_table(request):
    """
    Cloud Function to create a new `updated_routes` table without certain columns 
    and copy data from the existing `routes` table.
    """
    try:
        # Define project, dataset, and table details
        project_id = "ba882-team8-fall24"
        dataset_id = "mbta_dataset"
        old_table = f"{project_id}.{dataset_id}.routes"
        new_table = f"{project_id}.{dataset_id}.updated_routes"

        # Define the schema for the new table (without the dropped columns)
        new_schema = [
            bigquery.SchemaField("route_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("direction_destinations", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("direction_names", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fare_class", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("long_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("short_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sort_order", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("type", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("line_id", "STRING", mode="NULLABLE"),
        ]

        # Create the new table with the defined schema
        table_ref = client.dataset(dataset_id).table("updated_routes")
        new_table = bigquery.Table(table_ref, schema=new_schema)
        client.create_table(new_table, exists_ok=True)
        logging.info("New table `updated_routes` created successfully.")

        # SQL to copy data from the original `routes` table (excluding the dropped columns)
        query = f"""
        INSERT INTO `{new_table}`
        SELECT 
            route_id, description, direction_destinations, direction_names,
            fare_class, long_name, short_name, sort_order, type, line_id
        FROM `{old_table}`
        """
        
        # Execute the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logging.info("Data copied successfully to `updated_routes` table.")

        return "Routes table updated successfully", 200

    except Exception as e:
        logging.error(f"Error updating routes table: {e}")
        return f"Error: {e}", 500