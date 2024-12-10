import os
from google.cloud import bigquery

def join_tables(request):
    """
    Cloud Function to join existing tables and create a new table in BigQuery.
    """
    # Initialize BigQuery client
    client = bigquery.Client()

    # Query to join tables
    query = """
    WITH final AS (
        SELECT 
            j.prediction_id, j.schedule_id, j.arrival_time_p AS actual_arr, 
            j.arrival_time_s AS schedule_arr, j.departure_time_p AS actual_depart, 
            j.departure_time_s AS schedule_depart, j.direction_id_p AS direction_id, 
            j.last_trip, j.stop_sequence_p AS stop_sequence, j.route_id_p AS route_id, 
            j.stop_id_p AS stop_id, j.trip_id, j.vehicle_id, j.drop_off_type, 
            j.pickup_type, j.delay, j.day, j.time_of_the_day, 
            r.description AS route_description, 
            s.description AS stop_description, s.municipality
        FROM `ba882-team8-fall24.mbta_dataset.joined_prediction` AS j
        LEFT JOIN `ba882-team8-fall24.mbta_dataset.routes` AS r ON j.route_id_p = r.route_id
        LEFT JOIN `ba882-team8-fall24.mbta_dataset.stops` AS s ON j.stop_id_p = s.stop_id
    )
    SELECT DISTINCT * FROM final
    """

    # Configuration for the destination table
    destination_table = "ba882-team8-fall24.mbta_LLM.LLM_join"

    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Execute the query
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to finish
        return "Table created successfully!", 200
    except Exception as e:
        return f"Error: {e}", 500