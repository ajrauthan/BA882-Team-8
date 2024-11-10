from google.cloud import bigquery
import logging

# Initialize BigQuery client
client = bigquery.Client()

def create_and_update_joined_prediction_table(request):
    """
    Cloud Function to create the `joined_prediction` table by joining
    the `schedules` and `predictions` tables, and then updating the table with
    additional computed columns.
    """
    try:
        # Define the SQL query to create the joined table
        create_query = """
        CREATE OR REPLACE TABLE `ba882-team8-fall24.mbta_dataset.joined_prediction` AS
        SELECT
            p.prediction_id,
            s.schedule_id,
            p.arrival_time AS arrival_time_p,
            s.arrival_time AS arrival_time_s,
            p.arrival_uncertainty,
            p.departure_time AS departure_time_p,
            s.departure_time AS departure_time_s,
            p.departure_uncertainty,
            p.direction_id AS direction_id_p,
            s.direction_id AS direction_id_s,
            p.last_trip,
            p.revenue,
            p.schedule_relationship,
            p.status,
            p.stop_sequence AS stop_sequence_p,
            s.stop_sequence AS stop_sequence_s,
            p.update_type,
            p.route_id AS route_id_p,
            s.route_id AS route_id_s,
            p.stop_id AS stop_id_p,
            s.stop_id AS stop_id_s,  
            p.trip_id, 
            p.vehicle_id,
            s.drop_off_type,
            s.pickup_type,
            s.stop_headsign,
            s.timepoint,
            CAST(NULL AS INT64) AS delay,
            CAST(NULL AS STRING) AS day,
            CAST(NULL AS STRING) AS time_of_the_day
        FROM
            `ba882-team8-fall24.mbta_dataset.schedules` AS s
        INNER JOIN
            `ba882-team8-fall24.mbta_dataset.predictions` AS p
        ON
            s.trip_id = p.trip_id 
            AND s.route_id = p.route_id 
            AND s.stop_id = p.stop_id
            AND EXTRACT(DATE FROM p.arrival_time) = EXTRACT(DATE FROM s.arrival_time)
        WHERE
            s.arrival_time IS NOT NULL 
            AND p.arrival_time IS NOT NULL
            AND s.departure_time IS NOT NULL
            AND p.departure_time IS NOT NULL;
        """

        # Execute the creation query
        create_query_job = client.query(create_query)
        create_query_job.result()  # Wait for the query to complete

        logging.info("`joined_prediction` table created successfully.")

        # Define the SQL query to update the joined table with computed columns
        update_query = """
        UPDATE `ba882-team8-fall24.mbta_dataset.joined_prediction`
        SET 
            arrival_time_p = TIMESTAMP_SUB(arrival_time_p, INTERVAL 4 HOUR),
            arrival_time_s = TIMESTAMP_SUB(arrival_time_s, INTERVAL 4 HOUR),
            departure_time_p = TIMESTAMP_SUB(departure_time_p, INTERVAL 4 HOUR),
            departure_time_s = TIMESTAMP_SUB(departure_time_s, INTERVAL 4 HOUR),
            delay = TIMESTAMP_DIFF(arrival_time_p, arrival_time_s, MINUTE),
            day = CASE 
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 1 THEN 'Sunday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 2 THEN 'Monday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 3 THEN 'Tuesday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 4 THEN 'Wednesday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 5 THEN 'Thursday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 6 THEN 'Friday'
                WHEN EXTRACT(DAYOFWEEK FROM arrival_time_p) = 7 THEN 'Saturday'
            END,
            time_of_the_day = CASE
                WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(arrival_time_p, INTERVAL 4 HOUR)) BETWEEN 4 AND 11 THEN 'Morning'
                WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(arrival_time_p, INTERVAL 4 HOUR)) BETWEEN 12 AND 16 THEN 'Afternoon'
                WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(arrival_time_p, INTERVAL 4 HOUR)) BETWEEN 17 AND 20 THEN 'Evening'
                ELSE 'Night'
            END 
        WHERE TRUE;
        """

        # Execute the update query
        update_query_job = client.query(update_query)
        update_query_job.result()  # Wait for the query to complete

        logging.info("`joined_prediction` table updated successfully.")
        return "Table created and updated successfully", 200

    except Exception as e:
        logging.error(f"Error in creating or updating `joined_prediction` table: {e}")
        return f"Error: {e}", 500