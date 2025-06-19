import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


def extract_aircrafts(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='local_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    aircrafts_query = """
    SELECT ad.aircraft_code, model, range,
           array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Economy') AS economy_seats,
           array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Business') AS business_seats,
           array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Comfort') AS comfort_seats
    FROM aircrafts_data AS ad JOIN seats s ON ad.aircraft_code=s.aircraft_code
    GROUP BY ad.aircraft_code, model, range;
    """

    cursor.execute(aircrafts_query)
    columns = list(cursor.description)
    aircrafts = cursor.fetchall()

    aircrafts_results = []
    for row in aircrafts:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        aircrafts_results.append(row_dict)

    logging.info(f'Extracted {len(aircrafts_results)} aircrafts rows')

    kwargs['ti'].xcom_push(key='extracted_aircrafts', value=aircrafts_results)
    cursor.close()
    conn.close()


def extract_flights(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='local_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    flights_query = """
    WITH aircrafts_seats AS (
    SELECT ad.aircraft_code, model, range,
        array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Economy') AS economy_seats,
        array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Business') AS business_seats,
        array_agg(s.seat_no) FILTER (WHERE s.fare_conditions = 'Comfort') AS comfort_seats
    FROM aircrafts_data AS ad JOIN seats s ON ad.aircraft_code=s.aircraft_code
    GROUP BY ad.aircraft_code, model, range
    )
    SELECT f.flight_id, f.flight_no, json_build_object('aircraft_code', a.aircraft_code, 'model', a.model, 'range',
        a.range, 'economy_seats', a.economy_seats, 'business_seats', a.business_seats, 'comfort_seats',
        a.comfort_seats) AS aircraft, json_build_object('airport_code', departure_airport, 'airport_name',
        departure_airport_name, 'city', departure_city, 'coordinates', d_ap.coordinates, 'timezone', d_ap.timezone)
        AS departure_airport, json_build_object('airport_code', arrival_airport, 'airport_name', arrival_airport_name,
        'city', arrival_city, 'coordinates', ar_ap.coordinates, 'timezone', ar_ap.timezone) AS arrival_airport,
        json_build_object('scheduled_departure', scheduled_departure, 'scheduled_departure_local', scheduled_departure_local,
        'scheduled_arrival', scheduled_arrival, 'scheduled_arrival_local', scheduled_arrival_local,
        'scheduled_duration', scheduled_duration, 'actual_departure', actual_departure, 'actual_departure_local',
        actual_departure_local, 'actual_arrival', actual_arrival, 'actual_arrival_local', actual_arrival_local, 
        'actual_duration', actual_duration) AS flight_time, f.status
    FROM flights_vt AS f JOIN aircrafts_seats a ON f.aircraft_code=a.aircraft_code
    JOIN airports d_ap ON f.departure_airport=d_ap.airport_code
    JOIN airports ar_ap ON f.arrival_airport=ar_ap.airport_code;
    """

    cursor.execute(flights_query)
    columns = list(cursor.description)
    flights = cursor.fetchall()

    flights_results = []
    for row in flights:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        flights_results.append(row_dict)

    logging.info(f'Extracted {len(flights_results)} flghts rows')

    kwargs['ti'].xcom_push(key='extracted_flights', value=flights_results)
    cursor.close()
    conn.close()


def extract_bookings(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='local_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    bookings_query = """
    SELECT b.book_ref, b.book_date, b.total_amount, json_agg(json_build_object('ticket_no', t.ticket_no,
    'flight_id', tf.flight_id, 'passenger_id', t.passenger_id, 'passenger_name', t.passenger_name,
    'contact_data', t.contact_data, 'fare_conditions', tf.fare_conditions, 'amount', tf.amount, 'seat_no',
    bp.seat_no)) AS tickets
    FROM bookings AS b JOIN tickets t ON b.book_ref=t.book_ref
    JOIN ticket_flights tf ON t.ticket_no=tf.ticket_no
    LEFT JOIN boarding_passes bp ON tf.ticket_no=bp.ticket_no AND tf.flight_id=bp.flight_id
    GROUP BY b.book_ref, b.book_date, b.total_amount;
    """

    cursor.execute(bookings_query)
    columns = list(cursor.description)
    bookings = cursor.fetchall()

    bookings_results = []
    for row in bookings:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        bookings_results.append(row_dict)

    logging.info(f'Extracted {len(bookings_results)} bookings rows')

    kwargs['ti'].xcom_push(key='extracted_bookings', value=bookings_results)
    cursor.close()
    conn.close()


def load_collections(**kwargs):
    ti = kwargs['ti']

    extracted_aircrafts = ti.xcom_pull(task_ids='extract_aircrafts', key='extracted_aircrafts')
    extracted_flights = ti.xcom_pull(task_ids='extract_flights', key='extracted_flights')
    extracted_bookings = ti.xcom_pull(task_ids='extract_bookings', key='extracted_bookings')

    mongo_hook = MongoHook(mongo_conn_id='local_mongo_conn')
    client = mongo_hook.get_conn()
    db = client['aircraft']

    db.create_collection('aircrafts')
    db['aircrafts'].insert_many(extracted_aircrafts)
    logging.info('Inserted aircrafts documents')
    db.create_collection('flights')
    db['flights'].insert_many(extracted_flights)
    logging.info('Inserted flights documents')
    db.create_collection('bookings')
    db['bookings'].insert_many(extracted_bookings)
    logging.info('Inserted bookings documents')

    client.close()


with DAG(
    dag_id='postgres_to_mongo_migration_dag',
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
    tags=["migration"],
) as dag:
    task_start = EmptyOperator(task_id='start', dag=dag)
    task_finish = EmptyOperator(task_id='finish', dag=dag)

    task_extract_aircrafts = PythonOperator(task_id='extract_aircrafts', python_callable=extract_aircrafts, provide_context=True, dag=dag)
    task_extract_flights = PythonOperator(task_id='extract_flights', python_callable=extract_flights, provide_context=True, dag=dag)
    task_extract_bookings = PythonOperator(task_id='extract_bookings', python_callable=extract_bookings, provide_context=True, dag=dag)
    task_load = PythonOperator(task_id='load', python_callable=load_collections, provide_context=True, dag=dag)

    task_start >> task_extract_aircrafts >> task_extract_flights >> task_extract_bookings >> task_load >> task_finish
