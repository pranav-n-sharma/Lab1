from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator

from datetime import timedelta
from datetime import datetime
import requests


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def return_snowflake_conn():
    """Create a connection and cursor to Snowflake using Airflow's SnowflakeHook."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    return conn, conn.cursor()


@task
def extract_weather_data(latitude: float, longitude: float, table_name: str) -> dict:
    # Calculate date range for the past 80 days
    end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=80)).strftime("%Y-%m-%d")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "rain_sum",
        "timezone": "auto"
    }
    
    print(f"Fetching weather data for Lab1.raw.{table_name} from {start_date} to {end_date}...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    
    dates = data.get("daily", {}).get("time", [])
    rain_sums = data.get("daily", {}).get("rain_sum", [])
    
    if not dates or not rain_sums:
        raise ValueError(f"No weather data returned from the API for table {table_name}.")
        
    return {
        "latitude": latitude,
        "longitude": longitude,
        "table_name": table_name,
        "start_date": start_date,
        "end_date": end_date,
        "records": [{"date": d, "rain": r} for d, r in zip(dates, rain_sums)]
    }

@task
def load_weather_data(weather_payload: dict):
    latitude = weather_payload["latitude"]
    longitude = weather_payload["longitude"]
    table_name = weather_payload["table_name"]
    start_date = weather_payload["start_date"]
    end_date = weather_payload["end_date"]
    records = weather_payload["records"]
    
    conn, cursor = return_snowflake_conn()
    
    try:
        # start of sql transaction
        cursor.execute("BEGIN;")
        
        # Ensure target table exists in Lab1.raw with dynamic table naming
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS Lab1.raw.{table_name} (
                record_date DATE,
                rain_sum_mm FLOAT,
                latitude FLOAT,
                longitude FLOAT,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Idempotency check: Clear out overlapping data for these coordinates
        cursor.execute(f"""
            DELETE FROM Lab1.raw.{table_name} 
            WHERE record_date BETWEEN '{start_date}' AND '{end_date}'
              AND latitude = {latitude} 
              AND longitude = {longitude}
        """)
        
        # Insert rows
        insert_query = f"""
            INSERT INTO Lab1.raw.{table_name} (record_date, rain_sum_mm, latitude, longitude)
            VALUES (%s, %s, %s, %s)
        """
        
        formatted_records = [(r["date"], r["rain"], latitude, longitude) for r in records]
        cursor.executemany(insert_query, formatted_records)
        
        # committing if all queries executed successfully
        cursor.execute("COMMIT;")
        print(f"Successfully committed {len(formatted_records)} weather records into Lab1.raw.{table_name}.")
        
    except Exception as e:
        # rollback transaction if any error occurs
        cursor.execute("ROLLBACK;")
        print(f"Error encountered for {table_name}: {e}. Transaction rolled back.")
        raise e
        
    finally:
        conn.close()
        cursor.close()


with DAG(
    dag_id="weather_extration_2_cities",
    default_args=DEFAULT_ARGS,
    description="Extract past 80 days of daily rain data for Seattle and Miami using SnowflakeHook and load into separate tables",
    schedule="0 0 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    # --- Seattle Pipeline ---
    seattle_data = extract_weather_data(
        latitude=47.543677, 
        longitude=-122.290974, 
        table_name="seattle_weather_data"
    )
    seattle_load = load_weather_data(seattle_data)

    # --- Miami Pipeline ---
    miami_data = extract_weather_data(
        latitude=25.777604, 
        longitude=-80.235008, 
        table_name="miami_weather_data"
    )
    miami_load = load_weather_data(miami_data)

    run_dbt_models = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /opt/ && ls",
    )

    # --- Task Dependencies ---
    # dbt runs only after both Seattle and Miami load tasks are successfully finished
    [seattle_load, miami_load] >> run_dbt_models