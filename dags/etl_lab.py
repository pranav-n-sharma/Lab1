from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

from pathlib import Path

import csv
import json
import requests
import snowflake.connector

# City 1: Sunnyvale (37.37, -122.04)
# City 2: Bakersfield (35.37, -119.02)

@task
def extract():

	# Open Meteo parameters
	params = {
		"latitude": None,
		"longitude": None,
		"past_days": int(Variable.get("n_days")),
		"forecast_days": 0,
		"daily": [
			"temperature_2m_max",
			"temperature_2m_min",
			"precipitation_sum",
			"weather_code"
		],
		"timezone": "America/Los_Angeles"
	}

	# JSON path for each city's API response
	paths = [None, None]

	# Process each city
	for city in (1,2):
		params["latitude"] = float(Variable.get(f"lat{city}"))
		params["longitude"] = float(Variable.get(f"lng{city}"))

		response = requests.get(Variable.get("src"), params=params, timeout=30)
		response.raise_for_status()

		context = get_current_context()
		dag_id = context["dag"].dag_id
		run_id = context["run_id"].replace(":", "_").replace("+", "_")

		base = Path("/opt/airflow/data/") / dag_id / run_id
		base.mkdir(parents=True, exist_ok=True)

		ext_path = base / f"extract{city}.json"
		with open(ext_path, "w") as ext_json:
			json.dump(response.json(), ext_json, indent=4)

		paths[city - 1] = str(ext_path)

	paths = tuple(paths)
	return paths
		


@task
def transform(ext_paths):

	tr_paths = [None] * len(ext_paths)

	# Process each city
	for index, path in enumerate(ext_paths):
		with open(path, "r") as ext_json:
			data = json.load(ext_json)

		lat, lng = data["latitude"], data["longitude"]
		daily = data["daily"]
		n_days = len(daily["time"])

		date, t_max, t_min, prec, wc = \
		daily["time"], daily["temperature_2m_max"], daily["temperature_2m_min"], \
		daily["precipitation_sum"], daily["weather_code"]

		# Write rows to file
		base = Path(path).parent
		tr_path = base / f"transformed{index + 1}.csv"
		with open(tr_path, "w", newline="") as dst:
			writer = csv.writer(dst)

			# 2d tuple: 
			writer.writerows(
				(
					lat,
					lng,
					date[i],
					t_max[i],
					t_min[i],
					prec[i],
					wc[i]
				) \
				for i in range(n_days)
			)

		tr_paths[index] = str(tr_path)

	# Return transform path
	tr_paths = tuple(tr_paths)
	return tr_paths


@task
def load(tr_paths, tables):

	hook = SnowflakeHook(
		snowflake_conn_id="snowflake_conn",

	)

	for path, table in zip(tr_paths, tables):

		# Create table outside transaction due to default commit
		hook.run(
			"""
				CREATE TABLE IF NOT EXISTS IDENTIFIER(%s) (
					Latitude NUMBER(9,6),
					Longitude NUMBER(9,6),
					Record_Date DATE,
					Temp_Max NUMBER(4,1),
					Temp_Min NUMBER(4,1),
					Precipitation NUMBER(4,2),
					Weather_Code NUMBER(2,0),

					PRIMARY KEY (Latitude,Longitude,Record_Date)
				)
			""",
			parameters=(table,)
		)

		# Stage file outside transaction to isolate delays
		hook.run(
			f"PUT file://{path} @%{table}"
		)

		# Transaction
		conn = hook.get_conn()

		try:
			conn.autocommit(False)

			with conn.cursor() as cur:
				cur.execute(f"DELETE FROM {table}")
				cur.execute(f"COPY INTO {table} FROM @%{table}")

			conn.commit()
		except Exception as e:
			conn.rollback()
			raise e
		finally:
			conn.close()



with DAG(
	dag_id="lab",
	start_date=datetime(2026,10,1),
	schedule=timedelta(minutes=5),
	catchup=False
) as dag:

	ext_paths = extract()
	tr_paths = transform(ext_paths)
	load(tr_paths=tr_paths, tables=("City_1", "City_2"))
