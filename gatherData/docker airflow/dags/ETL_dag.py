from datetime import datetime, timedelta


from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'bliyuka',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# def import_packages():
#     import requests
#     # import pandas as pd
#     print(f"packages imported")


def Extract(ti):
    import requests
    CITY = 'Hanoi'
    UNITS = 'metric'
    API_KEY = 'd1c9f17783883637fa9d6640798d2b0e'
    url = f'https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        ti.xcom_push(key='raw_current_data', value=data)

def Transform_and_Load(ti):
    data = ti.xcom_pull(task_ids='Extract', key='raw_current_data')
    # print(data)
    if not data:
        raise ValueError("No data to load.")

    hook = PostgresHook(postgres_conn_id="postgres_localhost")



    # -------city data-------
    # rows_to_insert= (name, lat, lon)
    
    # lon = data["coord"]["lon"]
    # lat = data["coord"]["lat"]
    # name = data["name"]

    # rows_to_insert = [(name,lat,lon)]
    # target_fields = ["name", "latitude", "longitude"]
    
    # hook.insert_rows(
    # table="cities",
    # rows=rows_to_insert,
    # target_fields=target_fields,
    # # commit_every=1000 # For large datasets, commit in batches
    # )


    # -------weather data-------
    # date, main, des, temp, feels, vis, pres, hum, sea_ press, grnd_press, wind_spd, wind_deg, wind_gust, cloudiness, rain_vol, snow_vol, icon_id

    date = datetime.utcfromtimestamp(data["dt"] + data["timezone"]).strftime('%Y-%m-%d %H:%M:%S')
    main = data["weather"][0]["main"]
    des = data["weather"][0]["description"]
    temp = data["main"]["temp"]
    if temp > 100: temp -= 274.15
    feels =data ["main"]["feels_like"]
    if feels >100: feels -= 274.15
    vis = data["visibility"]
    pres = data["main"]["pressure"]
    hum = data["main"]["humidity"]
    sea_pres = data["main"]["sea_level"]
    grnd_pres = data["main"]["grnd_level"]
    wind_spd = data["wind"]["speed"]
    wind_deg = data["wind"]["deg"]
    wind_gust = data["wind"]["gust"]
    cloudiness = data["clouds"]["all"]
    rain_vol = 0
    if "rain" in data: rain_vol = data["rain"]["1h"]
    snow_vol = 0
    if "snow" in data: snow_vol = data["snow"]["1h"]
    icon_id = data["weather"][0]["icon"]

    rows_to_insert= [(date, 1,main, des, temp, feels, vis, pres, hum, sea_pres, grnd_pres, wind_spd, wind_deg, wind_gust, cloudiness, rain_vol, snow_vol, icon_id)]
    
    target_fields = ["date_time",
                     "city_id",
                     "weather_main",
                     "weather_description",
                     "Temperature",
                     "feels_like",
                     "visibility",
                     "pressure",
                     "humidity",
                     "sea_level_pressure",
                     "grnd_level_pressure",
                     "wind_speed",
                     "wind_deg",
                     "wind_gust",
                     "cloudiness",
                     "rain_volume",
                     "snow_volume",
                     "icon_id"]
    
    hook.insert_rows(
    table="weather_data",
    rows=rows_to_insert,
    target_fields=target_fields,
    # commit_every=1000 # For large datasets, commit in batches
    )


with DAG(
    default_args=default_args,
    dag_id="weather_ETL_dag_v12",
    # description="My first dag with python operator",
    start_date=datetime(2025, 9, 23),
    schedule_interval="@hourly"
) as dag:
    
    # import_packages = PythonOperator(
    #     task_id="import_packages",
    #     python_callable=import_packages,)
    
    
    extract_data = PythonOperator(
        task_id="Extract",
        python_callable=Extract
    )
    
    
    transform_data = PythonOperator(
        task_id="Transform_and_Load",
        python_callable=Transform_and_Load
    )
    
#     create_table = PostgresOperator(
#         task_id='create_postgres_table',
#         postgres_conn_id='postgres_localhost',
#         sql="""
# -- This script creates the complete database schema for the weather application.
# -- It is important to run these queries in the specified order due to table dependencies.

# -- Step 1: Create the 'cities' table.
# -- This table must be created first because 'weather_data' references it.

# CREATE TABLE IF NOT EXISTS cities (
#     -- Unique identifier for each city.
#     id SERIAL PRIMARY KEY,

#     -- The name of the city. Using UNIQUE constraint to prevent duplicate cities.
#     name VARCHAR(100) NOT NULL UNIQUE,

#     -- Latitude of the city for geographical queries.
#     latitude NUMERIC(9, 6),

#     -- Longitude of the city for geographical queries.
#     longitude NUMERIC(9, 6)
# );

# COMMENT ON TABLE cities IS 'Stores unique information for each city, such as name and coordinates.';


# -- Step 2: Create the 'weather_data' table.
# -- This table contains a foreign key that links to the 'cities' table.

# CREATE TABLE IF NOT EXISTS weather_data (
#     -- Unique identifier for each record.
#     id SERIAL PRIMARY KEY,

#     -- The date and time of the observation.
#     date_time TIMESTAMPTZ NOT NULL,

#     -- Foreign key that references the 'id' in the 'cities' table.
#     -- This links each weather entry to a specific city.
#     city_id INTEGER NOT NULL REFERENCES cities(id),

#     -- A short, general description of the weather (e.g., "Clouds", "Rain").
#     weather_main VARCHAR(50),

#     -- A more detailed description of the weather conditions.
#     weather_description VARCHAR(255),

#     -- Temperature in Celsius.
#     temperature NUMERIC(5, 2),

#     -- The "feels like" temperature.
#     feels_like NUMERIC(5, 2),

#     -- Visibility in meters.
#     visibility INTEGER,

#     -- Atmospheric pressure on the ground level, in hPa.
#     pressure INTEGER,

#     -- Humidity percentage.
#     humidity INTEGER,

#     -- Atmospheric pressure at sea level, in hPa.
#     sea_level_pressure INTEGER,

#     -- Atmospheric pressure at ground level, in hPa.
#     grnd_level_pressure INTEGER,

#     -- Wind speed in meter/sec.
#     wind_speed NUMERIC(5, 2),

#     -- Wind direction in degrees.
#     wind_deg INTEGER,

#     -- Wind gust speed in meter/sec.
#     wind_gust NUMERIC(5, 2),

#     -- Cloudiness percentage.
#     cloudiness INTEGER,

#     -- Rain volume for the last 1 or 3 hours, in mm.
#     rain_volume NUMERIC(7, 2),

#     -- Snow volume for the last 1 or 3 hours, in mm.
#     snow_volume NUMERIC(7, 2),

#     -- Weather icon ID from the data source.
#     icon_id VARCHAR(10),

#     -- A weather reading for a city at a specific time should be unique.
#     UNIQUE (date_time, city_id)
# );

# COMMENT ON TABLE weather_data IS 'Stores historical and forecasted weather data points, linked to a city.';
# COMMENT ON COLUMN weather_data.city_id IS 'A reference to the primary key of the cities table.';
        
#     INSERT INTO cities (city_id, city_name, country) VALUES ()

        
        
        
#         """
#     )
    
    
    extract_data >> transform_data