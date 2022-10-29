import meteomatics.api as api
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from fastapi import FastAPI
import datetime as dt
from datetime import datetime
import pandas as pd
import yaml

app = FastAPI()

with open('configfile.yml') as configfile:
    config_params = yaml.load(configfile, Loader=yaml.FullLoader)

days_to_forecast = config_params['WEATHER FORECAST PARAMETERS']['Forecast days']
starting_date = config_params['WEATHER FORECAST PARAMETERS']['Starting date']
interval_hours = config_params['WEATHER FORECAST PARAMETERS']['Interval hours']
cities_coordinates = config_params['WEATHER FORECAST PARAMETERS']['Cities']
weather_metrics = config_params['WEATHER FORECAST PARAMETERS']['Weather Metrics']

meteomatics_username = config_params['METEOMATICS LOGIN']['username']
meteomatics_pwd = config_params['METEOMATICS LOGIN']['password']

def collect_weather_data(coordinates, starting_date, interval, parameters, username, password):
    startdate_ts = dt.datetime(starting_date['Year'], starting_date['Month'], starting_date['Day']).replace(hour=0, minute=0, second=0, microsecond=1)
    enddate_ts = startdate_ts + dt.timedelta(days=days_to_forecast)
    interval_ts = dt.timedelta(hours=interval)

    coordinates_ts = [(coordinates['Athens'][0], coordinates['Athens'][1]), 
                (coordinates['Edinburgh'][0], coordinates['Edinburgh'][1]), 
                (coordinates['Barcelona'][0], coordinates['Barcelona'][1])]

    df_ts = api.query_time_series(coordinates_ts, startdate_ts, enddate_ts, interval_ts, parameters, username, password)
    return df_ts

result = collect_weather_data(cities_coordinates, starting_date, interval_hours, weather_metrics, meteomatics_username, meteomatics_pwd)

result = result.reset_index()

result['Coordinates'] = list(zip(result.lat, result.lon))
result['City'] = pd.Series(dtype='str')
result['City'] = result['City'].fillna('')

# Assign city name for each coordinate
result = result.assign(City = lambda c: [c for coords in list(result['Coordinates']) for c in cities_coordinates if str(coords)==str(tuple(cities_coordinates[c]))])

# Create date and datetime columns that are easier to munipulate
result['datetime'] = pd.to_datetime(result['validdate']).dt.tz_localize(None)
result['date'] = pd.to_datetime(result['validdate']).dt.date
result['time'] = result['datetime'].dt.time

del result['Coordinates']

# Connect to MS SQL Server in order to load the collected data
host = config_params['DATABASE ACCESS']['host']
database = config_params['DATABASE ACCESS']['database']
db_username = config_params['DATABASE ACCESS']['username']

conn_engine = create_engine(f"mssql+pyodbc://{db_username}@{host}/{database}?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0")

data_table = """
    DROP TABLE IF EXISTS WeatherForecast;
    CREATE TABLE WeatherForecast(
            id [int] IDENTITY(1,1) PRIMARY KEY,
            lat [float](20),
            lon [float](20),
            validdate [date],
            [t_2m:C] [float](20),
            [t_mean_2m_3h:C] [float](20),
            [relative_humidity_2m:p] [float](20),
            [wind_speed_2m:ms] [float](20),
            [wind_dir_2m:d] [float](20),
            City [varchar](20),
            datetime [datetime],
            date [date],
            time [time]
    );
"""

with conn_engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(data_table)) 

result.to_sql(name='WeatherForecast',con=conn_engine, index=False, if_exists='append')

weather_data_query = """ SELECT City,
                                date,
                                time,
                                datetime,
                                [t_2m:C],
                                [t_mean_2m_3h:C],
                                [relative_humidity_2m:p],
                                [wind_speed_2m:ms],
                                [wind_dir_2m:d]
                         FROM [WeatherForecast] """

weather_data_df = pd.read_sql_query(weather_data_query, con=conn_engine)

# List the latest forecast for each location for every day
@app.get('/latest-forecast/')
def get_latest_forecast_per_loc_day():
    weather_data_grouped = weather_data_df.groupby(by=['City', 'date'], as_index=False).last()
    weather_data_grouped = pd.DataFrame(weather_data_grouped.groupby(by=['City'], as_index=False).apply(lambda x: x.iloc[:-1]))
    result_json = (weather_data_grouped.groupby(['City', 'date']).apply(lambda x: x[weather_metrics].to_dict('records'))).to_json()
    return result_json

# List the average the_temp of the last 3 forecasts for each location for every day
@app.get('/average-temp/')
def get_average_of_last_n_per_loc_day():
    weather_data_grouped = pd.DataFrame(weather_data_df.groupby(by=['City', 'date', 'time'])['t_2m:C'].last())
    weather_data_grouped = weather_data_grouped.groupby(by=['City'], as_index=False).apply(lambda x: x.iloc[:-1])
    weather_data_grouped = weather_data_grouped.groupby(by=['City', 'date'], as_index=False).apply(lambda x: x.iloc[-3:]).reset_index()
    weather_data_grouped_avg = pd.DataFrame(weather_data_grouped.groupby(by=['City', 'date'], as_index=False)['t_2m:C'].mean())
    result_json = (weather_data_grouped_avg.groupby(by=['City', 'date']).apply(lambda x: x[['t_2m:C']].to_dict('records'))).to_json()
    return result_json 

# Get the top n locations based on each available metric where n is a parameter given to the API call.
@app.get('/top-locations/{n_top}/')
def get_top_locations(n_top: int):
    medians_per_metric = pd.DataFrame(weather_data_df.groupby('City')[weather_metrics].median())
    metrics_dict = {}
    for column in medians_per_metric.columns:
        sub_df = pd.DataFrame(medians_per_metric[column]).reset_index().sort_values(column, ascending=False)
        metrics_dict[column] = sub_df['City'].tolist()

    return pd.DataFrame(metrics_dict).head(n_top).to_json()