#! /usr/bin/env python3

import pandas as pd
import os
from datetime import datetime
import numpy as np


### Reading in the datasets
print('reading datasets...')

bike_dtypes = {
    'ride_id': str,
    'rideable_type': str,
    'started_at': str,
    'ended_at': str,
    'start_station_name': str,
    'start_station_id': str,
    'end_station_name': str,
    'end_station_id': str,
    'start_lat': float,
    'start_lng': float,
    'end_lat': float,
    'end_lng': float,
    'member_casual': str,
    'time': float,
    'distance': float,
}

weather_dtypes = {
    'name': str,
    'datetime': str,
    'temp': float,
    'feelslike': float,
    'dew': float,
    'humidity': float,
    'precip': float,
    'precipprob': float,
    'preciptype': str,
    'snow': float,
    'snowdepth': float,
    'windgust': float,
    'windspeed': float,
    'winddir': float,
    'sealevelpressure': float,
    'cloudcover': float,
    'visibility': float,
    'solarradiation': float,
    'solarenergy': float,
    'uvindex': float,
    'severerisk': str,
    'conditions': str,
    'icon': str,
    'stations': str,
}

data_path = os.getcwd() + '/../data/'
bike_data = pd.read_csv(data_path+'data_dist_time.csv', dtype=bike_dtypes, index_col=0)
print('finished reading bike dataset, {} records read.'.format(len(bike_data)))
weather_data = pd.read_csv(data_path+'chicago_04012020-05312023.csv', dtype=weather_dtypes, index_col=False)
print('finished reading weather dataset, {} records read.'.format(len(weather_data)))


### Cleaning bike dataset 
print('cleaning bike dataset...', end='', flush=True)

bike_data['started_at'] = pd.to_datetime(bike_data['started_at'])
bike_data['ended_at'] = pd.to_datetime(bike_data['ended_at'])
bike_data['time'] = bike_data['time'].div(60)

bike_data['year'] = bike_data['started_at'].dt.year.astype('int')
bike_data['month'] = bike_data['started_at'].dt.month.astype('int')
bike_data['day'] = bike_data['started_at'].dt.day.astype('int')
bike_data['hour'] = bike_data['started_at'].dt.hour.astype('int')

bike_data.drop(
    ['ride_id','started_at','ended_at','start_station_id','end_station_id'], 
    axis=1, 
    inplace=True
)

### We will filter with a more restrictive time range compared to EDA
### as this will allow for better correlation analysis.
### A majority of rides are less than 60 minutes so we are not filtering
### out too many records of interest.
bike_data = bike_data[(bike_data.time > 1) & (bike_data.time < 60)]
bike_data = bike_data[(bike_data.distance > 0) & (bike_data.distance < 25)]

bike_data = bike_data.replace({'docked_bike': 'classic_bike'}, regex=True)
print('done.')


### Cleaning weather dataset
print('cleaning weather dataset...', end='', flush=True)

weather_data['datetime'] = pd.to_datetime(weather_data['datetime'], format='ISO8601')
weather_data['year'] = weather_data['datetime'].dt.year.astype('int')
weather_data['month'] = weather_data['datetime'].dt.month.astype('int')
weather_data['day'] = weather_data['datetime'].dt.day.astype('int')
weather_data['hour'] = weather_data['datetime'].dt.hour.astype('int')

weather_data.drop(
    ['name','datetime','precipprob','preciptype','snow','windgust','solarradiation','solarenergy','severerisk','stations'], 
    axis=1, 
    inplace=True
)
print('done.')


### Computing bike aggregations
print('computing bike aggregations...', end='', flush=True)
bike_agg = pd.DataFrame()
classic = bike_data['rideable_type']=='classic_bike'
electric = bike_data['rideable_type']=='electric_bike'
member = bike_data['member_casual']=='member'
casual = bike_data['member_casual']=='casual'

bike_agg['member_classic_counts'] = bike_data[member & classic].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['member_electric_counts'] = bike_data[member & electric].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['casual_classic_counts'] = bike_data[casual & classic].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['casual_electric_counts'] = bike_data[casual & electric].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['member_classic_avg_time'] = bike_data[member & classic].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['member_electric_avg_time'] = bike_data[member & electric].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['casual_classic_avg_time'] = bike_data[casual & classic].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['casual_electric_avg_time'] = bike_data[casual & electric].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['member_classic_avg_dist'] = bike_data[member & classic].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['member_electric_avg_dist'] = bike_data[member & electric].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['casual_classic_avg_dist'] = bike_data[casual & classic].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['casual_electric_avg_dist'] = bike_data[casual & electric].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['member_avg_time'] = bike_data[member].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['member_avg_dist'] = bike_data[member].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['casual_avg_time'] = bike_data[casual].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['casual_avg_dist'] = bike_data[casual].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['classic_avg_time'] = bike_data[classic].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['classic_avg_dist'] = bike_data[classic].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['electric_avg_time'] = bike_data[electric].groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['electric_avg_dist'] = bike_data[electric].groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['total_avg_time'] = bike_data.groupby(['year','month','day','hour'])[['time']].mean()
bike_agg['total_avg_dist'] = bike_data.groupby(['year','month','day','hour'])[['distance']].mean()
bike_agg['total_classic_counts'] = bike_data[classic].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['total_electric_counts'] = bike_data[electric].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['total_member_counts'] = bike_data[member].groupby(['year','month','day','hour']).size().astype('int')
bike_agg['total_casual_counts'] = bike_data[casual].groupby(['year','month','day','hour']).size().astype('int')

def most_common(series):
    counts = series.value_counts()
    return None if counts.empty else counts.idxmax()

bike_agg['pop_station'] = bike_data.groupby(['year','month','day','hour'])['start_station_name'].agg(most_common)
print('done.')


### Merging the datasets and saving to bike_weather_merged.csv
print('merging and writing to bike_weather_merged.csv...', end='', flush=True)
bw = weather_data.merge(
    bike_agg, 
    on=['year','month','day','hour'], 
    how='left'
)

bw.to_csv(data_path+'bike_weather_merged.csv')
print('done.')

print('merging complete. summary:')
print(bw.info())