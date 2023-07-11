import pandas as pd
import os
from datetime import datetime
import geopy.distance

def calculate_time(row):
    delta = datetime.strptime(row['ended_at'], '%Y-%m-%d %H:%M:%S') - datetime.strptime(row['started_at'], '%Y-%m-%d %H:%M:%S')
    return delta.total_seconds()

def calculate_distance(row):
    start_coords = (row['start_lat'], row['start_lng'])
    end_coords = (row['end_lat'], row['end_lng'])
    return geopy.distance.geodesic(start_coords, end_coords).miles

cwd = os.getcwd()

data = pd.read_csv('/Users/pollyren/Downloads/data_raw.csv', dtype=object, index_col=0)
print('finished reading csv file, {} records read.'.format(len(data)))
# print(data.info())

print('removing missing latitude and longitude value...', end='', flush=True)
data.dropna(subset=['start_lat', 'start_lng', 'end_lat', 'end_lng'], inplace=True)    # drop data points with missing latitudes and longitudes
print('done.')
# print(data.info())

print('converting latitude and longitude to floats...', end='', flush=True)
data['start_lat'] = data['start_lat'].astype(float)
data['start_lng'] = data['start_lng'].astype(float)
data['end_lat'] = data['end_lat'].astype(float)
data['end_lng'] = data['end_lng'].astype(float)
print('done.')

print('performing time calculations...', end='', flush=True)
data['time'] = data.apply(calculate_time, axis=1)
print('done.')
print('performing distance calculations...', end='', flush=True)
data['distance'] = data.apply(calculate_distance, axis=1)
print('done.')

print('writing to data_dist_time.csv...', end='', flush=True)
data.to_csv(cwd + '/data_dist_time.csv')
print('done.')