import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import os

trips = ['202004-divvy-tripdata.zip', 
        '202005-divvy-tripdata.zip', 
        '202006-divvy-tripdata.zip', 
        '202007-divvy-tripdata.zip', 
        '202008-divvy-tripdata.zip', 
        '202009-divvy-tripdata.zip', 
        '202010-divvy-tripdata.zip', 
        '202011-divvy-tripdata.zip', 
        '202012-divvy-tripdata.zip', 
        '202101-divvy-tripdata.zip', 
        '202102-divvy-tripdata.zip', 
        '202103-divvy-tripdata.zip', 
        '202104-divvy-tripdata.zip', 
        '202105-divvy-tripdata.zip', 
        '202106-divvy-tripdata.zip', 
        '202107-divvy-tripdata.zip', 
        '202108-divvy-tripdata.zip', 
        '202109-divvy-tripdata.zip', 
        '202110-divvy-tripdata.zip', 
        '202111-divvy-tripdata.zip', 
        '202112-divvy-tripdata.zip', 
        '202201-divvy-tripdata.zip', 
        '202202-divvy-tripdata.zip', 
        '202203-divvy-tripdata.zip', 
        '202204-divvy-tripdata.zip', 
        '202205-divvy-tripdata.zip', 
        '202206-divvy-tripdata.zip', 
        '202207-divvy-tripdata.zip', 
        '202208-divvy-tripdata.zip', 
        '202209-divvy-tripdata.zip', 
        '202210-divvy-tripdata.zip', 
        '202211-divvy-tripdata.zip', 
        '202212-divvy-tripdata.zip', 
        '202301-divvy-tripdata.zip', 
        '202302-divvy-tripdata.zip', 
        '202303-divvy-tripdata.zip', 
        '202304-divvy-tripdata.zip', 
        '202305-divvy-tripdata.zip']

df = pd.DataFrame()

for trip in trips:
    link = urlopen('https://divvy-tripdata.s3.amazonaws.com/' + str(trip))
    zf = ZipFile(BytesIO(link.read()))
    name = zf.namelist()[0]
    tmp_df = pd.read_csv(zf.open(name), dtype=object)
    zf.close()
    df = pd.concat([df, tmp_df], ignore_index=True, sort=False)
    print('{} completed, {} rows added'.format(name, len(tmp_df)))

print(df.head())
print(df.tail())
print(len(df))
print(df.info())

dst = os.getcwd() + '/data_raw.csv'
df.to_csv(dst)

print('Dataframe saved at {}... done.'.format(dst))