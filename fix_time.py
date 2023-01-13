import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from tqdm import tqdm
import swifter
import geopandas as gpd
from shapely.geometry import Point

datapath = './data/'
data0 = pd.read_csv(datapath + 'data0.csv', encoding='gbk')
data1 = pd.read_csv(datapath + 'data1.csv', encoding='gbk')
station = pd.read_csv(datapath + 'station.csv')

station = station.sort_values('name').reset_index(drop = True)
pre_wdlist = station.loc[0,'name'].split('(')
for i,row in station.iterrows():
    if i == 0:
        continue
    wdlist = row['name'].split('(')
    if (wdlist[0] == pre_wdlist[0]):
        station.loc[i,'lon'] = np.nan
    pre_wdlist = wdlist
station = station.dropna()
for i,row in station.iterrows():
    wdlist = row['name'].split('(')
    station.loc[i,'name'] = wdlist[0][:-1]
    
station = station.sort_values('name').reset_index(drop = True)
station = gpd.GeoDataFrame(station)
station['geometry'] = station.swifter.apply(lambda row : Point(row['lon'], row['lat']), axis=1)

data0['name'].replace(['泰康', '鳌江', '南平北', '武夷山东'],
                        ['杜尔伯特', '平阳', '延平', '南平市'],inplace = True)
data1['name'].replace(['泰康', '鳌江', '南平北', '武夷山东'],
                        ['杜尔伯特', '平阳', '延平', '南平市'], inplace = True)

data0 = pd.merge(left=data0,right=station,left_on='name',right_on='name',
            how='left')
data1 = pd.merge(left=data1,right=station,left_on='name',right_on='name',
            how='left')
data = pd.concat([data0,data1]).reset_index(drop=True)
del data0,data1

def gen_std_arrive(row):
    if row['late_arrive'] >= 0:
        delta = pd.Timedelta(minutes=row['late_arrive'])
        row['std_arrive'] = row['real_arrive'] - delta
    else:
        delta = pd.Timedelta(minutes=-row['late_arrive'])
        row['std_arrive'] = row['real_arrive'] + delta
    return row['std_arrive']

def gen_std_depart(row):
    if row['late_depart'] >= 0:
        delta = pd.Timedelta(minutes=row['late_depart'])
        row['std_depart'] = row['real_depart'] - delta
    else:
        delta = pd.Timedelta(minutes=-row['late_depart'])
        row['std_depart'] = row['real_depart'] + delta
    return row['std_depart']

    
def generate_time(df):
    df.loc[df.loc[df['std_arrive']=='----'].index, 'std_arrive'] = '00:00'
    df.loc[df.loc[df['std_depart']=='----'].index, 'std_depart'] = '00:00'
    mid = pd.Series([' ' for _ in range(len(df))])
    df['std_arrive'] = df['std_arrive'] + mid + df['date']
    df['real_arrive'] = df['real_arrive'] + mid + df['date']
    df['std_depart'] = df['std_depart'] + mid + df['date']
    df['real_depart'] = df['real_depart'] + mid + df['date']
    print('transferred')
    
    dtformat='%H:%M %Y/%m/%d'
    df['std_arrive'] = pd.to_datetime(df['std_arrive'],
                                    format=dtformat)
    df['real_arrive'] = pd.to_datetime(df['real_arrive'],
                                    format=dtformat)
    df['std_depart'] = pd.to_datetime(df['std_depart'],
                                    format=dtformat)
    df['real_depart'] = pd.to_datetime(df['real_depart'],
                                    format=dtformat)
    print('fixing')
    tqdm.pandas(desc='std_arrive')
    # df['std_arrive'] = df.swifter.apply(gen_std_arrive)
    # df['std_arrive'] = df.swifter.progress_apply(gen_std_arrive,axis=1)
    df['std_arrive'] = df.swifter.apply(gen_std_arrive,axis=1)
    
    tqdm.pandas(desc='std_depart')
    # df['std_depart'] = df.swifter.apply(gen_std_depart)
    # df['std_depart'] = df.swifter.progress_apply(gen_std_depart,axis=1)
    df['std_depart'] = df.swifter.apply(gen_std_depart,axis=1)
    return df

data = generate_time(data)
data.to_pickle('fix_time_data.pkl')