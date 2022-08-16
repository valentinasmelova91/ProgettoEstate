import os
import pandas as pd
import datetime
import pytz
import numpy as np
import seaborn as sns

oldpwd = os.getcwd()
class all_forecasts:
    def get_forecasts(self):
        os.chdir(os.getcwd())
        os.chdir(os.getcwd() + '\Forecasts')
        forecasts = pd.DataFrame()
        path_of_the_directory = os.getcwd()
        for filename in os.listdir(path_of_the_directory):
            f = os.path.join(filename)
            if os.path.isfile(f):
                forecast_i = pd.read_csv(filename)
                forecast_i['file_name'] = filename
                try:
                    fc['id'] = fc['id'].astype(float)
                except:
                    pass
                forecasts = pd.concat([forecasts,forecast_i[['id', 'lat', 'lon', 'source', 'collection_datetime', 'datetime','rhum_2', 'temp_2', 'windd_10', 'winds_10', 'fc_leadtime','file_name']]])
            else:
                pass
        os.chdir(os.getcwd())

        return forecasts

class all_observations:
    def get_observations(self):
        os.chdir(oldpwd)
        os.chdir(os.getcwd() + '\Observations')
        observations = pd.DataFrame()
        path_of_the_directory = os.getcwd()
        for filename in os.listdir(path_of_the_directory):
            f = os.path.join(filename)
            if os.path.isfile(f):
                observation_i = pd.read_csv(filename)
                observation_i['file_name'] = filename
                observations = pd.concat([observations,observation_i])
            else:
                pass
        os.chdir(os.getcwd())

        return observations



all_forecasts= all_forecasts()
fc = all_forecasts.get_forecasts()

now = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
fc['datetime'] = fc['datetime'].apply(lambda x: datetime.datetime.fromisoformat(x))
fc = fc[fc['datetime'] < now ].reset_index(drop=True)
#fc['id'] = fc['id'].apply(lambda x: int(x))
#fc['id'] = fc['id'].astype(float)


all_observations = all_observations()
obs = all_observations.get_observations()
obs['datetime'] = obs['datetime'].apply(lambda x: datetime.datetime.fromisoformat(x))
obs['id'] = obs['id'].astype(str)
obs['rounded_date'] = obs['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
fc['rounded_date'] = fc['datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))


print(fc['rounded_date'].min())

fc['provider'] = fc['file_name'].apply(lambda x: x[:len(x)-37])
providers = fc['provider'].unique()
data_error = pd.DataFrame()
for provider in providers:
    fc_i = fc[fc['provider'] == provider]
    data_comb = obs.merge(fc_i, on=['id', 'rounded_date'])
    data_comb['delta'] = (data_comb['datetime_x'] - data_comb['datetime_y']).dt.total_seconds()
    data_comb['delta'] = data_comb['delta'].apply(lambda x: np.abs(x))
    data_comb_1 = pd.DataFrame(data_comb[['id', 'datetime_y', 'delta']].groupby(['id', 'datetime_y']).min()).reset_index()
    data_comb_1 = data_comb_1.rename(columns={'delta': 'min_delta'})
    data_comb_2 = pd.merge(data_comb, data_comb_1, on=['id', 'datetime_y'])
    data_comb_3 = data_comb_2[data_comb_2['delta'] == data_comb_2['min_delta']].reset_index(drop=True)
    data_comb_3['temp_2_error'] = data_comb_3['temp_2_x'] - data_comb_3['temp_2_y']
    data_comb_3['rhum_2_error'] = data_comb_3['rhum_2_x'] - data_comb_3['rhum_2_y']
    data_comb_3['windd_10_error'] = data_comb_3['windd_10_x'] - data_comb_3['windd_10_y']
    data_comb_3['winds_10_error'] = data_comb_3['winds_10_x'] - data_comb_3['winds_10_y']
    data_error = pd.concat([data_error,data_comb_3])
    print(provider)
print(data_error['fc_leadtime'].count())
os.chdir(oldpwd)
data_error.to_csv('forecast_error_new.csv')







"""
data_comb = obs.merge(fc, on=['id','rounded_date'])

data_comb['delta'] = (data_comb['datetime_x'] - data_comb['datetime_y']).dt.total_seconds()
data_comb['delta'] = data_comb['delta'].apply(lambda x: np.abs(x))
data_comb_1 = pd.DataFrame(data_comb[['id','datetime_y','delta']].groupby(['id','datetime_y']).min()).reset_index()
data_comb_1 = data_comb_1.rename(columns={'delta':'min_delta'})
data_comb_2 = pd.merge(data_comb,data_comb_1, on =['id','datetime_y'])
data_comb_3 = data_comb_2[data_comb_2['delta'] == data_comb_2['min_delta']].reset_index(drop=True)
data_comb_3['temp_2_error'] = data_comb_3['temp_2_x']-data_comb_3['temp_2_y']
data_comb_3['rhum_2_error'] = data_comb_3['rhum_2_x']-data_comb_3['rhum_2_y']
data_comb_3['windd_10_error'] = data_comb_3['windd_10_x']-data_comb_3['windd_10_y']
data_comb_3['winds_10_error'] = data_comb_3['winds_10_x']-data_comb_3['winds_10_y']

#'rhum_2_x', 'temp_2_x', 'windd_10_x', 'winds_10_x'
#print(data_comb_3[['datetime_y','temp_2_error','file_name_y']])
#sns.lineplot(x='fc_leadtime',y='temp_2_error', data=data_comb_3, hue='file_name_y')
print(data_comb_3['fc_leadtime'].count())
os.chdir(oldpwd)
data_comb_3.to_csv('forecast_error.csv')

"""
