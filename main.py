from observation_requests import *
from forecast_requests import *
from locations import *
import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import schedule
import time
import sqlite3 as sl
import pandas as pd

oldpwd = os.getcwd()


#Check if a folder "Forecasts" exists in the root directory, if not - create
if not os.path.isdir('Forecasts'):
    os.mkdir('Forecasts')
else:
    pass

#Check if a folder "Observations" exists in the root directory, if not - create
if not os.path.isdir('Observations'):
    os.mkdir('Observations')
else:
    pass


class app():
#____________________________________GET ALL BBOXES______________________________________
#Method get_bbox helps to get an array of all bboxes listed in bbox_list.csv

    def get_bbox(self):
        bbox = pd.read_excel('bbox_list.xlsx',  sheet_name='bbox_list', index_col=None, header=0)
        bbox_list = []
        for index, row in bbox.iterrows():
            bbox_i = []
            bbox_i.append(row['tl_lat'])
            bbox_i.append(row['tl_lon'])
            bbox_i.append(row['br_lat'])
            bbox_i.append(row['br_lon'])
            bbox_list.append(bbox_i)
        return bbox_list

#____________________________________GET OBSERVATIONS______________________________________
#____________________________________Get max avaiable date with an observations per station id and the source
    def get_max_observation_date(self):
        con = sl.connect('observations.db')
        try:
            max_date = pd.read_sql('''
            SELECT id, source, max(datetime) as max_datetime
            FROM OBSERVATIONS
            GROUP BY id, source
            ''', con)
        except sl.OperationalError:
            max_date = pd.DataFrame
        max_date['max_datetime'] = pd.to_datetime(max_date['max_datetime'])
        return max_date


#Method import_observations allows to export past observations for all listed bboxes from FMI and Aeris services

    def import_observations(self):
        bbox_list = self.get_bbox()
# FMI Observations
        fmi = Observations_FMI()
        fresh_observations_fmi = fmi.export_observations(bbox_list)
        #os.chdir(os.getcwd() + '\Observations')
        #try:
        #    old_observations_fmi = pd.read_csv('fmi_observations_full.csv')
        #    old_observations_fmi['datetime'] = pd.to_datetime(old_observations_fmi['datetime'])
        #except:
        #    old_observations_fmi = pd.DataFrame()
        fresh_observations_fmi = fresh_observations_fmi.merge(self.get_max_observation_date(),
                                                              on=['id','source'], how='left')
        fresh_observations_fmi_new = fresh_observations_fmi[(fresh_observations_fmi['datetime']>fresh_observations_fmi['max_datetime'])|(fresh_observations_fmi['max_datetime'].isna())]

        #combined_observations_fmi = pd.concat([old_observations_fmi, fresh_observations_fmi]).drop_duplicates()
        #combined_observations_fmi.to_csv('fmi_observations_full.csv', index=False)
        #os.chdir(oldpwd)
        con = sl.connect('observations.db')
        fresh_observations_fmi_new = fresh_observations_fmi_new.drop('max_datetime', axis=1)
        fresh_observations_fmi_new.to_sql('OBSERVATIONS', con, if_exists='append')
        fmi.export_stations()

# Aeris Observations
        os.chdir(os.getcwd() + '\Observations')
        aeris = Aeris_Observations()
        fresh_observations_aeris = aeris.export_observations(bbox_list)
        fresh_stations_aeris = aeris.export_stations(bbox_list)
        try:
            old_observations_aeris = pd.read_csv('aeris_observations_full.csv')
            old_observations_aeris['datetime'] = pd.to_datetime(old_observations_aeris['datetime'])
        except:
            old_observations_aeris = pd.DataFrame()
        combined_observations_aeris = pd.concat([old_observations_aeris, fresh_observations_aeris]).drop_duplicates()
        combined_observations_aeris.to_csv('aeris_observations_full.csv', index=False)
        os.chdir(oldpwd)
        try:
            old_stations_aeris = pd.read_csv('aeris_stations.csv')
        except:
            old_stations_aeris = pd.DataFrame()
        combined_stations_aeris = pd.concat([old_stations_aeris, fresh_stations_aeris])
        combined_stations_aeris['properties'] = combined_stations_aeris['properties'].astype(str)
        combined_stations_aeris = combined_stations_aeris.drop_duplicates(subset=None, keep="first", inplace=False)
        combined_stations_aeris.to_csv('aeris_stations.csv', index=False)



#_______________________________GET FORECASTS_____________________________________________
#Method import_forecasts allows to export available future forecasts for all
#coordinates available in the past saved observations from all the sources
    def import_forecasts(self):
        #first we load the array of all ground truth coordinates
        oldpwd = os.getcwd()
        os.chdir(oldpwd)
        ground_truth_locations_list = ground_truth_locations()
        locations_list = ground_truth_locations_list.export_locations()

        #AERIS FORECAST
        try:
            aeris_forecast = Forecasts_Aeris()
            aeris_new_forecast = aeris_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            aeris_new_forecast.to_csv(f'aeris_new_forecasts_full{now}.csv', index=False)
        except:
            print("Aeris Forecast was not reloaded")

        #FMI FORECAST
        try:
            fmi_forecast = Forecasts_FMI()
            fmi_new_forecast = fmi_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            fmi_new_forecast.to_csv(f'fmi_new_forecasts_full{now}.csv', index=False)
        except:
            print("FMI Forecast was not reloaded")

        #YR FORECAST
        try:
            yr_forecast = Forecasts_YR()
            yr_new_forecast = yr_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            yr_new_forecast.to_csv(f'yr_new_forecasts_full{now}.csv', index=False)
        except:
            print("YR Forecast was not reloaded")

        #ACCUWEATHER FORECAST
        try:
            accuweather_forecast = Forecasts_Accuweather()
            accuweather_new_forecast = accuweather_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            accuweather_new_forecast.to_csv(f'accuweather_new_forecasts_full{now}.csv', index=False)
        except:
            print("Accuweather Forecast was not reloaded")

        #THE WEATHER CHANNEL FORECAST
        try:
            the_weather_channel_forecast = Forecasts_Weather_Channel()
            the_weather_channel_new_forecast = the_weather_channel_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            the_weather_channel_new_forecast.to_csv(f'weather_channel_new_forecasts_full{now}.csv', index=False)
        except:
            print("The Weather Channel forecast was not reloaded")

        #METEOMATICS FORECAST
        try:
            meteomatics = Meteomatics()
            meteomatics_new_forecast = meteomatics.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            meteomatics_new_forecast.to_csv(f'meteomatics_new_forecasts_full{now}.csv', index=False)
        except:
            print("Meteomatics forecast was not reloaded")


def create_timed_rotating_log(path):
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(path,
                                       when="h",
                                       interval=3,
                                       backupCount=5)
    logger.addHandler(handler)



def main_body():
    os.chdir(oldpwd)
    log_file = "log_file.log"
    create_timed_rotating_log(log_file)
    #now_log = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    #logging_name = f"log_{now_log}.txt"
    logging.basicConfig(filename=log_file)
    application = app()
    application.import_observations()
    application.import_forecasts()



#schedule.every(3).hours.at(":01").do(main_body)
#while True:
#    schedule.run_pending()
#    time.sleep(1)


main_body()






