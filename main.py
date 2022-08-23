from observation_requests import *
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
        con_fmi = sl.connect('observations_fmi.db')
        fresh_observations_fmi_new = fresh_observations_fmi_new.drop('max_datetime', axis=1)
        fresh_observations_fmi_new.to_sql('OBSERVATIONS', con_fmi, if_exists='append')
        fmi.export_stations()


# Aeris Observations
        #os.chdir(os.getcwd() + '\Observations')
        aeris = Aeris_Observations()
        fresh_observations_aeris = aeris.export_observations(bbox_list)
        fresh_stations_aeris = aeris.export_stations(bbox_list)
        #try:
        #    old_observations_aeris = pd.read_csv('aeris_observations_full.csv')
        #    old_observations_aeris['datetime'] = pd.to_datetime(old_observations_aeris['datetime'])
        #except:
        #    old_observations_aeris = pd.DataFrame()
        max_date_obs = self.get_max_observation_date()
        max_date_obs['id'] = max_date_obs['id'].astype(str)
        fresh_observations_aeris = fresh_observations_aeris.merge(max_date_obs,
                                                                  on=['id', 'source'], how='left')
        fresh_observations_aeris_new = fresh_observations_aeris[(fresh_observations_aeris['datetime'] > fresh_observations_aeris['max_datetime']) | (fresh_observations_aeris['max_datetime'].isna())]
        con_aeris = sl.connect('observations_aeris.db')
        fresh_observations_aeris_new = fresh_observations_aeris_new.drop('max_datetime', axis=1)
        fresh_observations_aeris_new.to_sql('OBSERVATIONS', con_aeris, if_exists='append')
        #combined_observations_aeris = pd.concat([old_observations_aeris, fresh_observations_aeris]).drop_duplicates()
        #combined_observations_aeris.to_csv('aeris_observations_full.csv', index=False)
        #os.chdir(oldpwd)
        try:
            old_stations_aeris = pd.read_csv('aeris_stations.csv')
        except:
            old_stations_aeris = pd.DataFrame()
        combined_stations_aeris = pd.concat([old_stations_aeris, fresh_stations_aeris])
        combined_stations_aeris['properties'] = combined_stations_aeris['properties'].astype(str)
        combined_stations_aeris = combined_stations_aeris.drop_duplicates(subset=None, keep="first", inplace=False)
        combined_stations_aeris.to_csv('aeris_stations.csv', index=False)
        """
        try:
            old_stations_aeris = pd.read_sql('SELECT * FROM STATIONS', con_aeris)
            combined_stations_aeris = pd.concat([old_stations_aeris, fresh_stations_aeris])
            combined_stations_aeris['properties'] = combined_stations_aeris['properties'].astype(str)
            combined_stations_aeris = combined_stations_aeris.drop_duplicates(subset=None, keep="first", inplace=False)
            combined_stations_aeris.to_sql(name='STATIONS', con=con_aeris, if_exists='replace', index=False)
        except:
            fresh_stations_aeris.to_sql(name='STATIONS', con=con_aeris, if_exists='replace', index=False)
        """



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




schedule.every(3).hours.at(":01").do(main_body)
while True:
    schedule.run_pending()
    time.sleep(1)


#main_body()






