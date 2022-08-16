import datetime as dt
import pandas as pd
import pytz
from stations_observations import *
import logging


#logging.debug('This message is skipped as a level is set as INFO')
#logging.info('So should this')
#logging.warning('And this, too')
#logging.error('Testing non-ASCII character, Ø and ö')





class Observations_FMI:
    def export_observations(self, bbox_list):
        fin_tz = pytz.timezone('Europe/Helsinki')
        endtime = dt.datetime.utcnow()
        starttime = endtime - dt.timedelta(hours=12)
        starttime_fmi = starttime.isoformat(timespec="seconds") + "Z"
        endtime_fmi = endtime.isoformat(timespec="seconds") + "Z"
        new_observations = pd.DataFrame()
        for bbox in bbox_list:
            lat_left_top = bbox[0]
            long_left_top = bbox[1]
            lat_right_bottom = bbox[2]
            long_right_bottom = bbox[3]
            bbox_fmi = f'{str(long_left_top)},{lat_left_top},{long_right_bottom},{lat_right_bottom}'
            fmi_obs = fmi_observations(starttime_fmi, endtime_fmi, bbox_fmi)
            try:
                new_observations = pd.concat([new_observations, fmi_obs.get_observations()])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Observations for {bbox} were not loaded from FMI.')
        return new_observations

    def export_stations(self):
        stations_fmi = Stations_fmi()
        stations_fmi.export_stations()





class Aeris_Observations:
    def export_observations(self, bbox_list):
        limit = 100
        metrics = ['humidity', 'tempC', 'windDirDEG', 'windSpeedKPH','precipMM']

        fin_tz = pytz.timezone('Europe/Helsinki')
        today = dt.datetime.utcnow()
        #startdate_aeris = today.replace(tzinfo=pytz.utc).astimezone(fin_tz).strftime('%Y/%m/%d')
        #startdate_aeris = '-5days'
        #startdate_aeris = '2022/08/07'
        startdate_aeris = '-12hours'
        new_observations = pd.DataFrame()
        for bbox in bbox_list:
            lat_left_top = bbox[0]
            long_left_top = bbox[1]
            lat_right_bottom = bbox[2]
            long_right_bottom = bbox[3]
            bbox_aeris = f'{str(lat_left_top)},{long_left_top},{lat_right_bottom},{long_right_bottom}'
            aeris_obs = Aeris_observations(bbox_aeris, startdate_aeris, limit)
            try:
                new_observations = pd.concat([new_observations,aeris_obs.get_observations(metrics)])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Observations for {bbox} were not loaded from Aeris. API response: {aeris_obs.get_observations(metrics)}')
        return new_observations

    def export_stations(self, bbox_list):
        limit = 100
        startdate_aeris = '-12hours'
        stations = pd.DataFrame()
        for bbox in bbox_list:
            lat_left_top = bbox[0]
            long_left_top = bbox[1]
            lat_right_bottom = bbox[2]
            long_right_bottom = bbox[3]
            bbox_aeris = f'{str(lat_left_top)},{long_left_top},{lat_right_bottom},{long_right_bottom}'
            aeris_obs = Aeris_observations(bbox_aeris, startdate_aeris, limit)
            try:
                stations = pd.concat([stations, aeris_obs.get_stations()])
            except:
                logging.error(
                    f'{dt.datetime.utcnow()}: Stations for {bbox} were not loaded from Aeris.')
        return stations



#aeris = Aeris_Observations()
#fresh_observations_aeris = aeris.export_observations([[59.373294,17.811224,59.239389,18.324532]])
#print(fresh_observations_aeris)


"""

fmi = Observations_FMI()
fresh_observations_fmi = fmi.export_observations([[60.570482, 21.824370,59.847263, 22.966586],[60.326145,24.544329,60.126988,25.285425]])

"""
#aeris = Aeris_Observations()
#fresh_stations_aeris = aeris.export_stations([[60.570482, 21.824370,59.847263, 22.966586],[60.326145,24.544329,60.126988,25.285425]])
#print(fresh_stations_aeris)










