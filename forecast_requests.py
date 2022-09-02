import pandas as pd
from forecasts import fmi_forecasts, aeris_forecasts, yr_forecasts, Accuweather_forecast, weather_channel_forecast, Meteomatics_forecast
import logging
import datetime as dt


#get Aeris Forecasts for multiple coordinates and adjust the layout
class Forecasts_Aeris:

    def export_forecasts(self, locations_list):
        limit = 48
        metrics = ['humidity', 'tempC', 'windDirDEG', 'windSpeedKPH','precipMM']
        new_forecasts = pd.DataFrame()
        for index, row in locations_list.iterrows():
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            lon = locations_list.loc[index, 'lon']
            Aeris_Forecasts = aeris_forecasts(lon=lon, lat=lat, limit=limit)
            try:
                aeris_forecast_i = Aeris_Forecasts.get_forecasts(metrics)
                aeris_forecast_i['id'] = id
                aeris_forecast_i['source'] = source
                new_forecasts = pd.concat([new_forecasts, aeris_forecast_i])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from Aeris')
        new_forecasts = new_forecasts.pivot(index=['id', 'lat', 'lon', 'source', 'timezone', 'collection_datetime', 'datetime', 'fc_leadtime'],
                                            columns='metric_name', values='metric_value').reset_index()
        new_forecasts = new_forecasts.rename(columns={"tempC": "temp_2", "humidity": "rhum_2", "windSpeedKPH" : "winds_10", "windDirDEG" : "windd_10"})
        return new_forecasts





#get FMI Forecasts for multiple coordinates and adjust the layout
class Forecasts_FMI:

    def export_forecasts(self, locations_list):
        new_forecasts = pd.DataFrame()
        for index, row in locations_list.iterrows():
            lon = locations_list.loc[index, 'lon']
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            FMI_Forecasts = fmi_forecasts(lon=lon, lat=lat)
            try:
                fmi_forecast_i = FMI_Forecasts.get_forecasts()
                fmi_forecast_i['id'] = id
                fmi_forecast_i['source'] = source
                fmi_forecast_i['lat'] = lat
                fmi_forecast_i['lon'] = lon
                new_forecasts = pd.concat([new_forecasts, fmi_forecast_i])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from FMI')
        new_forecasts = new_forecasts.pivot(index=['id', 'lat', 'lon', 'source', 'timezone', 'collection_datetime', 'datetime','fc_leadtime'],
                                            columns='metric_name', values='metric_value').reset_index()
        new_forecasts = new_forecasts.rename(
            columns={"mts-1-1-Temperature": "temp_2", "mts-1-1-Humidity": "rhum_2", "mts-1-1-WindSpeedMS": "winds_10", "mts-1-1-WindDirection": "windd_10"})
        return new_forecasts



#get YR Forecasts for multiple coordinates and adjust the layout
class Forecasts_YR:

    def export_forecasts(self, locations_list):
        new_forecasts = pd.DataFrame()
        metrics = ['air_temperature', 'relative_humidity', 'wind_from_direction', 'wind_speed','precipitation_amount', 'probability_of_precipitation']
        for index, row in locations_list.iterrows():
            lon = locations_list.loc[index, 'lon']
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            YR_Forecasts = yr_forecasts(lon=lon, lat=lat)
            try:
                yr_forecast_i = YR_Forecasts.get_forecasts(metrics)
                yr_forecast_i['id'] = id
                yr_forecast_i['source'] = source
                yr_forecast_i['lat'] = lat
                yr_forecast_i['lon'] = lon
                new_forecasts = pd.concat([new_forecasts,yr_forecast_i])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from YR')
        new_forecasts = new_forecasts.pivot(index=['id', 'lat', 'lon', 'source', 'collection_datetime', 'datetime','fc_leadtime'],
                                            columns='metric_name', values='metric_value').reset_index()
        new_forecasts = new_forecasts.rename(columns={"air_temperature": "temp_2", "relative_humidity": "rhum_2",
                                                      "wind_speed": "winds_10","wind_from_direction": "windd_10"})
        return new_forecasts

#get Accuweather Forecasts for multiple coordinates and adjust the layout
class Forecasts_Accuweather:

    def export_forecasts(self, locations_list):
        new_forecasts = pd.DataFrame()
        metrics = ['Temperature','Wind', 'RelativeHumidity','PrecipitationProbability','TotalLiquid','HasPrecipitation']
        for index, row in locations_list.iterrows():
            lon = locations_list.loc[index, 'lon']
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            Accu_Forecasts = Accuweather_forecast(lon=lon, lat=lat)
            try:
                accu_forecast_i = Accu_Forecasts.get_forecasts(metrics)
                accu_forecast_i['id'] = id
                accu_forecast_i['source'] = source
                accu_forecast_i['lat'] = lat
                accu_forecast_i['lon'] = lon
                new_forecasts = pd.concat([new_forecasts, accu_forecast_i])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from Accuweather')
        new_forecasts = new_forecasts.pivot(index=['id', 'lat', 'lon', 'source', 'collection_datetime', 'datetime','fc_leadtime'],
                                            columns='metric_name', values='metric_value').reset_index()
        new_forecasts = new_forecasts.rename(columns={"Temperature": "temp_2", "RelativeHumidity": "rhum_2",
                                                      "Wind_Speed": "winds_10","Wind_Direction": "windd_10"})
        return new_forecasts


#get The Weather Channel Forecasts for multiple coordinates and adjust the layout
class Forecasts_Weather_Channel:

    def export_forecasts(self, locations_list):
        new_forecasts = pd.DataFrame()
        metrics = ['relativeHumidity', 'temperature', 'windDirection', 'windSpeed','precipChance']
        for index, row in locations_list.iterrows():
            lon = locations_list.loc[index, 'lon']
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            wc_forecast = weather_channel_forecast(lon=lon, lat=lat)
            try:
                wc_forecast_i = wc_forecast.get_forecasts(metrics)
                wc_forecast_i['id'] = id
                wc_forecast_i['source'] = source
                wc_forecast_i['lat'] = lat
                wc_forecast_i['lon'] = lon
                new_forecasts = pd.concat([new_forecasts,wc_forecast_i])
            except:
                logging.error(f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from the Weather Channel')
        new_forecasts = new_forecasts.rename(columns={"temperature": "temp_2", "relativeHumidity": "rhum_2",
                                                      "windSpeed": "winds_10","windDirection": "windd_10"})
        return new_forecasts

#get Meteomatics Forecasts for multiple coordinates and adjust the layout
class Meteomatics:

    #def create_list(column):
    #    coordinate_list = []
    #    it = iter(column)
    #    zip(it, it)
    #    return it

    def export_forecasts(self, locations_list):
        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))

        new_forecasts = pd.DataFrame()
        metrics = ['t_2m:C', 'relative_humidity_2m:p', 'wind_dir_10m:d', 'wind_speed_10m:ms','precip_1h:mm']

        locations_list['latlon'] = list(zip(locations_list['lat'], locations_list['lon']))
        for chunk in chunker(locations_list, 10):
            point_list = list(chunk['latlon'])
            id_list = list(chunk['id'])
            source_list = list(chunk['source'])
            meteomatics_forecast = Meteomatics_forecast(coordinates=point_list)
            try:
                fc = meteomatics_forecast.get_data(metrics)
                new_forecasts = pd.concat((new_forecasts, fc))
            except:
                logging.error(
                    f'{dt.datetime.utcnow()}: Forecast for some of {id_list} from {source_list} observation source was not loaded from Meteomatics')
        new_forecasts = new_forecasts.merge(locations_list[['lat', 'lon', 'id','source']], on=['lat', 'lon'], how='left')
        new_forecasts = new_forecasts.rename(columns={'t_2m:C': "temp_2", 'relative_humidity_2m:p': "rhum_2",
                                                      'wind_speed_10m:ms': "winds_10", 'wind_dir_10m:d': "windd_10",
                                                      'validdate': 'datetime'})
        return new_forecasts

"""
        for index, row in locations_list.iterrows():
            lon = locations_list.loc[index, 'lon']
            lat = locations_list.loc[index, 'lat']
            id = locations_list.loc[index, 'id']
            source = locations_list.loc[index, 'source']
            point = (lat, lon)
            meteomatics_forecast = Meteomatics_forecast(coordinates = [point])
            try:
                mm_forecast_i = meteomatics_forecast.get_forecasts(metrics)
                mm_forecast_i['id'] = id
                mm_forecast_i['source'] = source
                mm_forecast_i['lat'] = lat
                mm_forecast_i['lon'] = lon
                new_forecasts = pd.concat([new_forecasts, mm_forecast_i])
            except:
                logging.error(
                    f'{dt.datetime.utcnow()}: Forecast for {id} of {source} observation source was not loaded from Meteomatics')
        new_forecasts = new_forecasts.rename(columns={'t_2m:C': "temp_2", 'relative_humidity_2m:p': "rhum_2",
                                                      'wind_speed_10m:ms': "winds_10", 'wind_dir_10m:d': "windd_10",
                                                      'validdate': 'datetime'})
        return new_forecasts


"""
