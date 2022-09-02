import requests as req
import xml.etree.ElementTree as ET
import pandas as pd
import json
import numpy as np
import datetime
import pytz
import meteomatics.api as mmapi
import os



#Forecasts from FMI (Finnish Meteorological Institute)
#query_id=fmi::forecast::harmonie::hybrid::point::timevaluepair

class fmi_forecasts:

    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

    def get_forecasts(self):
        url_fmi_base = 'https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::forecast::harmonie::hybrid::point::timevaluepair&latlon='
        url_fmi = url_fmi_base + str(self.lat)+','+str(self.lon)
        request = req.get(url=url_fmi)
        output = request.text
        root = ET.fromstring(output)
        parameter_list = []
        time_list = []
        value_list = []
        time_series = root.findall(".//{http://www.opengis.net/waterml/2.0}MeasurementTimeseries")
        parameter_list_i = []
        time_list_i = []
        value_list_i = []
        for ts in time_series:
            parameter = ts.attrib['{http://www.opengis.net/gml/3.2}id']
            points = ts.findall(".//{http://www.opengis.net/waterml/2.0}MeasurementTVP")
            for point in points:
                times = point.findall(".//{http://www.opengis.net/waterml/2.0}time")
                values = point.findall(".//{http://www.opengis.net/waterml/2.0}value")
                for time in times:
                    values = point.findall(".//{http://www.opengis.net/waterml/2.0}value")
                    for value in values:
                        parameter_list_i.append(parameter)
                        time_list_i.append(datetime.datetime.strptime(time.text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC))
                        value_list_i.append(value.text)
        parameter_list = np.concatenate((parameter_list,parameter_list_i))
        time_list = np.concatenate((time_list, time_list_i))
        value_list = np.concatenate((value_list, value_list_i))
        fmi_forecasts = list(zip(parameter_list, time_list, value_list))
        fmi_forecasts_dataframe = pd.DataFrame(fmi_forecasts, columns=['metric_name', 'datetime', 'metric_value'])
        fmi_forecasts_dataframe['collection_datetime'] = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        fmi_forecasts_dataframe['fc_leadtime'] = (fmi_forecasts_dataframe['datetime'] - fmi_forecasts_dataframe['collection_datetime']).dt.total_seconds() / 3600
        fmi_forecasts_dataframe['fc_leadtime'] = fmi_forecasts_dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        fmi_forecasts_dataframe['timezone'] = 'Europe/Helsinki'
        return fmi_forecasts_dataframe

    def export_forecasts(self):
        self.get_forecasts().to_csv('fmi_forecasts.csv', index=False)



#Forecasts from Aeris

class aeris_forecasts:

    def __init__(self, lat, lon, limit):
        self.lon = lon
        self.lat = lat
        self.limit = limit

    def get_data(self):
        url_base = 'https://api.aerisapi.com/forecasts/' + str(self.lat) + ',' + str(self.lon) + '?format=json&filter=1hr&limit=' + str(self.limit) + '&'
        secret_key = 'O2U57aPy30qn6TRhMogxUNPgYs6zDX23rXTvOKuy'
        client_id = '0aeGF7FKmqjeuiTlyxuvN'
        url = url_base + 'client_id=' + client_id + '&client_secret=' + secret_key
        response = req.get(url=url)
        json_file = json.loads(response.text)
        return json_file

    def get_forecasts(self,input_metrics):
        fc_long = []
        fc_lat = []
        fc_period = []
        fc_tz = []
        fc_metric_name = []
        fc_metric_value = []
        if input_metrics == 'all':
            metrics = list(self.get_data()['response'][0]['periods'][0].keys())
            #'timestamp', 'validTime', 'dateTimeISO' - these time stamps must be excluded from the metric list
        else:
            metrics = input_metrics
        response = self.get_data()
        periods = response['response'][0]['periods']
        lat = response['response'][0]['loc']['lat']
        long = response['response'][0]['loc']['long']
        tz = response['response'][0]['profile']['tz']
        for period in periods:
            fc_long_i = []
            fc_lat_i = []
            fc_tz_i = []
            fc_period_i = []
            fc_metric_name_i = []
            fc_metric_value_i = []
            for metric in metrics:
                fc_long_i.append(long)
                fc_lat_i.append(lat)
                fc_tz_i.append(tz)
                fc_period_i.append(datetime.datetime.fromisoformat(period['dateTimeISO']).astimezone(pytz.utc))
                fc_metric_name_i.append(metric)
                if metric == 'windSpeedKPH':
                    fc_metric_value_i.append(round(period[metric]*1000/3600, 1))
                else:
                    fc_metric_value_i.append(period[metric])
            fc_lat = np.concatenate((fc_lat, fc_lat_i))
            fc_long = np.concatenate((fc_long, fc_long_i))
            fc_tz = np.concatenate((fc_tz, fc_tz_i))
            fc_period = np.concatenate((fc_period, fc_period_i))
            fc_metric_name = np.concatenate((fc_metric_name, fc_metric_name_i))
            fc_metric_value = np.concatenate((fc_metric_value, fc_metric_value_i))
        aeris_forecasts_loc = list(zip(fc_long, fc_lat, fc_period, fc_metric_name, fc_metric_value,fc_tz))
        aeris_forecasts_loc_dataframe = pd.DataFrame(aeris_forecasts_loc,
                                                    columns=['lon','lat', 'datetime', 'metric_name', 'metric_value','timezone'])
        aeris_forecasts_loc_dataframe['collection_datetime'] = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        aeris_forecasts_loc_dataframe['fc_leadtime'] = (aeris_forecasts_loc_dataframe['datetime'] - aeris_forecasts_loc_dataframe['collection_datetime']).dt.total_seconds() / 3600
        aeris_forecasts_loc_dataframe['fc_leadtime'] = aeris_forecasts_loc_dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        return aeris_forecasts_loc_dataframe

    def export_forecasts(self, metrics):
        self.get_forecasts(metrics).to_csv('aeris_forecasts.csv', index=False)


#Forecasts from YR

class yr_forecasts:

    def __init__(self, lat, lon):
        self.lon = lon
        self.lat = lat

    def get_data(self):
        url_weather = f'https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={self.lat}&lon={self.lon}'
        headers = {'User-Agent': 'ProgettoEstate/1.0 github.com/ProgettoEstate'}
        response = req.get(url=url_weather, headers=headers)
        json_data = json.loads(response.text)
        return json_data

    def get_forecasts(self, input_metrics):
        timeseries = self.get_data()['properties']['timeseries']
        fc_period_i = []
        fc_metric_value_i = []
        fc_metric_name_i = []
        enddate_yr = datetime.datetime.utcnow() + datetime.timedelta(hours=48)
        if input_metrics == 'all':
            metrics = list(timeseries[0]['data']['instant']['details'].keys())
        else:
            metrics = input_metrics
        for timeserie in timeseries:
            for metric in metrics:
                if metric in ['precipitation_amount', 'probability_of_precipitation']:
                    fc_period_i.append(datetime.datetime.strptime(timeserie['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC))
                    fc_metric_name_i.append(metric)
                    try:
                        fc_metric_value_i.append(timeserie['data']['next_1_hours']['details'][metric])
                    except:
                        fc_metric_value_i.append(None)
                else:
                    fc_period_i.append(datetime.datetime.strptime(timeserie['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC))
                    fc_metric_name_i.append(metric)
                    fc_metric_value_i.append(timeserie['data']['instant']['details'][metric])
        yr_forecasts_loc = list(zip(fc_period_i, fc_metric_name_i, fc_metric_value_i))
        yr_forecasts_loc_dataframe = pd.DataFrame(yr_forecasts_loc,
                                                  columns=['datetime', 'metric_name', 'metric_value'])
        yr_forecasts_loc_dataframe['collection_datetime'] = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        yr_forecasts_loc_dataframe['fc_leadtime'] = (yr_forecasts_loc_dataframe['datetime'] - yr_forecasts_loc_dataframe['collection_datetime']).dt.total_seconds() / 3600
        yr_forecasts_loc_dataframe['fc_leadtime'] = yr_forecasts_loc_dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        return yr_forecasts_loc_dataframe[yr_forecasts_loc_dataframe['datetime'] < enddate_yr.replace(tzinfo=pytz.UTC)]
#'air_pressure_at_sea_level', 'air_temperature', 'cloud_area_fraction', 'cloud_area_fraction_high', 'cloud_area_fraction_low', 'cloud_area_fraction_medium', 'dew_point_temperature', 'fog_area_fraction', 'relative_humidity', 'ultraviolet_index_clear_sky', 'wind_from_direction', 'wind_speed'

    def get_precipitation(self):
        pass




    def export_forecasts(self, metrics):
        self.get_forecasts(metrics).to_csv('yr_forecasts.csv', index=False)

#Forecasts from The Weather Channel
class weather_channel_forecast:

    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon

    def get_data(self):
        import http.client
        conn = http.client.HTTPSConnection("api.weather.com")
        headers = {'accept': "application/json; charset=UTF-8"}
        days = 2
        latlon = str(self.lat)+','+str(self.lon)
        units = 'm'
        #units = ''
        language = 'en-US'
        format = 'json'
        api_key = '28d0d590186f456190d590186f0561ff'
        URL_base = f'https://api.weather.com/v3/wx/forecast/hourly/{days}day?geocode={latlon}&format={format}&units={units}&language={language}&apiKey={api_key}'
        conn.request("GET", URL_base, headers=headers)
        res = conn.getresponse()
        data = res.read()
        data_json = json.loads(data.decode("utf-8"))
        return data_json

    def get_forecasts(self,input_metrics):
        if input_metrics == 'all':
            metrics = list(self.get_data().keys())
        else:
            metrics = input_metrics
        metric_name = []
        metric_value = []
        fc_lat = []
        fc_lon = []
        fc_collection_datetime  = []
        fc_datetime = []
        collection_dt = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        for dt in self.get_data()['validTimeLocal']:
            fc_datetime.append(datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z").astimezone(pytz.utc))
            fc_lat.append(self.lat)
            fc_lon.append(self.lon)
            fc_collection_datetime.append(collection_dt)
        wcf_forecast = list(zip(fc_lat, fc_lon, fc_datetime,fc_collection_datetime))
        wcf_forecast_dataframe = pd.DataFrame(wcf_forecast,columns=['lat', 'lon', 'datetime','collection_datetime'])
        for metric in metrics:
            wcf_forecast_dataframe[metric] = np.array(self.get_data()[metric])
        wcf_forecast_dataframe["windSpeed"] = wcf_forecast_dataframe["windSpeed"]*1000/3600
        wcf_forecast_dataframe["windSpeed"] = wcf_forecast_dataframe["windSpeed"].round(1)
        wcf_forecast_dataframe['fc_leadtime'] = (wcf_forecast_dataframe['datetime'] - wcf_forecast_dataframe['collection_datetime']).dt.total_seconds() / 3600
        wcf_forecast_dataframe['fc_leadtime'] = wcf_forecast_dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        return wcf_forecast_dataframe

#Forecasts from Accuweather
class Accuweather_forecast:

    def __init__(self, lat, lon):
        self.lon = lon
        self.lat = lat
        #self.api_key = "1YjFUtWv4U4Ag9xxEAOquo539uwAE50D"
        self.api_key = "CkoeDLfPcgAN9zLv5xtjFUNHLN9TpR5n"


    def code_location(self):
        url_location_key = f'http://dataservice.accuweather.com/locations/v1/cities/geoposition/search?apikey={self.api_key}&q={self.lat},{self.lon}&language=en-us'
        response = req.get(url_location_key, headers={"APIKey": self.api_key})
        json_data = json.loads(response.text)
        code = json_data['Key']
        return code


    def get_data(self):
        url_location_key = f'http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/{str(self.code_location())}?apikey={self.api_key}&language=en-us&details=true&metric=false'
        response = req.get(url_location_key, headers={"APIKey": self.api_key})
        json_data = json.loads(response.text)
        return json_data

    def get_forecasts(self, input_metrics):
        fc_period = []
        fc_metric_name = []
        fc_metric_value = []
        if input_metrics == 'all':
            metrics = list(self.get_data()[0].keys())
        else:
            metrics = input_metrics
        for datapoint in self.get_data():
            fc_period_i = []
            fc_metric_name_i = []
            fc_metric_value_i = []
            for metric in metrics:
                if metric in ['Wind', 'WindGust']:
                    #'Wind', 'WindGust'
                    try:
                        fc_metric_value_i.append(round(datapoint[metric]['Speed']['Value']*1000/3600, 1))
                        fc_metric_name_i.append(metric+'_Speed')
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                        fc_metric_value_i.append(datapoint[metric]['Direction']['Degrees'])
                        fc_metric_name_i.append(metric+'_Direction')
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                    except:
                        fc_metric_value_i.append(round(datapoint[metric]['Speed']['Value']*1000/3600, 1))
                        fc_metric_name_i.append(metric+'_Speed')
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                elif  metric in ['RelativeHumidity','PrecipitationProbability']:
                    try:
                        fc_metric_value_i.append(datapoint[metric])
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                        fc_metric_name_i.append(metric)
                    except:
                        pass
                elif metric in ['Temperature']:
                    try:
                        fc_metric_value_i.append(round((float(datapoint[metric]['Value']) - 32)*5/9,1))
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                        fc_metric_name_i.append(metric)
                    except:
                        pass
                else:
                    try:
                        fc_metric_value_i.append(datapoint[metric]['Value'])
                        fc_period_i.append(datetime.datetime.fromisoformat(datapoint['DateTime']).astimezone(pytz.utc))
                        fc_metric_name_i.append(metric)
                    except:
                        pass

            fc_period = np.concatenate((fc_period,fc_period_i))
            fc_metric_name = np.concatenate((fc_metric_name, fc_metric_name_i))
            fc_metric_value = np.concatenate((fc_metric_value, fc_metric_value_i))
        accuweather_forecasts_loc = list(zip(fc_period, fc_metric_name, fc_metric_value))
        accuweather_forecasts_loc_dataframe = pd.DataFrame(accuweather_forecasts_loc,
                                                           columns=['datetime', 'metric_name','metric_value'])
        accuweather_forecasts_loc_dataframe['collection_datetime'] = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        accuweather_forecasts_loc_dataframe['fc_leadtime'] = (accuweather_forecasts_loc_dataframe['datetime'] - accuweather_forecasts_loc_dataframe['collection_datetime']).dt.total_seconds() / 3600
        accuweather_forecasts_loc_dataframe['fc_leadtime'] = accuweather_forecasts_loc_dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        return accuweather_forecasts_loc_dataframe

    def export_forecasts(self, input_metrics):
        self.get_forecasts(input_metrics).to_csv('accuweather_forecasts.csv', index=False)


#Forecasts from Meteomatics
class Meteomatics_forecast:
    def __init__(self, coordinates):
        self.coordinates = coordinates
        self.username  = 'valentinasmelova_smelova'
        self.password = '629RuCVs9q'
        self.model = 'mix'
        self.startdate = datetime.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        self.enddate = datetime.datetime.utcnow() + datetime.timedelta(days=2)
        self.interval = datetime.timedelta(hours=1)

    def get_data(self, parameters):
        self.parameters = parameters
        df = mmapi.query_time_series(self.coordinates, self.startdate, self.enddate, self.interval,
                                     self.parameters, self.username, self.password, model=self.model)
        df = df.reset_index()
        return df

    def get_forecasts(self, parameters):
        dataframe = self.get_data(parameters = parameters)
        dataframe['collection_datetime'] = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        dataframe['fc_leadtime'] = (dataframe['validdate'] -dataframe['collection_datetime']).dt.total_seconds() / 3600
        dataframe['fc_leadtime'] = dataframe['fc_leadtime'].apply(lambda x: int(round(x)))
        return dataframe



