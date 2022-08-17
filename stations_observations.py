import requests as req
import xml.etree.ElementTree as ET
import pandas as pd
import json
from fmiopendata.wfs import download_stored_query
import numpy as np
import datetime as dt
import pytz


#Class Stations_fmi allows to get a list of ALL FMI available weather stations and their properties
# Weather-related data from FMI can be gathered from XML files by parsing
#query_id=fmi::ef::stations

class Stations_fmi:
    def get_stations(self):
        url_fmi_stations = 'https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::ef::stations&'
        request = req.get(url=url_fmi_stations)
        output = request.text
        root = ET.fromstring(output)
        st_id = []
        st_name = []
        st_properties = []
        st_location = []
        members = root.findall(".//{http://www.opengis.net/wfs/2.0}member")
        properties_dict = []
        st_source = []
        for member in members:
            name = member.findtext(".//{http://www.opengis.net/gml/3.2}name")
            id = member.findtext(".//{http://www.opengis.net/gml/3.2}identifier")
            location = tuple(float(p) for p in member.findtext(".//{http://www.opengis.net/gml/3.2}pos").split())
            source = 'FMI'
            st_id.append(id)
            st_name.append(name)
            st_location.append(location)
            st_source.append(source)
            names = member.findall(".//{http://www.opengis.net/gml/3.2}name")
            st_location_insert = []
            properties_dict_insert = {}
            for name_counter in names:
                attribute_name = name_counter.attrib['codeSpace'].replace('http://xml.fmi.fi/namespace/', '')
                attribute_value = name_counter.text
                properties_dict_insert.update({attribute_name: attribute_value})
                attributes = (attribute_name, attribute_value)
                st_location_insert.append(attributes)
            st_properties.append(st_location_insert)
            properties_dict.append(properties_dict_insert)
        fmi_stations = list(zip(st_id, st_name,st_location,properties_dict,st_source))
        fmi_stations_dataframe = pd.DataFrame(fmi_stations, columns=['id','name','location','properties','source'])
        return fmi_stations_dataframe

    def export_stations(self):
        self.get_stations().to_csv('fmi_stations.csv', index=False)

#Aeris_observations Class allows to get a list Aeris stations and observations within the given Polygon

class Aeris_observations:
    def __init__(self, bbox, from_date, limit):
        self.bbox = bbox
        self.from_date = from_date
        self.limit = limit

    def get_data(self):
        url_base = 'https://api.aerisapi.com/observations/archive/within?p=' + self.bbox + '&from=' + self.from_date + '&format=json&filter=allstations&limit=' + str(self.limit) + '&'
        secret_key = 'O2U57aPy30qn6TRhMogxUNPgYs6zDX23rXTvOKuy'
        client_id = '0aeGF7FKmqjeuiTlyxuvN'
        url = url_base + 'client_id=' + client_id + '&client_secret=' + secret_key
        response = req.get(url=url)
        json_file = json.loads(response.text)
        return json_file

    def get_stations(self):
        st_id = []
        st_source = []
        st_location = []
        st_properties = []
        st_tz = []
        if self.get_data()['success'] == True:
            for i in range(len(self.get_data()['response']) - 1):
                st_tz.append(self.get_data()['response'][i]['profile']['tz'])
                st_id.append(self.get_data()['response'][i]['id'])
                st_source.append(self.get_data()['response'][i]['dataSource'])
                st_location.append((self.get_data()['response'][i]['loc']['lat'],
                                    self.get_data()['response'][i]['loc']['long']))
                st_properties.append(self.get_data()['response'][i]['place'])
            aeris_stations = list(zip(st_id, st_tz, st_location, st_properties, st_source))
            aeris_stations_dataframe = pd.DataFrame(aeris_stations, columns=['id', 'timezone', 'location', 'properties', 'source'])

            return aeris_stations_dataframe
        else:
            return f"{self.bbox} location not found in Aeris Observations API"

    def export_stations(self):
        self.get_stations().to_csv('aeris_stations.csv', index=False)


    def get_observations(self, input_metrics):
        st_lat_full = []
        st_lon_full = []
        st_period_full = []
        st_metric_name_full = []
        st_metric_value_full = []
        st_id_full = []
        st_tz_full = []
        if self.get_data()['success'] == True:
            if input_metrics == 'all':
                metrics = list(self.get_data()['response'][0]['periods'][0]['ob'].keys())
            else:
                metrics = input_metrics

            for i in range(len(self.get_data()['response']) - 1):
                st_lat = []
                st_lon = []
                st_period = []
                st_metric_name = []
                st_metric_value = []
                st_id = []
                st_tz = []
                station = self.get_data()['response'][i]
                for j in range(len(station['periods'])):
                    st_lat_i = []
                    st_lon_i = []
                    st_id_i = []
                    st_tz_i = []
                    st_period_i = []
                    st_metric_name_i = []
                    st_metric_value_i = []
                    station_period = station['periods'][j]
                    for metric in metrics:
                        st_lat_i.append(station['loc']['lat'])
                        st_lon_i.append(station['loc']['long'])
                        st_id_i.append(station['id'])
                        st_tz_i.append(station['profile']['tz'])
                        st_period_i.append(dt.datetime.fromisoformat(station_period['ob']['recDateTimeISO']).astimezone(pytz.utc))
                        st_metric_name_i.append(metric)
                        st_metric_value_i.append(station_period['ob'][metric])
                    st_id = np.concatenate((st_id, st_id_i))
                    st_tz = np.concatenate((st_tz, st_tz_i))
                    st_lat = np.concatenate((st_lat,st_lat_i))
                    st_lon = np.concatenate((st_lon, st_lon_i))
                    st_period = np.concatenate((st_period,st_period_i))
                    st_metric_name = np.concatenate((st_metric_name, st_metric_name_i))
                    st_metric_value = np.concatenate((st_metric_value, st_metric_value_i))
                st_id_full = np.concatenate((st_id_full , st_id))
                st_tz_full = np.concatenate((st_tz_full, st_tz))
                st_lat_full = np.concatenate((st_lat_full ,st_lat))
                st_lon_full = np.concatenate((st_lon_full, st_lon))
                st_period_full = np.concatenate((st_period_full ,st_period))
                st_metric_name_full = np.concatenate((st_metric_name_full , st_metric_name))
                st_metric_value_full = np.concatenate((st_metric_value_full , st_metric_value))

            aeris_observations = list(zip(st_id_full, st_lat_full, st_lon_full, st_period_full , st_metric_name_full ,st_metric_value_full, st_tz_full ))
            aeris_observations_dataframe = pd.DataFrame(aeris_observations, columns=['id','lat', 'lon', 'datetime', 'metric_name', 'metric_value', 'timezone']).reset_index(drop=True)
            aeris_observations_dataframe['source'] = 'Aeris'
            aeris_observations_dataframe = aeris_observations_dataframe.pivot_table(index=['id', 'lat', 'lon', 'source', 'timezone', 'datetime'], columns='metric_name', values='metric_value', aggfunc='mean').reset_index()
            aeris_observations_dataframe = aeris_observations_dataframe.rename(columns={"tempC": "temp_2", "humidity": "rhum_2", "windSpeedKPH": "winds_10", "windDirDEG": "windd_10"})
            aeris_observations_dataframe['winds_10'] = aeris_observations_dataframe['winds_10']*1000/3600
            aeris_observations_dataframe['winds_10'] = aeris_observations_dataframe['winds_10'].round(1)
            return aeris_observations_dataframe
        else:
            return f"{self.bbox} location not found in Aeris Observations API. Error message"  \
                   f"{self.get_data()}"


    def export_observations(self, metrics):
        self.get_observations(metrics).to_csv('aeris_observations.csv', index=False)


#Class FMI observations allows to get the weather observations for all stations inside a given Polygon
# Query_id : fmi::observations::weather::multipointcoverage

class fmi_observations:
    def __init__(self, starttime, endtime, bbox):
        self.starttime = starttime
        self.endtime = endtime
        self.bbox = bbox

    def get_data(self):
        model_data = download_stored_query("fmi::observations::weather::multipointcoverage",
                                       args=["starttime="+self.starttime,
                                             "endtime="+self.endtime,
                                             "bbox="+self.bbox])
        return model_data

#    def get_stations(self):
#        location_data = self.get_data().location_metadata
#        st_id = []
#        st_name = []
#        st_location = []
#        #st_properties = []
#        st_source = []
#        for location in location_data.keys():
#            st_name.append(location)
#            st_id.append(location_data[location]['fmisid'])
#            st_location.append((location_data[location]['latitude'],location_data[location]['longitude']))
#            st_source.append('fmi_observations')
#        fmi_stations_o = list(zip(st_id, st_name, st_location, st_source))
#        fmi_stations_o_dataframe = pd.DataFrame(fmi_stations_o, columns=['id', 'name', 'location', 'source'])
#
#        return fmi_stations_o_dataframe
#
#    def export_stations(self):
#        self.get_stations().to_csv('fmi_observations_stations.csv', index=False)

    def get_observations(self):
        metric_list = ['Air temperature', 'Wind speed', 'Wind direction', 'Relative humidity']
        observation_data = self.get_data().data
        station_data = self.get_data().location_metadata
        if len(observation_data.keys()) == 0:
            return f"{self.bbox} location not found in FMI Observations XML"
        else:
            st_metric_value = []
            st_metric_name = []
            st_name = []
            st_period = []
            st_id = []
            st_lat = []
            st_lon = []
            for time in observation_data.keys():
                st_metric_value_i = []
                st_metric_name_i = []
                st_name_i = []
                st_period_i = []
                st_id_i = []
                st_lat_i = []
                st_lon_i = []

                for station in observation_data[time].keys():
                    station_id = station_data[station]['fmisid']
                    station_lat = station_data[station]['latitude']
                    station_lon = station_data[station]['longitude']
                    for metric in metric_list:
                        st_metric_value_i.append(observation_data[time][station][metric]['value'])
                        st_name_i.append(station)
                        st_id_i.append(station_id)
                        st_lat_i.append(station_lat)
                        st_lon_i.append(station_lon)
                        st_period_i.append(time.replace(tzinfo=pytz.UTC))
                        st_metric_name_i.append(metric)
                st_name = np.concatenate((st_name, st_name_i))
                st_id = np.concatenate((st_id, st_id_i))
                st_lat = np.concatenate((st_lat, st_lat_i))
                st_lon = np.concatenate((st_lon, st_lon_i))
                st_period = np.concatenate((st_period, st_period_i))
                st_metric_name = np.concatenate((st_metric_name, st_metric_name_i))
                st_metric_value = np.concatenate((st_metric_value, st_metric_value_i))
            fmi_observations = list(zip(st_id, st_lat, st_lon, st_period, st_metric_name, st_metric_value))
            fmi_observations_dataframe = pd.DataFrame(fmi_observations, columns=['id','lat','lon','datetime','metric_name','metric_value'])
            fmi_observations_dataframe['timezone'] = 'Europe/Helsinki'
            fmi_observations_dataframe['source'] = 'FMI'
            fmi_observations_dataframe=fmi_observations_dataframe.pivot(index=['id', 'lat', 'lon', 'source', 'timezone', 'datetime'],
                                                                        columns='metric_name', values='metric_value').reset_index()
            fmi_observations_dataframe = fmi_observations_dataframe.rename(columns={'Air temperature': "temp_2", 'Relative humidity': "rhum_2", 'Wind speed': "winds_10", 'Wind direction': "windd_10"})
            return fmi_observations_dataframe

    def export_observations(self):
        self.get_observations().to_csv('fmi_observations.csv', index=False)








