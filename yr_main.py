from forecast_requests import *
from locations import *
import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import schedule
import time

oldpwd = os.getcwd()


#Check if a folder "Forecasts" exists in the root directory, if not - create
if not os.path.isdir('Forecasts'):
    os.mkdir('Forecasts')
else:
    pass



class fc_app():
#_______________________________GET FORECASTS_____________________________________________
#Method import_forecasts allows to export available future forecasts for all
#coordinates available in the past saved observations from all the sources
    def import_forecasts(self):
        #first we load the array of all ground truth coordinates
        oldpwd = os.getcwd()
        os.chdir(oldpwd)
        ground_truth_locations_list = ground_truth_locations()
        locations_list = ground_truth_locations_list.export_locations()

        # YR FORECAST
        try:
            yr_forecast = Forecasts_YR()
            yr_new_forecast = yr_forecast.export_forecasts(locations_list)
            os.chdir(oldpwd)
            os.chdir(os.getcwd() + '\Forecasts')
            now = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
            yr_new_forecast.to_csv(f'yr_new_forecasts_full{now}.csv', index=False)
        except:
            print("YR Forecast was not reloaded")


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
    log_file = "log_file_yr.log"
    create_timed_rotating_log(log_file)
    logging.basicConfig(filename=log_file)
    application = fc_app()
    application.import_forecasts()



schedule.every(3).hours.at(":01").do(main_body)
while True:
    schedule.run_pending()
    time.sleep(1)


#main_body()

