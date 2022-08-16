import os
import pandas as pd


oldpwd = os.getcwd()
#we can also limit the timehirizon, how far fo we look backwards
class ground_truth_locations:
    def export_locations_from_obs(self):
        os.chdir(os.getcwd())
        os.chdir(os.getcwd() + '\Observations')
        #os.chdir(os.getcwd())
        locations = pd.DataFrame()
        #os.chdir(os.getcwd())
        path_of_the_directory = os.getcwd()
        for filename in os.listdir(path_of_the_directory):
            f = os.path.join(filename)
            if os.path.isfile(f):
                locations_i = pd.read_csv(filename)
                locations = pd.concat([locations,locations_i[['id','lat','lon','source']]])
            else:
                pass
        locations = locations.drop_duplicates().reset_index(drop=True)

        return locations

    def export_coordinates(self):
        os.chdir(oldpwd)
        user_coordinates = pd.read_excel('bbox_list.xlsx',  sheet_name='coordinate_list', index_col=None, header=0)
        user_coordinates['source'] = 'manual'
        return user_coordinates

    def export_locations(self):
        obs_locations = self.export_locations_from_obs()
        manual_locations = self.export_coordinates()
        all_locations = pd.concat([obs_locations, manual_locations]).reset_index(drop=True)
        return all_locations.iloc[:1]


    #.iloc[:1]



