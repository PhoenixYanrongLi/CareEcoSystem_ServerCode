__author__ = 'Brad'
from MetricsClass import MetricsClass
import math


class DailyLifeSpace(MetricsClass):

    def __init__(self, database, patient_id):
        MetricsClass.__init__(self,
                              database    = database,
                              patient_id  = patient_id,
                              table_name  = 'dataMMGPS',
                              metric_type = 'life_space')

    def get_home_coors(self):
        self.fetch_from_database(database_name = self.database_name,
                                 table_name    = 'profile',
                                 to_fetch      = ['HOME_LATITUDE', 'HOME_LONGITUDE'])
        data = self.fetchall()
        latitude, longitude = data[0][0]
        return latitude, longitude

    def process_data(self, data, start_time):

        h_lat, h_long = self.get_home_coors()
        #Radius of Earth (km)
        R = 6371000
        longest_dist = 0
        for i in data:
            dlong = i[1] - h_lat
            dlat  = i[2] - h_long
            a     = math.sin(dlat/2)**2 + math.cos(h_long)*math.cos(i[2])*(math.sin(dlong/2))**2
            c     = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
            d     = R*c
            if d > longest_dist:
                longest_dist = d

        return [start_time, longest_dist], ['timestamp', 'DailyLifeSpace'] , ['VARCHAR(100) NOT NULL PRIMARY KEY', 'FLOAT']
