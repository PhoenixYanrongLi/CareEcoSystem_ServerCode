__author__ = 'Brad'
from MetricsClass import MetricsClass
import math
import Queue

class DailyRoomTransitions(MetricsClass):

    def __init__(self, database, patient_id):
        MetricsClass.__init__(self,
                              database    = database,
                              patient_id  = patient_id,
                              table_name  = 'AnalysisRoomLocation',
                              metric_type = 'DailyRoomTransitions')

    def process_data(self, data, start_time):
        last_values_list = []
        prior_room = ''
        room_trans = 0
        data = list(zip(*data)[1])
        for i in data:
            if i != prior_room:
                if len(last_values_list) >= 5:
                    room_trans += 1
                    last_values_list = [i]
                    prior_room = i
                else:
                    last_values_list = [i]
                    prior_room = i
            if i == prior_room:
                last_values_list.append(i)
        return [start_time, room_trans], ['timestamp', 'NumberTransitions'], ['VARCHAR(100) NOT NULL PRIMARY KEY', 'FLOAT']
