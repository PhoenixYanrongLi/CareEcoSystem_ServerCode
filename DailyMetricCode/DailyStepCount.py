__author__ = 'Brad'
from MetricsClass import MetricsClass


class DailyStepCount(MetricsClass):

    def __init__(self, database, patient_id):
        MetricsClass.__init__(self,
                              database    = database,
                              patient_id  = patient_id,
                              table_name  = 'dataHMACC',
                              metric_type = 'step_count')

    def process_data(self, data, start_time):
        total_stepcount = 0

        for i in data:
            total_stepcount += i[9]

        return [start_time, total_stepcount]

