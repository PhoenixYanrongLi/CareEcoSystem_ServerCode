from MetricsClass import MetricsClass
__author__ = 'Brad'


class DailyGaitSpeed(MetricsClass):

    def __init__(self, database, patient_id):
        super(DailyGaitSpeed, self).__init__(database    = database,
                                             patient_id  = patient_id,
                                             table_name  = 'AnalysisGaitSpeedHM',
                                             metric_type = 'DailyGaitSpeed')