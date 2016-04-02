__author__ = 'Brad'
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from pytz import utc
import multiprocessing
from DatabaseManagementCode.databaseWrapper import DatabaseWrapper
from FileManagementCode.signalHandler import SignalHandler
import signal
from DailyStepCount import DailyStepCount
import logging
from AnalysisCode import AnalysisProcessingClass


class RunDataProcessing(multiprocessing.Process, DatabaseWrapper):

    def __init__(self, database):
        multiprocessing.Process.__init__(self)
        DatabaseWrapper.__init__(self, database)
        self.database = database

        #jobstores = {
        #    'default': SQLAlchemyJobStore(url='mysql://brad:moxie100@localhost/jobstore')
        #}

        executors = {
            'default': ThreadPoolExecutor(20),
        }

        self.scheduler = BackgroundScheduler(executors    = executors,
                                             timezone     = utc)
        self.jobs = []
        self.table_type_dict = {'step_count': 'dataHMACC'}

        self.metric_jobs = [
            DailyStepCount,

        ]

    def add_job(self, job_function, args):
        return self.scheduler.add_job(job_function,
                                      args=args,
                                      trigger='cron',
                                      hour = '0,12')

    def get_patients_list(self):
        if not self.fetch_from_database(database_name      = 'config',
                                        table_name         = 'caregiverPatientPairs',
                                        to_fetch           = 'patient',
                                        to_fetch_modifiers = 'DISTINCT'):
            return []
        return [row[0] for row in self]

    def check_table_completion(self, patient_id, table_name):
        try:
            return self.fetch_from_database(database_name = '_' + patient_id,
                                            table_name    = table_name,
                                            to_fetch      = 'timestamp')
        except:
            return False

    def launch_metric(self, patient_id, metric_type):
        """
        Launches new metrics
        :param patient_id:
        :return:
        """

        if self.check_table_completion(patient_id, self.table_type_dict[metric_type]):
            self.jobs.append(self.add_job(self.starter_job, [DailyStepCount, self.database, patient_id, self.table_type_dict[metric_type], metric_type]))
        return patient_id + metric_type

    def starter_job(self, funcName, database, patient_id, table, metric_type):
        logging.warning('Sched Ran!')
        p = funcName(database, patient_id)
        p.start()
        return True

    def run(self):

        """
            Every 12 Hours, run Analysis Code, then run Metrics Code, finally run Anomaly Detection Code
        :return:
        """

        self.scheduler.start()
        launched_jobs = []

        s = SignalHandler()
        signal.signal(signal.SIGINT, s.handle)
        while s.stop_value:
            patient_ids = self.get_patients_list()
            for patient_id in patient_ids:
                stepcount_job  = patient_id + 'step_count'
                gait_job       = patient_id + 'gait-speed'
                room_trans_job = patient_id + 'room-transitions'
                room_perc_job  = patient_id + 'room-percentage'
                life_space_job = patient_id + 'life-space'

                if stepcount_job not in launched_jobs:
                    if self.launch_metric(patient_id, 'step_count'):
                        launched_jobs.append(stepcount_job)