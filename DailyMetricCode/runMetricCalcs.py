__author__ = 'Brad'
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import datetime
from pytz import utc
import multiprocessing
from DatabaseManagementCode.databaseWrapper import DatabaseWrapper
from FileManagementCode.signalHandler import SignalHandler
import signal
from DailyStepCount import DailyStepCount
from DailyPercentRoom import DailyPercentRoom
from DailyGaitSpeed import DailyGaitSpeed
from DailyLifeSpace import DailyLifeSpace
from DailyRoomTransitions import DailyRoomTransitions
import logging

class RunMetricCalcs(multiprocessing.Process, DatabaseWrapper):

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

        self.patients = []


        self.metric_launcher_dict = {'step-count'      : DailyStepCount,
                                     'percent-room'    : DailyPercentRoom,
                                     'gait-speed'      : DailyGaitSpeed,
                                     'room-transitions': DailyRoomTransitions,
                                     'life-space'      : DailyLifeSpace}


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
        :param metric_type:
            Name of the metric to launch. Options are:
            - step-count
            - percent-room

        :return:
        """

        if self.check_table_completion(patient_id, self.table_type_dict[metric_type]):
            self.jobs.append(self.add_job(self.starter_job,
                                          [self.metric_launcher_dict[metric_type],
                                           self.database,
                                           patient_id]))
        return patient_id + metric_type

    def starter_job(self, funcName, database, patient_id):
        logging.warning('Sched Ran!')
        p = funcName(database, patient_id)
        p.start()
        return True

    def run(self):
        self.scheduler.start()
        launched_jobs = []

        s = SignalHandler()
        signal.signal(signal.SIGINT, s.handle)
        while s.stop_value:
            patient_ids = self.get_patients_list()
            for patient_id in patient_ids:
                self.patients.append(PatientClass(self.metric_launcher_dict, patient_id))

            for patient in self.patients:
                unlaunched = patient.get_unlaunched_jobs()

                for job in unlaunched:
                    self.launch_metric()



class PatientClass:
    """
        Patient Class is used to check if a patient has all jobs launched
    """
    def __init__(self, metric_launcher_dict, patient_id):
        self.metrics_list  = metric_launcher_dict.keys()
        self.patient_id    = patient_id
        self.launched_jobs = []

    def add_launched_job(self, job):
        self.launched_jobs.append(job)

    def get_unlaunched_jobs(self):
        return list(set(self.metrics_list) - set(self.launched_jobs))






