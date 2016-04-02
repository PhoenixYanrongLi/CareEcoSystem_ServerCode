__author__ = 'Brad'

import datetime
import multiprocessing
from DatabaseManagementCode.databaseWrapper import DatabaseWrapper
import abc
from simple_salesforce import Salesforce
from DatabaseManagementCode.dataUploadClass import GenericFileUploader
import logging

class MetricsClass(multiprocessing.Process, DatabaseWrapper):

    def __init__(self, database, patient_id, table_name, metric_type):
        logging.warning('Watch out!')
        multiprocessing.Process.__init__(self)
        DatabaseWrapper.__init__(self, database)
        self.database_name = '_' + patient_id
        self.table_name    = table_name
        self.metric_type   = metric_type
        self.sf = Salesforce(username='bbzylstra@randolphcollege.edu.careeco.dev',
                             password='_moxie100',
                             security_token='V6oPmGSaXW6Q5cv8MAEhLqCK',
                             sandbox=True)

    @staticmethod
    def get_day_window():
        """
        Returns the window between the current day and the last day
        :return:
        """
        current_date = datetime.datetime.now()
        start_window = datetime.datetime(current_date.year,
                                         current_date.month,
                                         current_date.day,
                                         current_date.hour) - datetime.timedelta(hours=12)
        start_window = start_window.strftime('%y-%m-%dT%H:%M:%S.%f')
        start_window = GenericFileUploader.timestamp_to_UTC(start_window)

        end_window   = datetime.datetime(current_date.year, current_date.month, current_date.day)
        end_window   = end_window.strftime('%y-%m-%dT%H:%M:%S.%f')
        end_window   = GenericFileUploader.timestamp_to_UTC(end_window)

        return start_window, end_window

    def poll_data(self, start_window, end_window):
        """
        Returns the data between the time frame specified
        :return:
        """

        if not self.fetch_from_database(database_name = self.database_name,
                                        table_name    = self.table_name,
                                        where         = [['timestamp', '<', start_window],
                                                         ['timestamp', '>=', end_window]],
                                        order_by      = ['timestamp', 'ASC']):
            return []
        else:
            metric_data = self.fetchall()

        if len(metric_data) == 0:
            return []
        else:
            return zip(*list(zip(*metric_data)))

    @abc.abstractmethod
    def process_data(self, data, start_window):
        """
        Returns the processed metric
        :param data:
        :return:
        """
        return

    def upload_to_database(self, metric, metric_names, metric_types):
        """
        Uploads the metric to the database ['VARCHAR(100) NOT NULL PRIMARY KEY', 'FLOAT']
        :return:
        """
        self.create_table(database_name= self.database_name,
                          table_name   = self.metric_type,
                          column_names = metric_names,
                          column_types = metric_types)
        self.insert_into_database(database_name = self.database_name,
                                  table_name    = self.metric_type,
                                  column_names  = ['timestamp', 'metric'],
                                  values        = metric)
        return True

    def upload_to_salesforce(self, metric):
        """
        Uploads the metric to salesforce
        :return:
        """
        metric_data = {
            "patientId": self.database_name[1:],
            "metricList": [{
                "metric": self.metric_type,
                "metricValue": metric[1]}
            ]
        }
        self.sf.apexecute('FMMetrics/insertMetrics', method='POST', data=metric_data)
        return True

    def run(self):
        """

        :return:
        """
        start_window, end_window = MetricsClass.get_day_window()
        data = self.poll_data(start_window, end_window)
        metric, metric_names, metric_types = self.process_data(data, start_window)
        self.upload_to_database(metric, metric_names, metric_types)
        self.upload_to_salesforce(metric)
        return True
