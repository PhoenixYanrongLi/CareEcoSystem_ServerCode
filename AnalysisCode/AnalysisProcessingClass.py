__author__ = 'Brad'
import MySQLdb
import pickle
import datetime
import numpy
import multiprocessing
import abc
import apscheduler
from FileManagementCode.signalHandler import SignalHandler
from DatabaseManagementCode.databaseWrapper import DatabaseWrapper
from DatabaseManagementCode.dataUploadClass import GenericFileUploader
import signal
import time
import sys


class AnalysisProcessingClass(multiprocessing.Process, DatabaseWrapper):

    def __init__(self, database, patient_id, type_name, table_name):
        """

        :param database: List of the database login info
        :param patient_id: The patientID
        :param type_name: name of the type of analysis (ex. room_locationing)
        :param table_name: The name of the datatype being analyzed
        :return:
        """
        multiprocessing.Process.__init__(self)
        DatabaseWrapper.__init__(self, database)
        #self.dataB = MySQLdb.connect(host=database[0], user=database[1], passwd=database[2])
        #self.cur = self.dataB.cursor()
        self.database_name = '_' + patient_id
        self.typename = type_name
        self.obj_table = self.typename + "_obj"
        self.table_name = table_name

        # Make sure the analysisprofile table exists
        if not self.table_exists(self.database_name, 'analysisprofile'):
            self.create_table(database_name = self.database_name,
                              table_name    = 'analysisprofile',
                              column_names  = ['table_name', 'type_name', 'timestamp'],
                              column_types  = ['VARCHAR(100)', 'VARCHAR(100) NOT NULL PRIMARY KEY', 'BIGINT(20)'])

    def get_analysis_data(self, latest_timestamp):
        """
        @param data_type: str of the column selected, pass * for all data in table
        @param table_name: str of the name of the table
        @return: data
        """

        if not self.table_exists(self.database_name, self.table_name):
            return []

        if not self.fetch_from_database(database_name = self.database_name,
                                        table_name    = self.table_name,
                                        where         = ['timestamp', '>', latest_timestamp],
                                        order_by      = ['timestamp', 'ASC']):
            return []
        else:
            analysis_data = self.fetchall()

        if len(analysis_data) == 0:
            return []
        else:
            return zip(*list(zip(*analysis_data)))

    @abc.abstractmethod
    def upload_analysis(self, data):
        """
        Upload the analysis to the database
        :param data:
        :return:
        """
        return

    @abc.abstractmethod
    def split_to_windows(self, data):
        """
        Split the data into n number of windows
        :param data:
        :return:
        """
        return

    @abc.abstractmethod
    def process_data(self, windowed_data):
        """
        Do the analysis on the data
        :param windowed_data:
        :return:
        """
        return

    def read_start_stamp(self):
        if not self.fetch_from_database(database_name     = self.database_name,
                                            table_name    = 'profile',
                                            to_fetch      = 'START',):
                # Case for if no start time recorded
                return None
        else:
                # Return the start time
                start_stamp = self.fetchall()
                start_stamp = list(zip(*start_stamp))[0][0]
                return start_stamp


    def read_timestamp(self, ):
        """
        Reads in the timestamp of the last processed data
        :return: Timestamp if exists, otherwise NULL
        """
        if not self.fetch_from_database(database_name = self.database_name,
                                        table_name    = 'analysisprofile',
                                        to_fetch      = 'timestamp',
                                        where         = [['table_name', self.table_name], ['type_name', self.typename]]):
            # If no analysisprofile database exists then return the start timestamp
            return self.read_start_stamp()

        else:
            # If timestamp column does exist in analysis profile, return it if it has length,
            # otherwise return the start_stamp
            timestamp = self.fetchall()

            if len(timestamp) == 0:
                # If no timestamp in timestamp column, return start_stamp
                return self.read_start_stamp()

            else:
                # If timestamp exists in timestamp column, return that
                timestamp = list(zip(*timestamp))[0][0]
                return timestamp

    def write_timestamp(self, timestamp):
        return self.insert_into_database(database_name       = self.database_name,
                                         table_name          = 'analysisprofile',
                                         column_names        = ['table_name', 'type_name', 'timestamp'],
                                         values              = [self.table_name, self.typename, timestamp],
                                         on_duplicate_update = [ 2 ])

    def run(self):
        s = SignalHandler()
        signal.signal(signal.SIGINT, s.handle)
        while s.stop_value:
            timestamp = self.read_timestamp()
            # If no timestamp found, break the analysis code. Bad Profile!
            if timestamp is None: break

            data = self.get_analysis_data(timestamp)
            if data:
                windowed_data = self.split_to_windows(data)
                processed_data, new_timestamp = self.process_data(windowed_data)
                self.upload_analysis(processed_data)
                self.write_timestamp(new_timestamp)
            time.sleep(10)
