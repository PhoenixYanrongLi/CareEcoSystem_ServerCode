import unittest
from os import getcwd
from os.path import join
import csv
from DailyMetricCode import DailyPercentRoom

__author__ = 'Brad'


class MetricsTester(unittest.TestCase):

    def setUp(self):
        base_path = getcwd()
        test_data_path = join(base_path, 'testData')

        room_data = join(test_data_path, 'RoomData.csv')
        self.roomdata = []
        with open(room_data, 'rb') as rooms:
            rooms_reader = csv.reader(rooms, delimiter=',')

            for row in rooms_reader:
                self.roomdata.append(row)

        # Remove header, data is in ascending order
        self.roomdata = list(reversed(self.roomdata))
        self.roomdata.pop()

        host = "localhost"
        user = "root"
        password = "mysql"

        self.database = (host, user, password)

    def testPercentRoom(self):
        tester = DailyPercentRoom.DailyPercentRoom(database=self.database,
                                                   patient_id='george.netscher')

        start, end = tester.get_day_window()

        metric, metric_names, metric_types = tester.process_data(self.roomdata, int(start))
        self.assertEqual([int(start), .25, .25, .25, .25], metric, "Metric: Room Percentage Failed!")


if __name__ == '__main__':
    runner = unittest.main()
