__author__ = 'Brad'
from MetricsClass import MetricsClass


class DailyPercentRoom(MetricsClass):

    def __init__(self, database, patient_id):
        super(DailyPercentRoom, self).__init__(database    = database,
                                               patient_id  = patient_id,
                                               table_name  = 'AnalysisRoomLocation',
                                               metric_type = 'DailyPercentRoom')

    def fetch_rooms_list(self):
        if not self.fetch_from_database(database_name = self.database_name,
                                        table_name    = 'rooms',
                                        to_fetch      = 'ROOM_NAME'
                                        ):
            return False
        else:
            data = self.fetchall()
            data = zip(*list(zip(*data)))
            return list(zip(*data)[0])

    def process_data(self, data, start_time):

        # Fetch the data and the list of room associated with the patient
        data = list(zip(*data)[1])
        rooms_list = self.fetch_rooms_list()
        if not rooms_list:
            raise RuntimeError

        # Create a 2d list n rooms wide initialized to a value of 0
        total_per_room = [0] * len(rooms_list)

        # Get the total number of data points for normalization later
        total_number_points = len(data)

        """
            For each data point, fetch the room that it is associated with in the rooms_list table, and then add
            1 to the total for that room.
            ex:
            rooms_list = ['bathroom', 'living_room', 'bed_room']
            data = ['living_room', 'living_room', 'living_room', 'bed_room', 'bed_room','bathroom']

            Then the total_per_room list will look like this:
            total_per_room = [1, 3, 2]
        """
        for i in data:
            total_per_room[rooms_list.index(i)] =  total_per_room[rooms_list.index(i)] + 1

        """
            Normalize the total_per_room list
            For our example, it should look something like this:

            total_per_room = [.16, .50, .33]
        """
        normalized = [x / float(total_number_points) for x in total_per_room]

        """
            Return a list of items in format [metric], [metric_names], [metric type], example:

            [1436533203000, [.16, .50, .33]], ['timestamp', ['bathroom', 'living_room', 'bed_room']],
            [['VARCHAR(100) NOT NULL PRIMARY KEY FLOAT'], ['VARCHAR(100) NOT NULL PRIMARY KEY FLOAT'],
            ['VARCHAR(100) NOT NULL PRIMARY KEY FLOAT']]
        """
        return [start_time] + normalized,\
               ['timestamp'] + rooms_list,\
               ['VARCHAR(100) NOT NULL PRIMARY KEY'] + ['FLOAT'] * len(rooms_list)
