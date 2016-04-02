__author__ = 'george'

import numpy as np
from sklearn.datasets import make_blobs
from sklearn.preprocessing import normalize
from TrendDetection.DailyStepCountTrendDetection import DailyStepCountTrendDetection
from AnomalyDetection.HourlyRoomPercentageAnomalyDetection import HourlyRoomPercentageAnomalyDetection
import matplotlib.pyplot as plt

"""
This file contains simulations of the methods used. Plots are created using fake data to demonstrate how they work.
"""


class Simulate:
    """
    This class groups together related methods for performing the required simulations
    """

    @staticmethod
    def show_ransac():
        """
        This method demonstrates the RANSAC (RANdom SAmple Consensus) by generating a plot with simulation data
        """

        # simulate normal data
        activeday = 10000
        lazyday = 3000
        stepcounts, truelabels = make_blobs(n_samples=31, centers=[[activeday]], cluster_std=750, random_state=8)
        for i in range(len((stepcounts))):
            if (i % 6 == 0) and (i != 0):
                stepcounts[i] = np.random.normal(loc=lazyday, scale=750)
                stepcounts[i-1] = np.random.normal(loc=lazyday, scale=750)

        # apply trend detection
        steptrender = DailyStepCountTrendDetection(stepcounts)
        slope = steptrender.normalize_and_fit()

        # output regression slope and whether or not alarming
        alarmflag = steptrender.is_most_recent_trend_alarming()
        if alarmflag:
            print 'The slope is %f which is alarming' % slope
        else:
            print 'The slope is %f which is not alarming' % slope

        # plot results
        linex = np.arange(0, 31)
        pointsx = range(0, 31)
        liney = steptrender.get_model().predict(linex[:, np.newaxis])
        plt.plot(linex, liney, 'r-', label='RANSAC regressor')
        pointsy = normalize(stepcounts, norm='l1', axis=0)
        plt.plot(pointsx, pointsy, 'ko', label='Step Counts')
        plt.legend(loc='lower left')

        plt.show()

    @staticmethod
    def show_dbscan():
        """
        simulate 1 month of normal hourly room percentage data followed by an anomalous percentage
        the normal data is bimodal following most peoples activity patterns in which there is routinely a weekday
        percentage and a weekend percentage. 1 day in which the person spends a large amount of time in the bathroom
        is simulated
        """

        # simulate normal hourly data
        weekday = ([0.05, 0.95], 0.05) #bath, bed
        weekend = ([0.3, 0.7], 0.1)
        roomperwd, truelabelswd = make_blobs(n_samples=23, centers=weekday[0],
                                             cluster_std=weekday[1], random_state=0)
        roomperwe, truelabelswe = make_blobs(n_samples=8, centers=weekend[0],
                                             cluster_std=weekend[1], random_state=0)

        # combine modes
        roompers = np.vstack((roomperwd, roomperwe))

        # make positive and sum to one to simulate valid distribution
        for i in range(roompers.shape[0]):
            for j in range(roompers.shape[1]):
                if roompers[i, j] < 0:
                    roompers[i, j] = 0
        roompersnorm = normalize(roompers, norm='l1')

        # simulate anomaly on most recent day where don't leave bedroom
        roompersnorm[-1, :] = np.array([0.8, 0.2])

        # detect outliers
        roompersdetector = HourlyRoomPercentageAnomalyDetection(roompersnorm, eps=0.3, min_samples=3)
        labels = roompersdetector.scale_and_proximity_cluster(eps=0.3, min_samples=3)

        # plot results
        plt.figure()
        seenflag1 = False; seenflag2 = False; seenflag3 = False;
        for i, label in enumerate(labels):
            if label == 0:
                if seenflag1:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'ro')
                else:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'ro', label='Cluster 1')
                    seenflag1 = True
            elif label == 1:
                if seenflag2:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'kx')
                else:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'kx', label='Cluster 2')
                    seenflag2 = True
            elif label == -1:
                if seenflag3:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'b^')
                else:
                    plt.plot(roompersnorm[i][0], roompersnorm[i][1], 'b^', label='Outlier')
                    seenflag3 = True
        plt.legend(loc='lower left')
        plt.axis([0, 1, 0, 1])
        plt.show()

    def show_klms(self):
        """
        Demonstrate the klms (kernal least mean squares) algorithm on a sample set of 50 RSSI points
        Maximum correntropy criterion (MCC) is used instead of mean square error (MSE)
        """
        inputRSSI = np.array([-999, -999, -999, -999, -90,  -90, -999, -999, -999, -999,
                              -999, -999, -999, -999, -92,  -92, -999, -999, -999, -999,
                              -999, -999, -91,  -91,  -79,  -77, -82,  -80,  -90,  -94,
                              -92,  -90,  -999, -999, -999, -93, -93,  -93,  -83,  -85,
                              -88,  -89,  -89,  -91,  -73,  -73, -999, -999, -999, -999])

        trainRSSI = np.array([-999, -999, -999, -999, -999, -999, -999, -999, -999, -999,
                              -999, -999, -999, -999, -999, -999, -999, -999, -999, -999,
                              -999, -999, -75,  -75,  -75,  -75,  -75,  -75,  -75,  -75,
                              -75,  -75,  -75,  -75,  -75,  -75,  -75,  -75,  -75,  -75,
                              -75,  -75,  -75,  -75,  -75,  -75,  -999, -999, -999, -999])



if __name__ == '__main__':
    simulator = Simulate()
    simulator.show_ransac()
    simulator.show_dbscan()
