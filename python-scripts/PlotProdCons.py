#!/usr/bin/python
'''
Reads a set of data files representing hourly demand over a set of games.
Lines in each file are formatted as
  timeslot, day-of-week, hour-of-day, production, consumption
Net demand is the negative difference between consumption and production.
Data is gathered and plotted by hour-of-week or hour-of-day, aggregated
across games in a tournament.

Depends on a Python 3 installation with the SciPy/NumPy libraries
Tournament logs (in the form 'game-n-sim-logs.tar.gz') must be in a directory
named by the tournament var inside a dir named by the gameDir var. The Java
modules in dir ../logtool-examples must have been built (using mvn clean test).

Usage: Evaluate or import this module to suck in all the data. If necessary,
this will unpack the game logs and run the logtoolClass processor over each
state log to generate the raw data. Once this completes (can take several hours
the first time, a second or two on subsequent runs), use your Python IDE or
command-line env to do the plotting. For example, once this module is
imported (import PlotProdCons as pc), the weekday contour plot is
generated as pc.plotContours(pc.weekdayData, [.02,.25,.5,.75,.98])
'''

import numpy as np
import scipy as sp
import matplotlib.pyplot as plt
import matplotlib.lines as mline
import pylab as pl
from scipy import stats
from pathlib import Path
import DatafileIterator as di

gameDir = '../../games'
tournament = 'finals-201504'
#tournament = 'finals-2014'

logtoolClass = 'org.powertac.logtool.example.ProductionConsumption'
dataPrefix = 'data/prod-cons-'

#logtoolClass = 'org.powertac.logtool.example.SolarProduction'
#dataPrefix = 'data/solar-prod-'

def collectData (tournament):
    '''
    Processes data from data files in the specified directory.
    '''
    for dataFile in di.datafileIter(gameDir + "/" +  tournament,
                                    logtoolClass,
                                    dataPrefix):
        # note that dataFile is a Path, not a string
        processFile(str(dataFile))

gameData = []
weekData = [[] for x in range(168)]
weekdayData = [[] for x in range(24)]
weekendData = [[] for x in range(24)]
dayData = [[] for x in range(24)]
dataMap = {'Weekly':weekData, 'Weekday':weekdayData, 'Weekend':weekendData,
           'Daily':dayData}

def processFile (dataFile):
    '''
    Collects data from a data file into a global 168-column array,
    one column per hour-of-week.
    Note that the day-of-week numbers are in the range [1-7].
    '''
    data = open(dataFile, 'r')
    gameSeries = []
    gameData.append(gameSeries)
    junk = data.readline() # skip first line
    for line in data.readlines():
        row = line.split(', ')
        dow = int(row[1])
        hod = int(row[2])
        how = (dow - 1) * 24 + hod # hour-of-week
        prod = floatMaybe(row[3])
        cons = floatMaybe(row[4])
        #if cons < 0.0: # omit rows where cons == 0
        net = -prod - cons
        gameSeries.append([how, net])
        weekData[how].append(net)
        dayData[hod].append(net)
        if dow <= 5:
            #weekday
            weekdayData[hod].append(net)
        else:
            #weekend
            weekendData[hod].append(net)

def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str != '' :
        result = float(str)
    else:
        print('failed to float', str)
    return result

def plotMeans (data):
    '''
    Reduces the raw data into means and 1-sigma error bars, plots result
    '''
    d = [np.array(c) for c in data]
    print('Shape:', np.shape(d))
    means = [c.mean() for c in d]
    stds = [c.std() for c in d]
    x = range(np.shape(d)[0])
    plt.title('Mean consumption, 1-sigma error bars')
    plt.errorbar(x, means, yerr=stds)
    plt.xlabel('hour')
    plt.ylabel('Net demand (MW)')
    plt.show()

def plotContours (dataName, contours):
    '''
    Extracts data points from the raw data at the given contour intervals.
    The contours arg is a list of probabilities 0.0 < contour <= 1.0.
    For example, contours=[0.05, 0.5, 0.95] plots the 5%, 50%, and 95% contours.
    '''
    data = dataMap[dataName]
    rows = []
    for c in data:
        c.sort()
    for prob in contours:
        row = []
        rows.append(row)
        for c in data:
            n = len(c)
            if n > 0:
                index = round((n - 0.5) * prob)
                if index < 0:
                    index = 0
                if index >= n:
                    index = n - 1
                row.append(c[index])
            else:
                row.append(0.0)
    x = range(len(data))
    plt.grid(True)
    plt.title('{} net demand contours'.format(dataName))
    plt.ylim((0, 110))
    for y,lbl in zip(rows, contours):
        plt.plot(x, y, label = '{0}%'.format(lbl * 100))
    plt.xlabel('Hour')
    plt.ylabel('Net demand (MW)')
    plt.legend()
    tickFreq = 12
    if len(data) < 49:
        tickFreq = 2
    plt.xticks(np.arange(0, len(data) + 1, tickFreq))
    plt.show()

def plotContourRamps (data, contours):
    '''
    Extracts data points from the raw data at the given contour intervals.
    The contours arg is a list of probabilities 0.0 < contour <= 1.0.
    For example, contours=[0.05, 0.5, 0.95] plots the 5%, 50%, and 95% contours.
    '''
    rows = []
    for c in data:
        c.sort()
    for prob in contours:
        row = []
        rows.append(row)
        for c in data:
            n = len(c)
            index = round((n - 0.5) * prob)
            if index < 0:
                index = 0
            if index >= n:
                index = n - 1
            row.append(c[index])
    x = range(len(data))
    plt.ylim((-20, 30))
    plt.title('Ramp rates by usage contour')
    plt.grid(True)
    for y, lbl in zip(rows, contours):
        dy = []
        y.append(y[0])
        for i in range(len(y) - 1):
            dy.append(y[i + 1] - y[i])
        plt.plot(x, dy, label = '{0}%'.format(lbl * 100))
    plt.xlabel('Hour')
    plt.ylabel('Ramp rate (MW/hr)')
    plt.legend()
    tickFreq = 12
    if len(data) < 49:
        tickFreq = 2
    plt.xticks(np.arange(0, len(data) + 1, tickFreq))
    plt.show()

def plotHistogram ():
    '''
    Flattens the weekData and plots it as a histogram
    '''
    flat = [item for row in weekData for item in row]
    #print('range', min(flat), max(flat))
    fa = np.array(flat)
    mean = fa.mean()
    std = fa.std()
    print('mean = {:.2f}, std dev = {:.2f}'.format(mean, std))
    plt.hist(flat, bins=250, normed=True)
    meanMark = plt.axvline(x=mean, color='r', label='mean')
    stdMark = plt.axvline(x=mean+std, color='y', label='std')
    plt.axvline(x=mean-std, color='y')
    plt.title('Net Demand density')
    plt.xlabel('Net demand (MW)')
    plt.ylabel('Normalized frequency')
    plt.legend([meanMark, stdMark],
               ['mean = {:.2f}'.format(mean),
                'std dev = {:.2f}'.format(std)])
    plt.show()

def plotPeakHistogram (horizon):
    '''
    Plots the distribution of peak demand magnitudes over the given horizon,
    specified in days.
    '''
    peaks = []
    hours = horizon * 24
    for game in gameData:
        for i in range(len(game) - hours + 1):
            peak = max(game[i:i+hours], key=(lambda x: x[1]))
            peaks.append(peak[1])
    plt.hist(peaks, bins=250)
    plt.title('Peak demand distribution')
    plt.xlabel('Net peak demand, horizon = {} days'.format(horizon))
    plt.ylabel('Frequency (230 games)')
    plt.show()

def plotPeakHourDistribution (horizon, limit, n, weighted=False):
    '''
    Plots the hour-of-week for all top-n peak events greater than limit
    over the given horizon.
    '''
    week = 168
    peakData = [0 for x in range(week)]
    gamePeaks = []
    currentPeaks = []
    noPeakCount = 0
    integral = 0
    hours = horizon * 24
    # iterate over games
    for game in gameData:
        currentPeaks = []
        peakCount = 0
        hour = 0
        # iterate over hours in the game
        for item in game:
            [how, pwr] = item
            # clean out expired peaks
            for peak in currentPeaks:
                [hr, hw, pk] = peak
                if hr < (hour - hours):
                    peak[2] = 0 # smash the peak-power value
            # possibly add new peak
            if pwr > limit:
                currentPeaks.append([hour, how, pwr])
                peakCount += 1
            currentPeaks.sort(reverse=True, key=(lambda x: x[2]))
            # trim the peak list
            newPeaks = []
            for peak in currentPeaks:
                [hr, hw, pk] = peak
                if pk > 0:
                    newPeaks.append(peak)
            currentPeaks = newPeaks[0:n]
            # count the no-peak instances
            if len(currentPeaks) == 0:
                noPeakCount += 1
            # record the hours
            for [hr, hw, pk] in currentPeaks:
                if weighted:
                    wt = pk - limit
                else:
                    wt = 1
                peakData[hw] += wt
                integral += wt
            hour += 1
        gamePeaks.append(peakCount)

    if weighted:
        ttl = 'weighted '
        ylbl = 'Weight/game'
    else:
        ttl = ''
        ylbl = 'Observations/game'
    print("No peaks: {0} times per game".format(noPeakCount / len(gameData)))
    print("Integrated value per game", integral / len(gameData))
    for i in range(len(peakData)):
        peakData[i] /= len(gameData)
    x = range(len(peakData))
    plt.plot(x, peakData)
    plt.grid(True)
    plt.xticks(np.arange(0, week + 1, 12))
    plt.title('Net demand > {0} {1}distribution, {2}-day horizon'.format(limit, ttl, horizon))
    plt.xlabel('Hour of week')
    plt.ylabel(ylbl)
    plt.show()
   
def plotGamePeakHistogram (limit):
    '''
    Determines the number of peaks over limit for each game, plots result as
    histogram.
    '''
    gamePeaks = []
    # iterate over games
    for game in gameData:
        peakCount = 0
        # iterate over hours in the game
        for item in game:
            [how, pwr] = item
            # possibly add new peak
            if pwr > limit:
                peakCount += 1
        gamePeaks.append(peakCount)
    plt.hist(gamePeaks, bins=80)
    plt.title('Distribution of peaks > {} over games'.format(limit))
    plt.xlabel('Peak event count')
    plt.ylabel('Density')
    plt.show()
    
# ----------- debug init ---------
# collectData(tournament)
