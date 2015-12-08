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

Usage: Evaluate or import this module using your Python IDE,
then call collectData(tournament) to suck in all the data.
The tournament arg is a subdirectory of ../../games
containing the compressed game logs of the tournament. If necessary,
this will unpack the game logs and run the logtoolClass processor over each
state log to generate the raw data. Once this completes (can take several hours
the first time, a second or two on subsequent runs), use the various plotXxx
functions to do the plotting. For example, once this module is
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
import statistics as st
import math

logtoolClass = 'org.powertac.logtool.example.ProductionConsumption'
dataPrefix = 'data/prod-cons-'

#logtoolClass = 'org.powertac.logtool.example.SolarProduction'
#dataPrefix = 'data/solar-prod-'

def collectAllData (tournament, force=False):
    collectData(tournament, force=force)
    collectData(tournament, logtype='boot', force=force)

def collectData (tournament, logtype='sim', force=False):
    '''
    Processes data from sim data files in the specified directory.
    Use force=True to force re-analysis of the data. Otherwise the logtool
    code won't be run if its output is already in place.
    '''
    actualPrefix = dataPrefix
    if logtype != 'sim':
        actualPrefix = dataPrefix + '{}-'.format(logtype)
    for [gameId, dataFile] in di.datafileIter(tournament,
                                              logtoolClass,
                                              actualPrefix,
                                              logtype=logtype,
                                              force = force):
        # note that dataFile is a Path, not a string
        if logtype == 'sim':
            processFile(gameId, str(dataFile))
        else:
            processBootFile(gameId, str(dataFile))

gameData = []
gameDict = {}
bootDict = {}
bootStats = {}
weekData = [[] for x in range(168)]
weekdayData = [[] for x in range(24)]
weekendData = [[] for x in range(24)]
dayData = [[] for x in range(24)]
dataMap = {'Weekly':weekData, 'Weekday':weekdayData, 'Weekend':weekendData,
           'Daily':dayData}

def processFile (gameId, dataFile):
    '''
    Collects data from a data file into a global 168-column array,
    one column per hour-of-week.
    Note that the day-of-week numbers are in the range [1-7].
    '''
    data = open(dataFile, 'r')
    gameSeries = []
    gameData.append(gameSeries)
    gameDict[gameId] = gameSeries
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

def processBootFile (gameId, dataFile):
    '''
    Collects data from a bootstrap data file into a global 168-column array,
    one column per hour-of-week.
    Note that the day-of-week numbers are in the range [1-7].
    '''
    data = open(dataFile, 'r')
    gameSeries = []
    bootDict[gameId] = gameSeries
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
    hours = int(horizon * 24)
    for game in gameData:
        for i in range(len(game) - hours + 1):
            peak = max(game[i:i+hours], key=(lambda x: x[1]))
            peaks.append(peak[1])
    plt.hist(peaks, bins=250)
    plt.title('Peak demand distribution')
    plt.xlabel('Net peak demand, horizon = {} days'.format(horizon))
    plt.ylabel('Frequency (230 games)')
    plt.show()

def plotPeriodicPeakHistogram (period):
    '''
    Divides each game into periods of the specified length (specified in days),
    plots the distribution of peak demand magnitudes seen in each period.
    '''
    peaks = []
    hours = int(period * 24)
    for game in gameData:
        for i in range(0, len(game) - hours + 1, hours):
            peak = max(game[i:i+hours], key=(lambda x: x[1]))
            peaks.append(peak[1])
    plt.hist(peaks, bins=250)
    plt.title('Periodic peak demand distribution')
    plt.xlabel('Net peak demand, period = {} days'.format(period))
    plt.ylabel('Frequency (230 games)')
    plt.show()

def plotGamePeriodicPeaks (period, threshold):
    '''
    Discovers the over-threshold peaks over the given periods in each game,
    computes the weighted sum per game, plots result as histogram.
    '''
    games = []
    hours = int(period * 24)
    for game in gameData:
        gameWeight = 0
        for i in range(0, len(game) - hours + 1, hours):
            peak = max(game[i:i+hours], key=(lambda x: x[1]))
            if peak[1] > threshold:
                gameWeight += peak[1] - threshold
        games.append(gameWeight)
    # Report stats
    fa = np.array(games)
    print('mean = {:.2f}, std = {:.3f}'.format(fa.mean(), fa.std()))
    # Plot histogram
    plt.hist(games, bins = 50)
    plt.title('Weighted total of {}-day peaks over {} per game'
              .format(period, threshold))
    plt.xlabel('Weighted total')
    plt.ylabel('Number of games')
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

def plotStdContours(mult, contours):
    '''
    Computes mean and standard deviation from end-of-boot through end-of-game,
    plots pk = (mean + mult * std_dev) contours
    '''
    games = [] # capture pk values for each game in each timeslot
    # timeslot zero comes from the boot data
    index = 0
    minCount = 1e6
    for key in bootDict:
        # iterate over games
        series = bootDict[key]
        game = []
        games.append(game)
        cons = [d for [h,d] in series]
        mean = st.mean(cons)
        std = st.stdev(cons)
        game.append(mean + mult * std)
        gsum = sum(cons)
        gsumsq = sum([math.pow(d - mean, 2.0) for d in cons])
        count = len(cons)
        for [how, gcons] in gameDict[key]:
            # iterate over timeslots in game
            gsum += gcons
            count += 1
            mean = gsum / count
            gsumsq += math.pow(gcons - mean, 2.0)
            std = math.sqrt(gsumsq / count)
            game.append(mean + mult * std)
        if len(game) < minCount:
            minCount = len(game)
    # Each row in games is now a game.
    # We need rows to be timeslots
    timeslots = [[] for idx in range(minCount)]
    for ts in range(len(timeslots)):
        for game in range(len(games)):
            timeslots[ts].append(games[game][ts])

    rows = []
    for c in timeslots:
        c.sort()
    for prob in contours:
        row = []
        rows.append(row)
        for c in timeslots:
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

    x = [d/168 for d in range(len(timeslots))]
    plt.grid(True)
    plt.title('peak demand contours, mult={}'.format(mult))
    for y,lbl in zip(rows, contours):
        plt.plot(x, y, label = '{0}%'.format(lbl * 100))
    plt.xlabel('Week')
    plt.ylabel('Peak demand threshold (MW)')
    plt.legend()
    plt.show()

def bootTransitionHistogram (ratio=False):
    '''
    Plots histogram of the difference between the mean demand during the
    2-week boot period and the first 2 weeks of the corresponding sim.
    '''
    diffs = []
    for key in bootDict:
        boot = [d for [h,d] in bootDict[key]]
        game = [d for [h,d] in gameDict[key][:(2 * 7 * 24)]]
        if ratio:
            diffs.append(st.mean(game) / st.mean(boot))
        else:
            diffs.append(st.mean(game) - st.mean(boot))
    plt.hist(diffs, bins=50)
    op = '-'
    unit = ' (MW)'
    if ratio:
        op = '/'
        unit = ''
    plt.title('Distribution of game {} boot demand over games'.format(op))
    plt.xlabel('mean game demand {} mean boot demand{}'.format(op, unit))
    plt.ylabel('observations')
    plt.show()

    
# ----------- debug init ---------
tournament = 'finals-201504'
#tournament = 'finals-2014'
# collectData(tournament)
