#!/usr/bin/python3
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
The tournament arg is typically a subdirectory of ../../games
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
import statistics as st
import math, copy

import GameData as gc

# fill in with an appropriate GameData instance
#gameData = gc.GameData()

def init ():
    gameSummaries = []
    gameSizes = []
    brokerData = {}

def plotMeans (treatment='', dataInterval='Daily',
               dataType='net-demand', title='', showTitle=False):
    '''
    Reduces the raw data into means and 1-sigma error bars, plots result
    '''
    data = gameData.getDataArray(treatment, dataInterval)
    d = [np.array(c) for c in data]
    print('Shape:', np.shape(d))
    means = [c.mean() for c in d]
    stds = [c.std() for c in d]
    x = range(np.shape(d)[0])
    if title != '':
        plt.title(title)
    elif showTitle:
        plt.title('Mean {} {}, 1-sigma error bars'.format(dataInterval, dataType))
    plt.errorbar(x, means, yerr=stds)
    plt.xlabel('hour')
    plt.ylabel('Net demand (MW)')
    plt.show()

def plotContours (contours,
                  treatment='',
                  dataInterval='Daily',
                  dataType='net-demand',
                  tournamentYear = '',
                  ylimit=0,
                  units='MW',
                  title='', showTitle=False):
    '''
    Extracts data points from the raw data at the given contour intervals.
    The contours arg is a list of probabilities 0.0 < contour <= 1.0.
    For example, contours=[0.05, 0.5, 0.95] plots the 5%, 50%, and 95% contours.
    '''
    #Fix!
    #if gameData.dataType != dataType:
    #    gameData.reset(dataType, tournamentYear)
    data = gameData.getDataArray(treatment, dataInterval)
    #data = gameData.dataArray(dataInterval)
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
    if title != '':
        plt.title(title)
    elif showTitle:
        plt.title('{} {} contours {}'.format(dataInterval,
                                             gameData.dataType,
                                             gameData.tournamentYear))
    if ylimit > 0:
        plt.ylim((0, ylimit))
    for y,lbl in zip(rows, contours):
        plt.plot(x, y, label = '{0}%'.format(lbl * 100))
    plt.xlabel('Hour')
    plt.ylabel('{} ({})'.format(dataType, units))
    plt.legend()
    tickFreq = 12
    if len(data) < 49:
        tickFreq = 2
    elif len(data) > 168:
        #print(len(data))
        tickFreq = 168
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

def computeIntervalPeaks (interval, threshold=1.6, npeaks=3, impute=False):
    '''
    For a given interval in days, and a given threshold, peaks are detected
    according to the method specified in the 2016 spec. The multiplier is the
    multiple of the stdev that defines a peak. Output is a list of triples, each
    containing the timeslot index (starting at the start of boot), the threshold,
    and the amount by which the peak exceeded the threshold in kWh.
    If impute is True, then boot data is not used, but rather imputed by
    sampling the game data. This is needed to get good results from
    pre-2016 games where demand elasticity was excessively high.
    '''
    # start by finding the mean consumption in the boot sim records, so we
    # can normalize the boot numbers. Necessary only for records prior to
    # 2016
    if gameData.dataType != 'net-demand':
        gameData.reset('net-demand')
    if impute:
        gameData.imputeBootData()
    else:
        gameData.ensureBootData()
    gameData.ensureGameData()
    #bootScale = {}
    bc = copy.deepcopy(gameData.bootDict)
    gc = copy.deepcopy(gameData.gameDict)
    print('{} games, {} boots'.format(len(gc), len(bc)))
    #for gameId in bc.keys():
    #    bootMean = st.mean(bc[gameId])
    #    gameMean = st.mean([x[1] for x in gc[gameId]])
    #    bootScale[gameId] = gameMean / bootMean

    # Get the production numbers
    #gameData.reset('production')
    #gameData.ensureBootData()
    #gameData.ensureGameData()
    #bp = gameData.bootDict
    #gp = gameData.gameDict

    # For each game, walk the boot record, then walk the game and collect
    # peak events for each interval, as specified by the interval, threshold,
    # and npeaks values. Each peak is recorded as [ts, val] where val is the
    # amount by which the peak exceeds the threshold
    results = {}
    for gameId in gc.keys():
        print('game {}'.format(gameId))
        runningMean = 0.0
        runningVar = 0.0
        runningSigma = 0.0
        runningCount = 0
        scale = 1.0
        # process a single boot record by the method of Welford, as outlined
        # in Knuth ACP, vol 2 Seminumerical Algorithms, Sec. 4.2.2 Eq. 15, 16.
        for net in bc[gameId]:
            #net = prod + scale * cons
            if runningCount == 0:
                # first time through
                runningMean = net
                runningCount = 1
            else:
                lastM = runningMean
                runningCount += 1
                runningMean = lastM + (net - lastM) / runningCount
                runningVar = runningVar + (net - lastM) * (net - runningMean)
                runningSigma = math.sqrt(runningVar / (runningCount -1))
        # process the corresponding game
        nets = []
        remaining = interval * 24
        result = []
        results[gameId] = result
        for net in gc[gameId]:
            #net = prod[1] + cons[1]
            nets.append([runningCount, net[1]])
            
            lastM = runningMean
            runningCount += 1
            runningMean = lastM + (net[1] - lastM) / runningCount
            runningVar = runningVar + (net[1] - lastM) * (net[1] - runningMean)
            runningSigma = math.sqrt(runningVar / (runningCount -1))
            
            remaining -= 1
            if remaining == 0:
                # time to assess
                nets.sort(reverse=True, key=lambda x: x[1])
                thr = runningMean + threshold * runningSigma
                for i in range(npeaks):
                    ev = nets[i]
                    if ev[1] > thr:
                        result.append([ev[0], thr, ev[1] - thr])
                remaining = interval * 24
                nets = []
    return results                    


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

#tournament = 'finals-201504'
# collectData(tournament)
