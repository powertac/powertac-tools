#!/usr/bin/python3

'''
Reads a set of data files containing 2D arrays of market clearing data,
each entry of the form [mwh, price]. 
Array is indexed by target ts, leadtime, and qty/unit-price.
Data is gathered from a tournament directory and plotted by
hour-of-week or hour-of-day, aggregated across games in the tournament.

Depends on a Python 3 installation with the SciPy/NumPy libraries
Tournament logs (in the form 'game-n-sim-logs.tar.gz') must be in a directory
named by the tournamentDir arg to collectData(). The Java
modules in dir ../logtool-examples must have been built (using mvn clean test).
'''

from numpy import *
import scipy, pylab, matplotlib
from pylab import *
from scipy import stats
import string,re,os
import TournamentLogtoolProcessor as tl

logtoolClass = 'org.powertac.logtool.example.MktPriceStats'
tournamentDir = 'finals-2018'
dataPrefix = 'mktPr-'
initialSkip = 6 # timeslots to skip at start
plotDir = 'plots/'

gameData = []
weekData = [[] for x in range(168)]
weekdayData = [[] for x in range(24)]
weekendData = [[] for x in range(24)]
dayData = [[] for x in range(24)]
dataMap = {'Weekly':weekData, 'Weekday':weekdayData, 'Weekend':weekendData,
           'Daily':dayData}

def collectData (csvUrl, tournamentDir, em=False):
    '''
    Processes data from data files in the specified directory.
    '''
    for info in tl.dataFileIter(csvUrl, tournamentDir,
                                logtoolClass,
                                dataPrefix, em=em):
        # note that dataFile is a Path, not a string
        processFile(str(info['path']))

# collectData('file:./finals-2018/finals_2018_07.games_.csv', 'finals-2018')

def processFile (filename):
    '''
    Given a filename, reads the file, reduces the data to mean $/MW
    and adds to the various datasets.
    '''
    print('opening', filename)
    datafile = open(filename, 'r')
    drop = initialSkip
    game = []
    gameData.append(game)
    lineCount = 0
    for line in datafile.readlines():
        row = []
        lineCount += 1
        if drop > 0:
            drop -= 1
            continue
        tokens = line[:-1].split(',') # remove EOL, tokenize on comma
        timeslot = 0
        dow = 0
        hod = 0
        offset = 0 # where to start looking for [qty price] pairs
        if line[0] != '[':
            # get the timeslot info
            timeslot = int(tokens[0])
            dow = int(tokens[1])
            hod = int(tokens[2])
            offset = 3
            #print('ts={}, dow={}, hod={}, tokens={}'.format(timeslot, dow, hod, tokens))
        for pair in tokens[offset:]:
            nums = pair[1:-1].split(' ') # leave off []
            row.append([floatMaybe(nums[0]), floatMaybe(nums[1])])
        if len(row) < 24:
            print('short row', lineCount, len(row))
        # Reduce to single pair/timeslot
        pair = [0, 0]
        for cell in row:
            pair[0] += cell[0] # sum quantity
            pair[1] += cell[0] * cell[1] # sum cost
        if pair[0] != 0:
            pair[1] = pair[1] / pair[0] # convert cost back to price
        how = (dow - 1) * 24 + hod # hour of week
        game.append(pair)
        weekData[how].append(pair)
        dayData[hod].append(pair)
        if dow <= 5:
            weekdayData[hod].append(pair)
        else:
            weekendData[hod].append(pair)

def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str != '' :
        result = float(str)
    return result

def meanPrices (data):
    '''computes mean prices for delivered power in each timeslot'''
    result = []
    for ts in data:
        # 24 [mwh, price] entries
        qty = 0.0
        cost = 0.0
        for pair in ts:
            qty += float(pair[0])
            cost += float(pair[0]) * float(pair[1])
        if qty == 0.0:
            result.append(0.0)
        else:
            result.append(cost/qty)
    return result

#prices = getData("../../powertac-tools/logtool/clearedTrades.data",
#                 rawData)
#avgPrices = meanPrices(prices)

def dailyPrices (meanPrices):
    '''computes hourly mean prices and std dev across all days'''
    means = []
    stds = []
    # Throw away the first 21 timeslots, since data starts at 0300
    for i in range(0, 24):
        # for each hour, collect observations
        obs = []
        for j in range(i, len(meanPrices), 24):
            if meanPrices[j] != 0:
                obs.append(meanPrices[j])
        # obs is now a list of mean prices for hour i
        d = array(obs)
        means.append(scipy.mean(d))
        stds.append(scipy.std(d))
    return [means, stds]

def plotDailyPrices (dailyPrices, ymin=0, ymax=0):
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.errorbar(range(24), dailyPrices[0], dailyPrices[1])
    if ymin != 0 or ymax != 0:
        axis(ymin = ymin)
        axis(ymax = ymax)
    xlabel('hour')
    ylabel('price in $/MWh')
    show()

def priceByLeadtime (data):
    '''Computes normalized price vs leadtime, subtracting out the\
       mean price for each timeslot'''
    means = meanPrices(data)
    timeslots = []
    for i in range(len(data)):
        row = []
        for entry in data[i]:
            if entry[0] != 0:
                row.append(entry[1] - means[i])
            else:
                row.append(0.0)
        timeslots.append(row)
    # timeslots is now a row for each ts, columns are normalized prices
    means = []
    stds = []
    print('timeslots[', len(timeslots), len(timeslots[0]), ']')
    for col in range(len(timeslots[0])):
        obs = []
        for row in range(len(timeslots)):
            if len(timeslots[row]) < col + 1:
                print('short row', row, len(timeslots[row]))
                continue
            item = timeslots[row][col]
            if item != 0.0:
                obs.append(item)
        d = array(obs)
        means.append(scipy.mean(d))
        stds.append(scipy.std(d))
    return [means, stds]

def plotPricesByLeadtime (data, ymin=0, ymax=0):
    stats = priceByLeadtime(data)
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.errorbar(range(1, len(stats[0]) + 1), stats[0], stats[1])
    if ymin != 0 or ymax != 0:
        axis(ymin = ymin)
        axis(ymax = ymax)
    xlabel('leadtime')
    ylabel('price deviation in $/MWh')
    show()

def plotPrices (meanPrices, start=100, end=1300):
    '''Plots mean prices for a range of timeslots'''
    if end == -1:
        end = len(meanPrices)
    fig = figure()
    pltData = array(meanPrices[start:end])
    ax = fig.add_subplot(1,1,1)
    #x = range(start, end)
    ax.plot(pltData)#, label=labels)
    ax.set_xticks(range(0, end-start, 24))
    xlabel('hour')
    ylabel('price in $/MWh')
    #legend(loc='upper left')

# columns are indices of the series to plot. Typically the last
# two are aggregates.
def plotDataWithLabels (data, labels, columns, low=1, high=-1):
    if high == -1:
        high = len(data)
    pltData = array(data[low:high])
    rows = shape(pltData)[0]
    x = range(low, high)
    for i in range(len(columns)):
        index = columns[i]
        col = pltData[0:len(pltData), index:index+1].reshape(rows)
        plot(x, col, label=labels[index])
    legend(loc='lower left')
    show()

def plotContours (dataName, contours, saveAs=''):
    '''
    Extracts data points from the raw data at the given contour intervals.
    The contours arg is a list of probabilities 0.0 < contour <= 1.0.
    For example, contours=[0.05, 0.5, 0.95] plots the 5%, 50%, and 95% contours.
    '''
    data = []
    for day in dataMap[dataName]:
        data.append([pair[1] for pair in day])
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
    plt.title('{} wholesale price contours'.format(dataName))
    #plt.ylim((0, 110))
    for y,lbl in zip(rows, contours):
        plt.plot(x, y, label = '{0}%'.format(lbl * 100))
    plt.xlabel('Hour')
    plt.ylabel('Wholesale price ($/MWh)')
    plt.legend()
    tickFreq = 12
    if len(data) < 49:
        tickFreq = 2
    plt.xticks(np.arange(0, len(data) + 1, tickFreq))
    if saveAs == '':
        plt.show()
    else:
        plt.savefig(plotDir + saveAs + '.png')

def plotHistogram ():
    '''
    Flattens the weekData and plots it as a histogram
    '''
    flat = [item[1] for row in weekData for item in row]
    #print('range', min(flat), max(flat))
    fa = np.array(flat)
    mean = fa.mean()
    std = fa.std()
    print('mean = {:.2f}, std dev = {:.2f}, max = {:.2f}, min = {:.2f}'
          .format(mean, std, fa.max(), fa.min()))
    plt.hist(flat, bins=250, normed=True)
    meanMark = plt.axvline(x=mean, color='r', label='mean')
    stdMark = plt.axvline(x=mean+std, color='y', label='std')
    plt.axvline(x=mean-std, color='y')
    plt.title('Wholesale price density')
    plt.xlabel('Wholesale price ($/MWh)')
    plt.ylabel('Normalized frequency')
    plt.legend([meanMark, stdMark],
               ['mean = {:.2f}'.format(mean),
                'std dev = {:.2f}'.format(std)])
    plt.show()

def qtyCostPrice ():
    '''
    Extracts total quantity, total cost, mean price from the game data.
    '''
    qtys = []
    costs = []
    prices = []
    for game in gameData:
        # game is an array of [qty, price], one per timeslot
        qty = 0
        cost = 0
        for item in game:
            qty += item[0]
            cost += item[0] * item[1]
        qtys.append(qty)
        costs.append(cost)
        prices.append(cost / qty)
    return qtys, costs, prices

def plotTotalCost ():
    qtys, costs, prices = qtyCostPrice()
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.hist(costs, bins=50)
    ax.set_title('Total wholesale energy cost')
    #ax.ticklabel_format(axis='x', style='sci')
    ax.set_xlabel('Energy cost')
    ax.set_ylabel('Number of games')
    formatter = matplotlib.ticker.ScalarFormatter()
    formatter.set_powerlimits((-3,4))
    ax.xaxis.set_major_formatter(formatter)
    plt.show()

def plotTotalConsumption ():
    qtys, costs, prices = qtyCostPrice()
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.hist(qtys, bins=50)
    plt.title('Total wholesale energy consumption')
    plt.xlabel('Energy consumed')
    ax.set_ylabel('Number of games')
    formatter = matplotlib.ticker.ScalarFormatter()
    formatter.set_powerlimits((-3,4))
    ax.xaxis.set_major_formatter(formatter)
    plt.show()

def plotMeanPrices ():
    qtys, costs, prices = qtyCostPrice()
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.hist(prices, bins=50)
    plt.title('Mean wholesale price distribution')
    plt.xlabel('Mean energy price')
    ax.set_ylabel('Number of games')
    ax.set_ylabel('Number of games')
    plt.show()

def plotCostQtyPrice ():
    qtys, costs, prices = qtyCostPrice()
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    sc = ax.scatter(costs, qtys, c = prices, cmap = 'rainbow')
    ax.set_title('Cost, consumption, price for 2015 final games')
    ax.set_xlabel('Total energy cost')
    ax.set_ylabel('Total energy usage')
    plt.colorbar(sc, label='mean price/MWh')
    formatter = matplotlib.ticker.ScalarFormatter()
    formatter.set_powerlimits((-3,4))
    ax.xaxis.set_major_formatter(formatter)
    plt.show()
