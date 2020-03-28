#!/usr/bin/python3
'''
Extracts and charactierizes the ratios of
  balancing-market-clearing-price / bootstrap-market-price
for all the games in a tournament.
'''

import TournamentLogtoolProcessor as TLP
from numpy import *
import scipy, matplotlib
import matplotlib.pyplot as pp
from pylab import *
#from scipy import stats
import statistics
import csv, string, re, os

plotDir = './plots'
upregRatios = []
upregMeans = []
downregRatios = []
downregMeans = []
priceOffset = 0.0
epsilon = .00001
forceExtract = False
perGame = True

# --- data collection ---

def collectData (tournamentCsvUrl, tournamentDir):
    global plotDir
    plotDir = tournamentDir + '/' + 'analysis'
    #count = 2
    for f in TLP.dataFileIter(tournamentCsvUrl,
                             tournamentDir,
                             'org.powertac.logtool.example.ImbalanceCostAnalysis',
                             'ica', force=forceExtract):
        processGame(f['path'])
        #count = count - 1
        #if count < 0:
        #    break

def processGame (filepath):
    ''' input file formatted as csv with columns
        timeslot; mean-boot-price; imbalance; imbalance-price; ratio
    '''
    datafile = open(filepath, 'r')
    reader = csv.reader(datafile, delimiter=';')
    headers = next(reader)
    for row in reader:
        #print(row)
        mbp = float(row[1])
        price = float(row[3])
        ratio = float(row[4])
        if float(row[2]) < 0:
            ratio = (price + priceOffset) / -mbp
            upregRatios.append(ratio)
        else:
            ratio = (price - priceOffset - 2 * mbp)/ -mbp
            downregRatios.append(ratio)
    #upregDesc = stats.describe(upregRatios)
    #downregDesc = stats.describe(downregRatios)
    #print(filepath)
    #print('upreg', upregDesc)
    #print('downreg', downregDesc)
    if perGame:
        upregMeans.append(statistics.mean(upregRatios))
        upregRatios.clear()
        downregMeans.append(statistics.mean(downregRatios))
        downregRatios.clear()

def plotHistogram ():
    fig = plt.figure(figsize=(10, 4))
    ax1 = fig.add_subplot(1, 2, 1)
    ax1.set_xlabel('clearing price ratio')
    ax2 = fig.add_subplot(1, 2, 2)
    ax2.set_xlabel('clearing price ratio')
    ax1.hist(upregMeans, bins=25)
    ax1.set_title('Up regulation')
    ax2.hist(downregMeans, bins=25)
    ax2.set_title('Down regulation')
    fig.show()

# uncomment to execute directly
perGame = True
priceOffset = 0.0
collectData('file:./finals-2019/finals_2019_07.games_.csv', 'finals-2019')
plotHistogram()
