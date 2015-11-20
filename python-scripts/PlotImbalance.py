#!/usr/bin/python

'''
Reads files produced by the ImbalanceSummary logtool script, and generates
plots.

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
import DatafileIterator as di

logtoolClass = 'org.powertac.logtool.example.ImbalanceSummary'
dataPrefix = 'data/imbalance-'

# Extracted data
gameSummaries = []
gameSizes = []
brokerData = {}

# offsets
c_ = 0
cr_ = 1
p_ = 2
pr_ = 3
i_ = 4
i_rms = 5
ir_ = 6

def collectData (tournamentDir):
    '''
    Processes data from data files in the specified directory.
    '''
    for dataFile in di.datafileIter(tournamentDir,
                                    logtoolClass,
                                    dataPrefix):
        # note that dataFile is a Path, not a string
        processFile(str(dataFile))

def processFile (filename):
    datafile = open(filename, 'r')

    # first line is game summary
    line = datafile.readline()
    tokens = line[:-1].split(',') # remove EOL, tokenize on comma
    # first two tokens are game_id, n_brokers
    gameSizes.append(int(tokens[1]))
    row = []
    gameSummaries.append(row)
    for token in tokens[2:]:
        row.append(floatMaybe(token))

    # remainder of file is broker data
    for line in datafile.readlines():
        tokens = line[:-1].split(',')
        brokername = tokens[0]
        if brokername not in brokerData:
            brokerData[brokername] = []
        rows = brokerData[brokername]
        row = []
        rows.append(row)
        for token in tokens[1:]:
            row.append(floatMaybe(token))

def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str != '' :
        result = float(str)
    return result

def usageImbalance ():
    usage = [-item[c_]/1000000 for item in gameSummaries]
    imbalance = [item[i_]/1000000 for item in gameSummaries]
    sc = plt.scatter(usage, imbalance, c = gameSizes, cmap = 'rainbow')
    plt.title('Total energy usage vs total imbalance, 2015 finals')
    plt.xlabel('Total customer energy usage (GWh)')
    plt.ylabel('Total imbalance (GWh)')
    plt.colorbar(sc, label = 'number of brokers')
    plt.show()

    
