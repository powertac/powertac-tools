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
import csv
import TournamentLogtoolProcessor as tl

logtoolClass = {'summary': 'org.powertac.logtool.example.ImbalanceSummary',
                'detail': 'org.powertac.logtool.example.ImbalanceStats'}
dataPrefix = {'summary': 'imbSum', 'detail': 'imbStats'}

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

def init ():
    gameSummaries = []
    gameSizes = []
    brokerData = {}

def collectData (tournamentCsvUrl, tournamentDir, datatype='summary'):
    '''
    Processes data from data files in the specified directory.
    '''
    init()
    for f in tl.dataFileIter(tournamentCsvUrl,
                             tournamentDir,
                             logtoolClass[datatype],
                             dataPrefix[datatype]):
        # note that f is a dict{path, gameId}
        if datatype == 'summary':
            processSummary(f['path'])
        else:
            processDetail(f['path'])

def processSummary (filepath):
    datafile = open(filepath, 'r')
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
    if str not in ['', '-']:
        result = float(str)
    return result


def usageImbalance (tournament):
    usage = [-item[c_]/1000000 for item in gameSummaries]
    imbalance = [item[i_]/1000000 for item in gameSummaries]
    sc = plt.scatter(usage, imbalance, c = gameSizes, cmap = 'rainbow')
    plt.title('Total net consumption vs total imbalance, ' + tournament)
    plt.xlabel('Total customer energy usage (GWh)')
    plt.ylabel('Total imbalance (GWh)')
    plt.colorbar(sc, label = 'number of brokers')
    plt.show()

rows2drop = 8
def processDetail (filepath):
    data = open(filepath, 'r', newline='')
    # first two columns are game_id, timeslot
    hdr = data.readline().strip().split(',')[2:-2]
    for broker in hdr:
        if broker not in brokerData:
            brokerData[broker] = []
    drop = rows2drop
    for line in data.readlines():
        drop -= 1
        if drop >= 0:
            continue
        row = line.strip().split(',')[2:-2]
        if len(row) < len(hdr):
            # deal with truncated files
            break
        for broker in hdr:
            idx = hdr.index(broker)
            data = floatMaybe(row[idx])
            if data != 0.0:
                brokerData[broker].append(data * len(hdr) / 8000)


def plotBrokers (title):
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.violinplot(brokerData.values(), showmeans=False, showmedians=True)
    ax.set_title(title)
    ax.yaxis.grid(True)
    ax.set_ylabel("MW")
    ax.set_xlabel("Broker")
    brokers = [' ']
    brokers = brokers + [b for b in brokerData.keys()]
    ax.set_xticks(np.arange(len(brokers)))
    ax.set_xticklabels(brokers, rotation='vertical')
    fig.tight_layout()
    fig.show()

    
#collectData('file:./finals-2017/finals_2017_06.games.csv', 'finals-2017')
