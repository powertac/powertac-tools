#!/usr/bin/python3
'''
Reads files produced by the BrokerAccounting logtool, pulls apart debit/credit
data, generates plots. Plots are generated in a

Requires a Python 3 installation with SciPy/NumPy libraries.
Assumes BrokerAccounting tool is built in parallel directory ../logtool-examples
'''

import TournamentLogtoolProcessor as tl
from numpy import *
import scipy, matplotlib
import matplotlib.pyplot as pp
from pylab import *
from scipy import stats
import string, re, os

# Dict indexed by broker usernames
# For each broker, dict indexed by report column, containing sums from each game
# Sums in selected columns scaled by (nbrokers/8)
brokerSummaries = {}
scaledColumns = {'ttx-uc','ttx-ud','mtx-c','mtx-d','btx-c','btx-d',
                 'dtx-c','dtx-d','ctx-c','ctx-d','bce-c','bce-d',
                 'bank-c','bank-d'}

plotDir = './plots'

def init ():
    brokerSummaries = {}

# --- data collection ---

def collectData (tournamentCsvUrl, tournamentDir):
    init()
    global plotDir
    plotDir = tournamentDir + '/' + 'analysis'
    for f in tl.dataFileIter(tournamentCsvUrl,
                             tournamentDir,
                             'org.powertac.logtool.example.BrokerAccounting',
                             'ba'):
        processGame(f['path'])

# --- data extraction ---

sums = {'ttx-s': ('ttx-sc', 'ttx-sd'),
        'ttx-u': ('ttx-uc', 'ttx-ud'),
        'mtx': ('mtx-c', 'mtx-d'),
        'btx': ('btx-c', 'btx-d'),
        'dtx': ('dtx-c', 'dtx-d'),
        'ctx': ('ctx-c', 'ctx-d'),
        'bce': ('bce-c', 'bce-d'),
        'bank': ('bank-c', 'bank-d')}

def processGame (filepath):
    datafile = open(filepath, 'r')
    brokerdata = {}
    badFile = False
    # first line is heading
    header = datafile.readline().strip().split(',')
    brokerOffsets = getBrokerOffsets(header)
    itemOffsets = getItemOffsets(header)
    for line in datafile.readlines():
        row = line.strip().split(',')
        for bi in brokerOffsets:
            broker = row[bi]
            if broker not in brokerdata:
                brokerdata[broker] = {}
            for k, v in itemOffsets.items():
                key = header[bi + v]
                if key not in brokerdata[broker]:
                    brokerdata[broker][key] = []
                value = float(row[bi + v])
                if value > 1e9:
                    badFile = True
                    print("Bad file", filepath)
                    return
                brokerdata[broker][key].append(value)
    for bk, data in brokerdata.items():
        if bk not in brokerSummaries:
            brokerSummaries[bk] = {}
        summ = brokerSummaries[bk]
        for k, v in data.items():
            if k not in summ:
                summ[k] = []
            # Scale the results
            summ[k].append(sum(v) * (len(brokerdata) - 1.0) / 8.0)
        for tag in sums:
            summ[tag] = [sum(t) for t in zip(summ[sums[tag][0]],
                                             summ[sums[tag][1]])]

def getBrokerOffsets (header):
    ''' header is a list of tokens, including 'brokern' entries'''
    brokerIndex = 0
    offsets = []
    more = True
    while more:
        tag = 'broker' + str(brokerIndex)
        if tag in header:
            offsets.append(header.index(tag))
            brokerIndex += 1
        else:
            more = False
    return offsets

def getItemOffsets (header):
    offsets = {}
    more = True
    start = header.index('broker0')
    ptr = 0
    while more:
        ptr += 1
        if header[start + ptr] == 'broker1':
            more = False
        else:
            offsets[header[start + ptr]] = ptr
    return offsets

# --- plotting ---

def brokerDistributions (title, factor, saveAs = ''):
    '''
    Plots distributions of a factor across brokers.
    Factor can be one of the keys in the scaledColumns list,
    or one of the keys in the sums table defined above'''

    data = [v[factor]
            for k,v in brokerSummaries.items()
            if k != 'default broker']
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.violinplot(data, showmeans=False, showmedians=True)
    ax.set_title(title)
    ax.yaxis.grid(True)
    brokers = [' ']
    brokers = brokers + [b for b in brokerSummaries.keys() if b != 'default broker']
    ax.set_xticks(np.arange(len(brokers)))
    ax.set_xticklabels(brokers, rotation='vertical')
    fig.tight_layout()
    if saveAs == '':
        fig.show()
    else:
        pp.savefig(plotDir + '/' + saveAs + '.png', transparent=True)
        pp.savefig(plotDir + '/' + saveAs + '.svg', transparent=True)

def factorDistributions (title, broker,
                         factors = ['ttx-s', 'ttx-u', 'mtx', 'ctx', 'btx', 'bce', 'bank'],
                         yLimit = [],
                         saveAs = ''):
    '''
    Plots distributions of a set of factors for a single broker in a tournament.
    '''

    data = []
    for k in brokerSummaries[broker].keys():
        if k in factors:
            data.append(brokerSummaries[broker][k])
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.violinplot(data, showmeans=False, showmedians=True)
    ax.set_title(title)
    ax.yaxis.grid(True)
    if len(yLimit) == 2:
        ax.set_ylim(yLimit[0], yLimit[1])
    factors = [' '] + factors
    ax.set_xticks(np.arange(len(factors)))
    ax.set_xticklabels(factors)
    #fig.tight_layout()
    if saveAs == '':
        fig.show()
    else:
        pp.savefig(plotDir + '/' + saveAs + '.png', transparent=True)
        pp.savefig(plotDir + '/' + saveAs + '.svg', transparent=True)

# The following requires that
# a. the logtool-examples module has been built (mvn clean test), and
# b. both this directory (powertac-tools/python-scripts) and the
#    parallel directory powertac-tools/logtool examples contain symlinks to
#    a directory finals-2018 where the downloaded logs will be downloaded
#    and extracted, and where the data files will be created.
#
# collectData('file:./finals-2018/finals_2018_07.games_.csv', 'finals-2018')
