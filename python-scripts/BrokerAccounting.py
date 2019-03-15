#!/usr/bin/python3
'''
Reads files produced by the BrokerAccounting logtool, pulls apart debit/credit
data, generates plots.

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
                brokerdata[broker][key].append(float(row[bi + v]))
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
    '''factor can be one of the keys in the broker accounting table'''

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
        pp.savefig(plotDir + '/' + saveAs + '.png')
        pp.savefig(plotDir + '/' + saveAs + '.svg')
    

# collectData('file:./finals-2018/finals_2018_07.games_.csv', 'finals-2018')
