#!/usr/bin/python3

'''
Reads a set of data files containing capacity-transaction data from a tournament.
Each file contains a line for each timeslot in which capacity transactions
were issued, of the form slot, threshold, excess, cost. Computes and plots
stats for these three values vs time-in-game across the entire tournament.
'''

from numpy import *
import scipy, pylab, matplotlib
from pylab import *
from scipy import stats
import string,re,os
import csv, json
from pathlib import Path
import TournamentGameIterator as ti
import TournamentLogtoolProcessor as tl

# Arrays indexed by timeslot, then by game
#threshold = {}
#excess = {}
#cost = {}
# for each game, an array indexed by assessment. Index o is boot threshold
games = {}  #{game: [{threshold:t, excess:e, cost:c}]}

csv.register_dialect('CaDialect', skipinitialspace=True, quoting=csv.QUOTE_NONE)

def dataFileIter (tournamentCsvUrl, tournamentDir):
    '''
    Iterates through games in a tournament specified in the CsvUrl, downloads
    if necessary, unpacks if necessary, runs the CapacityAnalysis extractor
    on each one, extracts threshold, excess, and cost from the resulting
    data files.
    '''
    for f in tl.dataFileIter(tournamentCsvUrl, tournamentDir,
                             'org.powertac.logtool.example.CapacityAnalysis',
                             'ca'):
        extractData(f['path'], f['gameId'])


def traceFileIter (tournamentCsvUrl, dirPath):
    '''
    Iterates through trace logs extracting mean and sigma values from
    their boot records as reported by DistributionUtilityService, along
    with the mean and sigma values from each assessment period.
    NOTE: this code assumes it will run BEFORE the logtool extraction
    '''
    stdKey = re.compile('distributionUtilityService.stdCoefficient=([1-9]+.[1-9]+),')
    meanKey = re.compile('mean = ([0-9]+.[0-9]+)')
    sigKey = re.compile('sigma = ([0-9]+.[0-9]+)')
    for gameDict in ti.csvIter(tournamentCsvUrl, dirPath):
        game = gameDict['gameId']
        sim = gameDict['sim']
        #print("sim={}".format(sim))
        trace = open(str(sim).replace('state', 'trace'))
        if game not in games:
            games[game] = []
        state = 'boot'
        for line in trace:
            if state == 'boot':
                if 'DistributionUtilityService: Bootstrap data' in line:
                    mean = floatMaybe(meanKey.search(line).group(1))
                    sigma = floatMaybe(sigKey.search(line).group(1))
                    state = 'config'
            elif state == 'config':
                if 'CompetitionControlService: Published configuration' in line:
                    stdCoeff = floatMaybe(stdKey.search(line).group(1))
                    th = mean + sigma * stdCoeff
                    games[game].append({'mean': mean,
                                        'sigma': sigma,
                                        'threshold':th,
                                        'excess':0.0,
                                        'cost':0.0})
                    state = 'assess'
            elif state == 'assess':
                if 'Peak-demand assessment' in line:
                    state = 'mean'
            elif state == 'mean':
                if 'Net demand k' in line:
                    mean = floatMaybe(meanKey.search(line).group(1))
                    sigma = floatMaybe(sigKey.search(line).group(1))
                    games[game].append({'mean': mean,
                                        'sigma': sigma})
                    state = 'assess'
                
        
def extractData (datapath, game):
    '''
    Extracts threshold, excess, and cost info from datafiles produced
    by the CapacityAnalyzer logtool.
    '''
    data = open(datapath, newline='')
    rdr = csv.DictReader(data, dialect='CaDialect')
    for index, row in enumerate(rdr):
        # Assume timeslots occur in order
        if len(games[game]) <= index:
            print('Error: game {} shorter than index {}'.format(game, index))
        else:
            info = games[game][index + 1]
            ts = int(row['slot'])
            info['threshold'] = floatMaybe(row['threshold'])
            info['excess'] = floatMaybe(row['excess'])
            info['cost'] = floatMaybe(row['cost'])


def floatMaybe (str):
    result = 0.0
    if str != '' :
        result = float(str)
    return result


def maxGames (source, assessment, count):
    gameIds = []
    data = []
    for g, d in games.items():
        if len(d) <= (assessment) or source not in d[assessment]:
            #print('Game {} source {} data {}'.format(g, source, d))
            continue
        gameIds.append(g)
        data.append(d[assessment][source])
    l, g = zip(*sorted(zip(data, gameIds)))
    result = []
    limit = count
    for game, value in zip(reversed(g), reversed(l)):
        result.append([game, value])
        limit -= 1
        if limit <= 0:
            break
    return result


#def shortestGames (n):
    

def plotAggregate (source, title, yAxisLabel = 'kWh'):
    '''
    Draws violin plots of the distribution of source at each assessment
    across games.
    '''
    index = 0
    data = []
    more = True
    while more:
        more = False
        for g, d in games.items():
            if len(d) >= index + 1:
                if source in d[index]:
                    if len(data) < index + 1:
                        more = True
                        data.append([])
                    data[index].append(d[index][source])
        index += 1
    
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.violinplot(data, showmeans=False, showmedians=True)
    ax.set_title(title)
    ax.yaxis.grid(True)
    ax.set_xlabel('assessment')
    ax.set_ylabel(yAxisLabel)
    fig.show()


def plotAggregateComplete (tournamentCsvUrl, tournamentDir, source, title):
    '''
    Extracts data from a tournament and runs plotAggregate on the result
    '''
    processFiles(tournamentCsvUrl, tournamentDir)
    plotAggregate(source, title)


def plotTrend (source, title, ev = 'ratio'):
    '''
    Draws violin plots of the distribution of inter-assessment ratios
    across games. To get inter-assessment differences, use ev = 'difference'
    '''
    if not ev in ['ratio', 'difference']:
        print("Value of ev is {}, must be 'ratio' or 'difference'".format(ev))
        return
    last = 0.0
    epsilon = .00001
    label = source + 'Trend'
    for g, v in games.items():
        for index in range(len(v)):
            #print('g={}, index={}, last={}'.format(g, index, last))
            if index == 0:
                last = v[0][source]
                if ev == 'ratio':
                    v[0][label] = 1.0
                else:
                    v[0][label] = 0.0
            elif source in v[index]:
                if ev == 'ratio':
                    if last == 0.0:
                        if index == 1:
                            last = v[index][source]
                        else:
                            print('Game {} index {}, last = 0.0'.format(g, index))
                            last = epsilon
                    v[index][label] = v[index][source] / last
                else:
                    v[index][label] = v[index][source] - last
                if v[index][source] != 0.0:
                    last = v[index][source]
            else:
                # fill in blanks
                if ev == 'ratio':
                    v[index][label] = 1.0
                else:
                    v[index] = v[index - 1]
    plotAggregate(label, title, yAxisLabel = ev)


def plotTrendComplete (tournamentCsvUrl, tournamentDir, source, title):
    processFiles(tournamentCsvUrl, tournamentDir)
    plotTrend(source, title)


def processFiles (tournamentCsvUrl, tournamentDir):
    '''
    Iterates through trace and state files, gathering data for plotting,
    just in case this has not already been done. It can take quite a while.
    '''
    global games
    gamesPath = Path(tournamentDir, 'data', 'capacity-data-games.json')
    if not gamesPath.exists():
        traceFileIter(tournamentCsvUrl, tournamentDir)
        dataFileIter(tournamentCsvUrl, tournamentDir)
        output = gamesPath.open('w')
        json.dump(games, output)
        output.close()
    else:
        input = gamesPath.open('r')
        games = json.load(input)
        input.close()


#processFiles('file:./finals-2018/finals_2018_07.games_.csv', 'finals-2018')
processFiles('file:./finals-2017/finals_2017_06.games.csv', 'finals-2017')
