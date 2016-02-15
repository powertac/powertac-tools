#!/usr/bin/python
'''
Extracts balancing market data from trace logs. Output for a game consists
of one line/timeslot formatted as

  gameId,timeslot,pPlus,pMinus,totalImbalance

Can be used in two modes. If the argument is a directory, it is taken as
a collection of compressed game logs, from which trace logs will be extracted
and analyzed. Otherwise, the argument is assumed to be a single trace log file.
The output data will be stored in a directory ./data, each file named
pplus-pminus-gameId.csv
'''

import string,re,sys,os
import TournamentIterator as ti
import pathlib

# regular expressions to pick off timeslot, pplus/pminus, imbalance
gameRe = re.compile('powertac-sim-(\d+).trace')
tsRe = re.compile('Deactivated timeslot (\d+),')
ppRe = re.compile('balancing prices: pPlus=(-?\d+\.\d+), pMinus=(-?\d+\.\d+)')
imRe = re.compile('totalImbalance=(-?\d+\.\d+)')

def extractData (source):
    '''
    Extracts data from a single trace log, or from a directory of compressed
    game logs using TournamentIterator
    '''
    if os.path.isdir(source):
        # in this case, we use the iterator
        for trace in ti.traceLogIter(source):
            traceName = str(trace)
            dataName = 'data/pp-data.csv'
            m = gameRe.search(traceName)
            if m:
                dataName = 'data/pp-data-{}.csv'.format(m.group(1))
            else:
                print('Could not extract game ID from {}'.format(traceName))
            extractFile(traceName, dataName)
    else:
        extractFile(source, source + '-pp.csv')

def extractFile (traceIn, dataOut):
    '''
    Reads a trace log from traceIn, writes results to dataOut.
    '''
        
    infile = open(traceIn, 'r')
    outfile = open(dataOut, 'w')

    # extract game ID
    m = gameRe.search(traceIn)
    gameId = 'unknown'
    if m:
        gameId = 'game-{}'.format(m.group(1))
    state = 'ts'
    pplus = 0.0
    pminus = 0.0
    imbalance = 0.0
    timeslot = 0
    for line in infile.readlines():
        # looking for timeslot tag
        m = tsRe.search(line)
        if m:
            if state != 'ts':
                print('New timeslot {} in bad state {}, {}'.format(m.group(1), state, traceIn))
            timeslot = int(m.group(1))
            state = 'bal'
            continue
        m = ppRe.search(line)
        if m:
            if state != 'bal':
                print('Balancing prices in bad state', state)
            pplus = floatMaybe(m.group(1))
            pminus = floatMaybe(m.group(2))
            state = 'ti'
            continue
        m = imRe.search(line)
        if m:
            if state != 'ti':
                print('Total imbalance in bad state', state)
            imbalance = floatMaybe(m.group(1))
            state = 'ts'
            outfile.write('{},{},{:.4f},{:.4f},{:.4f}\n'.format(gameId, timeslot, pplus, pminus, imbalance))
    outfile.close()
    infile.close()

def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str != '' :
        result = float(str)
    return result

def main ():
    extractData(sys.argv[1])

#if __name__ == '__main__':
#    main()
