#!/usr/bin/python
'''
Extracts balancing market data from trace logs. Output for a game consists
of one line/timeslot formatted as

  gameId,timeslot,pPlus,pMinus,totalImbalance

Usage: PplusPminus.py tracelog datafile
'''

import string,re,sys

# regular expressions to pick off timeslot, pplus/pminus, imbalance
gameRe = re.compile('powertac-sim-(\d+).trace')
tsRe = re.compile('Deactivated timeslot (\d+),')
ppRe = re.compile('balancing prices: pPlus=(-?\d+\.\d+), pMinus=(-?\d+\.\d+)')
imRe = re.compile('totalImbalance=(-?\d+\.\d+)')

def extractData (traceIn, dataOut):
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
                print('New timeslot in bad state', state)
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
    extractData(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
