#!/usr/bin/python3
'''
Populates the game, broker, and broker_game tables in the database from
the logs of a tournament.
'''

import TournamentIterator as ti
import string, re, os, subprocess
from pathlib import Path

logtoolDir = "../logtool-examples"
processEnv = {'JAVA_HOME': '/usr/lib/jvm/java-7-oracle'}

stateIdRe = re.compile('powertac-sim-(\d+).state')
def extractData (statefileName, extractorClass, dataPrefix, options):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
    print(statefileName)
    m = stateIdRe.search(statefileName)
    if not m:
        print('Failed to find game ID in ' + statefileName)
        return
    gameId = m.group(1)
    datafileName = dataPrefix + gameId + '.data'
    dataPath = Path(logtoolDir, datafileName)
    if not options == '' and not options.endswith(' '):
        options.append(' ')
    if not dataPath.exists():
        args = ''.join([extractorClass, ' ',
                        options,
                        statefileName,
                        ' ',
                        datafileName])
        #print(os.getcwd())
        #print(args)
        subprocess.check_output(['mvn', 'exec:exec',
                                 '-Dexec.args=' + args],
                                env = processEnv,
                                cwd = logtoolDir)
    return str(dataPath)

def datafileIter (tournamentDir, extractorClass, dataPrefix, options=''):
    '''
    Iterates through game logs found in tournamentDir, extracting production
    and consumption data
    '''
    return (extractData(str(statelog), extractorClass, dataPrefix, options)
            for statelog in ti.stateLogIter(tournamentDir))
