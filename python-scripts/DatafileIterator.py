#!/usr/bin/python3
'''
Populates the game, broker, and broker_game tables in the database from
the logs of a tournament.
'''

import TournamentIterator as ti
import string, re, os, subprocess
from pathlib import Path
year = 2016
processEnv = {'JAVA_HOME': os.environ.get('JAVA_HOME'),
              'Path' : os.environ.get('PATH') }

def extractData (statefileName, extractorClass, 
                 dataPrefix, options, logtype, force, logtoolDir):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
    if year == 2016:
        stateIdRe = re.compile('powertac-{}-2016_finals_(\d+).state'.format(logtype))
    elif year != 2016:
        stateIdRe = re.compile('powertac-{}-(\d+).state'.format(logtype))
    print("Processing ", statefileName)
    m = stateIdRe.search(statefileName)
    if not m:
        print('Failed to find game ID in ' + statefileName)
        return
    gameId = m.group(1)
    datafileName = dataPrefix + gameId + '.data'
    dataPath = Path(logtoolDir, datafileName)
    if not options == '' and not options.endswith(' '):
        options += ' '
    if force or not dataPath.exists():
        args = ''.join([extractorClass, ' ',
                        options,
                        statefileName,
                        ' ',
                        datafileName])
        args = args.replace("\\","/")
        #print(args)
        if os.name == 'nt':
            print(subprocess.check_output(['mvn', 'exec:exec',
                                 '-Dexec.args =' + args],
                                 shell = True,
                                env = processEnv,
                                cwd = logtoolDir))
        elif os.name == 'posix':
            subprocess.check_output(['mvn', 'exec:exec',
                                 '-Dexec.args =' + args],
                                env = processEnv,
                                cwd = logtoolDir)
            
    return [gameId, str(dataPath)]

def datafileIter (tournamentDir, extractorClass, dataPrefix,
                  extractorOptions='', logtype='sim',
                  force=False, logtoolDir = "../logtool-examples/"):
    '''
    Iterates through game logs found in tournamentDir, extracting production
    and consumption data
    '''
    return (extractData(str(statelog), extractorClass, dataPrefix,
                        extractorOptions, logtype, force, logtoolDir)
            for statelog in ti.stateLogIter(tournamentDir, sessionType=logtype))
