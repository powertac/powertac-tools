#!/usr/bin/python3
'''
Populates the game, broker, and broker_game tables in the database from
the logs of a tournament.
'''

import TournamentIterator as ti
import string, re, os, subprocess
from pathlib import Path

if os.name == 'nt':
    #logtoolDir = r"C:/Users/Mohammad/Documents/Google Drive/PhD/Spyder workspace/production-consumption/powertac-tools/logtool-examples"
    processEnv = {'JAVA_HOME': 'C:/Program Files/Java/jdk1.8.0_66/jre/',
              'Path' : 'C:/Program Files/apache-maven-3.3.3/bin/'}
elif os.name == 'posix':
    #logtoolDir = "../logtool-examples"
    processEnv = {'JAVA_HOME': '/usr/lib/jvm/java-7-oracle'}


def extractData (statefileName, extractorClass, 
                 dataPrefix, options, logtype, force, logtoolDir):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
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
        if os.name == 'nt':
            subprocess.check_output(['mvn', 'exec:exec',
                                 '-Dexec.args =' + args],
                                 shell = True,
                                env = processEnv,
                                cwd = logtoolDir)
        elif os.name == 'posix':
            subprocess.check_output(['mvn', 'exec:exec',
                                 '-Dexec.args =' + args],
                                env = processEnv,
                                cwd = logtoolDir)
            
    return [gameId, str(dataPath)]

def datafileIter (tournamentDir, extractorClass, dataPrefix,
                  extractorOptions='', logtype='sim', force=False, logtoolDir = "../logtool-examples/"):
    '''
    Iterates through game logs found in tournamentDir, extracting production
    and consumption data
    '''
    return (extractData(str(statelog), extractorClass, dataPrefix,
                        extractorOptions, logtype, force, logtoolDir)
            for statelog in ti.stateLogIter(tournamentDir, logtype=logtype))