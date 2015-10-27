#!/usr/bin/python
'''
Extracts production and consumption data from the games in a directory.
Assumes the games are in their packed, compressed state, so must unpack
each game before processing it.
'''
# Uses Python 3.4 or later

import string,re,os,sys,subprocess
from pathlib import Path

gameDir = "../../games"
logtoolDir = "../logtool-examples"
processEnv = {'JAVA_HOME': '/usr/lib/jvm/java-7-oracle'}
outdir = "data"

def stateLogIter (tournamentDir):
    '''
    Returns a generator of state logs extracted from a directory
    of compressed game logs
    '''
    path = Path(tournamentDir)
    return (extractStateLog(name) for name in path.glob('game-*.tar.gz'))

gameIdRe = re.compile('game-(\d+)-sim-logs.tar.gz')
def extractStateLog (gameLog):
    '''
    Extracts logs from compressed game log file, if not already extracted.
    Returns path to state log.
    '''
    path = Path(gameLog)
    m = gameIdRe.search(str(path))
    if m:
        gameId = m.group(1)
        logPath = Path(path.parent, 'log', 'powertac-sim-' + gameId + '.state')
        if not logPath.exists():
            p1 = subprocess.Popen(['tar', 'xzf', path.name], cwd = str(path.parent))
            p1.wait()
        return logPath
    else:
        gameId = 'xx'
        print('Failed to find game ID in ' + str(path))
        return false

stateIdRe = re.compile('powertac-sim-(\d+).state')
def extractData (statefileName):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
    #currentDir = os.getcwd()
    #os.chdir(logtoolDir)
    m = stateIdRe.search(statefileName)
    if not m:
        print('Failed to find game ID in ' + statefileName)
        return
    gameId = m.group(1)
    args = ''.join(['org.powertac.logtool.example.ProductionConsumption ',
                    statefileName,
                    ' ',
                    os.path.join(outdir,
                                 gameId + '-prod-cons.data')])
    # print(args)
    subprocess.check_output(['mvn', 'exec:exec',
                             '-Dexec.args=' + args],
                            env = processEnv,
                            cwd = logtoolDir)
    #print('status(', gameId, ') =', p1.wait())

def processTournament (tournamentDir):
    '''
    Iterates through game logs found in tournamentDir, extracting production
    and consumption data
    '''
    for statelog in stateLogIter(tournamentDir):
        print(statelog)
        extractData(str(statelog))
