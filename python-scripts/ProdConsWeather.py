#!/usr/bin/python
'''
Extracts production and consumption data, along with hour-of-day, day-of-week,
and weather conditions from the games in a directory.
Assumes the games are in their packed, compressed state, so must unpack
each game before processing it.
'''
# Uses Python 3.4 or later

import string,re,os,sys,subprocess
from pathlib import Path
import TournamentGameIterator as tg

logtoolDir = "../logtool-examples"
processEnv = {'JAVA_HOME': '/usr/lib/jvm/java-8-oracle'}
outdir = "data"

##def stateLogIter (tournamentURL, tournamentDir):
##    '''
##    Returns a generator of state log paths. Logs are downloaded, if needed,
##    from tournamentURL into tournamentDir, which becomes a directory of
##    compressed game logs. The tournamentDir directory must already exist.
##    '''
##    path = Path(tournamentDir)
##    return (extractStateLog(name) for name in path.glob('game-*.tar.gz'))

##def extractStateLog (gzLog):
##    '''
##    Extracts logs from compressed game log file, if not already extracted.
##    Returns path to state log.
##    '''
##    path = Path(gzLog)
##    # new naming convention
##    m = re.search('\S*/?game-(\d+)-sim.tar.gz', str(path))
##    if not m:
##        # old naming convention
##        m = re.search('\S*/?game-(\d+)-sim-logs.tar.gz', str(path))
##        if not m:
##            gameId = 'xx'
##            print('Failed to find game ID in ' + str(path))
##            return false
##
##    gameId = m.group(1)
##    #needed for pre-2016 games
##    logdir = "log"
##    logDirectory = Path(path.parent, logdir)
##    if not logDirectory.exists():
##        logdir = gameId
##    logPath = Path(path.parent, logdir, 'powertac-sim-' + gameId + '.state')
##    if not logPath.exists():
##        p1 = subprocess.Popen(['tar', 'xzf', path.name], cwd = str(path.parent))
##        p1.wait()
##    return logPath
    

def extractData (gameNum, statefilePath):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
    args = ''.join(['org.powertac.logtool.example.ProductionConsumptionWeather ',
                    statefilePath,
                    ' ',
                    os.path.join(outdir,
                                 gameNum + '-prod-cons-weather.data')])
    #print(args)
    result = subprocess.run(['mvn', 'exec:exec', '-Dexec.args=' + args],
                            env = processEnv,
                            cwd = logtoolDir,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #print(result)

def processTournament (tournamentURL, tournamentDir):
    '''
    Iterates through game logs found in tournamentDir, extracting production
    and consumption data
    '''
    for gameNum in tg.logIter(tournamentURL, tournamentDir):
        stateDir = Path(tournamentDir).joinpath(gameNum)
        statelog = False
        for file in stateDir.glob('powertac-sim*.state'):
            statelog = file
        if not statelog:
            print('Failed to find state log in {}', stateDir)
            return
        print(statelog)
        extractData(gameNum, str(statelog))
