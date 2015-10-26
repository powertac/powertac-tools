#!/usr/bin/python3
'''
Extracts state logs from compressed game files in a directory that presumably
contains the game logs from a tournament. Call stateLogIter(dir) where
dir is the directory containing the game logs. This returns an iterator over
paths to state logs extracted from the tournament.
'''
# Uses Python 3.4 or later

import re
import subprocess
from pathlib import Path

def stateLogIter (tournamentDir):
    '''
    Returns a generator of state logs extracted from a directory
    of compressed game logs
    '''
    path = Path(tournamentDir)
    return (extractStateLog(name) for name in path.glob('game-*-sim-logs.tar.gz'))

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
        return False
