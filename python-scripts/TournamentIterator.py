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

def stateLogIter (tournamentDir, logtype='sim'):
    '''
    Returns a generator of state logs extracted from a directory
    of compressed game logs
    '''
    path = Path(tournamentDir)
    return (extractStateLog(name, logtype)
            for name in path.glob('game-*-{}-logs.tar.gz'.format(logtype)))

def extractStateLog (gameLog, logtype):
    '''
    Extracts logs from compressed game log file, if not already extracted.
    Returns path to state log.
    '''
    gameIdRe = re.compile('game-(\d+)-{}-logs.tar.gz'.format(logtype))
    path = Path(gameLog)
    m = gameIdRe.search(str(path))
    if m:
        gameId = m.group(1)
        logPath = Path(path.parent, 'log',
                       'powertac-{}-{}.state'.format(logtype, gameId))
        if not logPath.exists():
            p1 = subprocess.Popen(['tar', 'xzf', path.name], cwd = str(path.parent))
            p1.wait()
        return logPath
    else:
        gameId = 'xx'
        print('Failed to find game ID in ' + str(path))
        return False
