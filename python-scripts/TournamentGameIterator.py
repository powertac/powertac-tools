#!/usr/bin/python3
'''
Extracts sim and boot records from compressed game files downloaded from
a tournament archive. Call logIter(url, dir) where url is the archive
descriptor (a csv file) and
dir is the directory that is to contain the (compressed) game logs.
This returns an iterator over directory names within the target directory.
Initially this directory must exist.
'''
# Uses Python 3.4 or later

import re, os, tarfile, subprocess
import urllib.request
import requests, csv
import io
from pathlib import Path

def logIter (tournamentURL, tournamentDir):
    '''
    Returns a generator of log dirs extracted from a directory
    of compressed game logs, downloading them first if necessary.
    Each dir will contain the state log, trace log, and boot.xml file
    '''
    games = open(os.path.join(tournamentDir, "games.txt"), 'r')
    return (extractLog(tournamentURL, game.rstrip(), tournamentDir)
            for game in games)

def csvIter (tournamentCsvUrl, dirPath):
    '''
    Reads the tournament summary spreadsheet, extracts game IDs and URLs,
    downloads and unpacks the game logs if necessary.
    Each iteration returns the dict produced by extractLogs()
    for the given game.
    '''
    content = urllib.request.urlopen(tournamentCsvUrl)
    gameList = io.StringIO(content.read().decode('utf-8'))    
    csvReader = csv.DictReader(gameList, delimiter=',')
    return(extractLogs(row['logUrl'], row['gameId'],
                       re.search('/([^/]+)$', row['logUrl']).group(1),
                       dirPath)
           for row in csvReader)


def extractLogs (url, game, tarname, dirPath):
    '''
    Extracts logs from compressed game log file, if not already extracted.
    Returns the name of a directory inside tournamentDir that contains two
    subdirectories, 'log' and 'boot-log'. Inside each of those directories are
    the state and trace logs. Returns the paths to the boot and sim logs as
    a dict of the form {'gameId': id, 'boot':bootlog, 'sim':simlog}.
    '''
    # make sure we have the bundle locally
    currentDir = os.getcwd()
    os.chdir(dirPath)

    # extract the bundle into the directory if needed
    gameDirPath = game
    if not os.path.isdir(gameDirPath):
        os.mkdir(gameDirPath)
        os.chdir(gameDirPath)
        g = urllib.request.urlopen(url)
        with open(tarname, 'wb') as f:
            f.write(g.read())
        
        tar = tarfile.open(tarname)
        tar.extractall()
        tar.close
    os.chdir(currentDir)

    # find the actual paths to the boot and sim logs
    bootDir = Path(dirPath, gameDirPath, 'boot-log')
    bootfile = ''
    simDir = Path(dirPath, gameDirPath, 'log')
    simfile = ''
    for file in bootDir.glob('*.state'):
        if (not str(file).endswith('init.state')):
            bootfile = file
    for file in simDir.glob('*.state'):
        if (not str(file).endswith('init.state')):
            simfile = file
    
    return {'gameId': game, 'boot': str(bootfile), 'sim':str(simfile)}
