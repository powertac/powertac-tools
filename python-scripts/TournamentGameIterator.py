#!/usr/bin/python3
'''
Extracts sim and boot records from compressed game files downloaded from
a tournament archive. Call logIter(url, dir) where url is the archive and
dir is the directory that is to contain the (compressed) game logs.
This returns an iterator over directory names within the target directory.
Initially this directory must exist, and must contain a txt file containing
the game numbers for all the games to be downloaded. As an enhancement, the
list of games could be obtained from the url as the first column of the
tournament spreadsheet.
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

def csvIter (tournamentCsvUrl):
    '''
    Reads the tournament summary spreadsheet, extracts game URLs, downloads
    the games if necessary.
    '''
    content = urllib.request.urlopen(tournamentCsvUrl)
    gameList = io.StringIO(content.read().decode('utf-8'))    
    csvReader = csv.DictReader(gameList, delimiter=';')
    #print(csvReader.fieldnames)
    for row in csvReader:
        print(row['gameId'], row['logUrl'])


def extractLog (url, game, dirPath):
    '''
    Extracts logs from compressed game log file, if not already extracted.
    Returns the name of a directory inside tournamentDir that contains
    state and trace logs.
    '''
    # make sure we have the bundle locally
    currentDir = os.getcwd()
    os.chdir(dirPath)
    gameBundlePath = 'game-'+game+'-sim.tar.gz'
    if not os.path.exists(gameBundlePath):
        #print('retrieve '+url+'/game-'+game+'-sim.tar.gz')
        g = urllib.request.urlopen(url+'/game-'+game+'-sim.tar.gz')
        with open(gameBundlePath, 'wb') as f:
            f.write(g.read())

    # extract the bundle into the directory if needed
    gameDirPath = game
    if not os.path.isdir(gameDirPath):
        tar = tarfile.open(gameBundlePath)
        tar.extractall()
        tar.close
        os.rename('log', game)
        bootPath = game+'/game-'+game+'-boot.xml'
        g = urllib.request.urlopen(url+'/game-'+game+'-boot.xml')
        with open(bootPath, 'wb') as f:
            f.write(g.read())
    os.chdir(currentDir)
    return game
