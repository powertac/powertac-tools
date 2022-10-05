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

import re, os, shutil, tarfile, subprocess
import urllib
import requests, csv
import io
from pathlib import Path

def logIter (tournamentURL, tournamentDir):
    '''
    **Deprecated**
    Returns a generator of log dirs extracted from a directory
    of compressed game logs, downloading them first if necessary.
    Each dir will contain the state log, trace log, and boot.xml file.
    To use this, you need a file games.txt in the data dir that contains
    the filenames of the compressed log files.
    '''
    games = open(os.path.join(tournamentDir, "games.txt"), 'r')
    return (extractLog(tournamentURL, game.rstrip(), tournamentDir)
            for game in games)

def csvIter (tournamentCsvUrl, dirPath, target, em = False):
    '''
    Reads the tournament summary spreadsheet, extracts game IDs and URLs,
    downloads and unpacks the game logs if necessary.
    Each iteration returns the dict produced by extractLogs()
    for the given game. If em == True, then we assume the directory structure
    produced by the Experiment Manager
    '''
    content = urllib.request.urlopen(tournamentCsvUrl)
    gameList = io.StringIO(content.read().decode('utf-8-sig'))
    #csvReader = csv.DictReader(gameList, delimiter=';')
    csvReader = csv.DictReader(gameList)
    print('Fields', csvReader.fieldnames)
    return(extractLogs(row['logUrl'], row['gameId'],
                       #re.search('/([^/]+)$', row['logUrl']).group(1),
                       row['gameId'] + ".tar.gz",
                       dirPath, target, em)
           for row in csvReader)


def extractLogs (url, game, tarname, dirPath, target, em):
    '''
    Extracts logs from compressed game log file, if not already extracted.
    Returns the name of a directory inside tournamentDir that contains two
    subdirectories, 'log' and 'boot-log'. Inside each of those directories are
    the state and trace logs. Returns the paths to the boot and sim logs as
    a dict of the form {'gameId': id, 'boot':bootlog, 'sim':simlog}.
    '''
    print('tgi.extractLogs, game url =', url, 'target =', target)
    # don't do anything if the target path exists
    targetPath = target + game + '.csv'
    print('target =', targetPath)
    if os.path.exists(targetPath):
        print('target', targetPath, 'exists')
        return {'gameId': game, 'boot': '', 'sim': '', 'path': targetPath}
    # make sure we have the bundle locally
    currentDir = os.getcwd()
    os.chdir(dirPath)
    # handle tournament and EM bundles differently
    if em:
        return extractEMBundle(url, game, tarname, dirPath, target, currentDir)
    else:
        return extractTSBundle(url, game, tarname, dirPath, target, currentDir)

def extractEMBundle(url, game, tarname, dirPath, target, originalDir):
    ''' EM bundles contain only sim logs, no boot artifacts '''
    # extract the bundle into the directory if needed
    gameDirPath = game
    if not os.path.isdir(gameDirPath):
        os.mkdir(gameDirPath)
    # check for existing download
    if not os.path.exists(tarname):
        print('extractLogs opening', tarname)
        g = urllib.request.urlopen(url)
        with open(tarname, 'wb') as f:
            f.write(g.read())
    else:
        print("download", tarname, "exists")
    if not os.path.exists(gameDirPath + "./log"):
        tar = tarfile.open(tarname)
        #if (tar.getmember(game)):
        #    os.chdir("..")
        tar.extractall()
        tar.close
    else:
        print("logs for game", game, "exist")
    os.chdir(originalDir)

    # find the actual paths to the sim logs and boot record
    simDir = Path(dirPath, gameDirPath, 'log')
    simfile = ''
    for file in simDir.glob('*.state'):
        if (not str(file).endswith('init.state')):
            simfile = file
    
    return {'gameId': game,
            'sim': str(simfile)}

def extractTSBundle(url, game, tarname, dirPath, target, originalDir):
    # extract the bundle into the directory if needed
    gameDirPath = game
    if not os.path.isdir(gameDirPath):
        os.mkdir(gameDirPath)
    # check for existing download
    if not os.path.exists(tarname):
        print('extractLogs opening', tarname)
        g = urllib.request.urlopen(url)
        with open(tarname, 'wb') as f:
            f.write(g.read())
    else:
        print("download", tarname, "exists")
    if not os.path.exists("./log"):
        tar = tarfile.open(tarname)
        if (tar.getmember(game)):
            os.chdir("..")
        tar.extractall()
        tar.close
    else:
        print("logs for game", game, "exist")

    os.chdir(originalDir)

    # find the actual paths to the boot and sim logs and xml boot record
    bootxml = ''
    gameDir = Path(dirPath, gameDirPath)
    for file in gameDir.glob('*.xml'):
        bootxml = file
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
    
    return {'gameId': game,
            'boot': str(bootfile),
            'sim': str(simfile),
            'bootRecord': str(bootxml)}

def csvClearDecompressed (tournamentCsvUrl, dirPath):
    '''
    Clears out the docompressed files from a tournament set to conserve
    disk space'''
    content = urllib.request.urlopen(tournamentCsvUrl)
    gameList = io.StringIO(content.read().decode('utf-8'))
    csvReader = csv.DictReader(gameList, delimiter=';')
    for row in csvReader:
        cleanLogs(row['gameId'],
                  re.search('/([^/]+)$', row['logUrl']).group(1),
                  dirPath)

def cleanLogs (game, tarname, dirPath):
    '''
    clears out boot and sim logs for a single game'''
    # switch to tournament dir
    currentDir = os.getcwd()
    os.chdir(dirPath)

    # Nothing to do if the game dir does not exist
    gameDirPath = game
    if os.path.isdir(gameDirPath):
        shutil.rmtree(Path(gameDirPath, 'log'), ignore_errors = True)
        shutil.rmtree(Path(gameDirPath, 'boot-log'), ignore_errors = True)
    os.chdir(currentDir)
