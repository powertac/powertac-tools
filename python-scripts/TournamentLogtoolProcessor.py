#!/usr/bin/python3
'''
Applies a specific logtool analyzer to all the sim logs in a tournament,
using TournamentGameIterator.csvIterator to pull in the game logs.
'''

import TournamentGameIterator as ti
import string, re, os, subprocess, sys
from pathlib import Path
processEnv = {'JAVA_HOME': os.environ.get('JAVA_HOME'),
              'Path' : os.environ.get('PATH') }

def extractData (statefileName, gameId, extractorClass,
                 dataPrefix, extractorOptions,
                 logtoolDir, dataDir='data', force=False):
    '''
    Extracts data from individual game state log, using the specified
    logtool extractor class with the specified options, and leaving the
    result in dataDir/dataPrefix-gameId.csv relative to the logtool used.
    Requires working Java 8 and maven installations.
    '''
    #print("Processing ", statefileName)
    datafileName = dataPrefix + gameId + '.csv'
    dataPath = Path(logtoolDir, dataDir, datafileName)
    if force or not dataPath.exists():
        args = ''.join([extractorClass, ' ',
                        extractorOptions, ' ',
                        statefileName, ' ',
                        dataDir + "/" + datafileName])
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
            
    return {'gameId': gameId, 'path': str(dataPath)}

def dataFileIter (tournamentCsvUrl, tournamentDir, extractorClass, dataPrefix,
                  extractorOptions='', logtype='sim',
                  logtoolDir = "../logtool-examples/",
                  force=False):
    '''
    Iterates through sim logs found in tournamentDir, running the specified
    data extractor. If force is False (the default), then the extractor will be
    run only on games for which no data output file already exists.
    '''
    return (extractData(log[logtype], log['gameId'],
                        extractorClass, dataPrefix,
                        extractorOptions, logtoolDir, force=force)
            for log in ti.csvIter(tournamentCsvUrl, tournamentDir))

def iterate (url, tournamentDir, extractorClass, dataPrefix, options):
    for data in dataFileIter(url, tournamentDir,
                             extractorClass, dataPrefix,
                             options):
        print(data)
    

def main ():
    '''
    Command-line invocation
    '''
    if len(sys.argv) < 5:
        print('Usage: TournamentLogtoolProcessor url tournamentDir extractorClass dataPrefix options...')
    else:
        options = ''
        if len(sys.argv) > 5:
            for index in range(5, len(sys.argv)):
                options = options + ' ' + sys.argv[index]
            #print('options', options)

        iterate(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], options)

if __name__ == "__main__":
    main()
