#!/usr/bin/python3
'''
Populates the game, broker, and broker_game tables in the database from
the logs of a tournament.
'''

import TournamentIterator as ti
import string, re, subprocess, os
import mysql.connector
from pathlib import Path

# configuration
fifo = 'pt_fifo'
logtoolDir = "../logtool-examples"
processEnv = {'JAVA_HOME': '/usr/lib/jvm/java-7-oracle'}
dbConfig = {'user': 'jcollins',
            'password': 'mysql-JEC',
            'host': '127.0.0.1',
            'database': 'powertac_analysis'}

# queries
addGame = ('INSERT INTO game (idgame, size, length) '\
           'VALUES ("{0}", {1}, {2})')
findBroker = ('SELECT idbroker, name FROM broker '\
              'WHERE (name = "{0}")')
addBroker = ('INSERT INTO broker (name) VALUES ("{0}")')
addBrokerGame = ('INSERT INTO broker_game '\
                 '(broker_idbroker, game_idgame, game_idbroker) '\
                 'VALUES ({0}, {1}, {2})')

stateIdRe = re.compile('powertac-sim-(\d+).state')
def extractData (statefileName):
    '''
    Extracts data from individual game state log, leaving
    result in data/gameid-pc.data
    '''
    # set up the pipe
    if os.path.exists(fifo):
       os.remove(fifo)
    os.mkfifo(fifo)

    # invoke mvn in subprocess
    m = stateIdRe.search(statefileName)
    if not m:
        print('Failed to find game ID in ' + statefileName)
        return
    gameId = m.group(1)
    args = ''.join(['org.powertac.logtool.example.GameBrokerInfo ',
                    statefileName, ' ',
                    os.getcwd() + "/" + fifo])
    #print(args)
    mvn = subprocess.Popen(['mvn', 'exec:exec',
                             '-Dexec.args=' + args],
                            env = processEnv,
                            cwd = logtoolDir)

    # read from pipe, populate database
    stream = open(fifo, 'r')
    try:
        cnx = mysql.connector.connect(**dbConfig)
        cursor = cnx.cursor()
        idgame = -1
        for line in stream.readlines():
            tokens = line.rstrip().split(', ')
            #print(tokens)
            if tokens[0] == 'competition':
                idgame = tokens[1]
                cursor.execute(addGame.format(tokens[1], tokens[3], tokens[2]))
            elif tokens[0] == 'broker':
                cursor.execute(findBroker.format(tokens[1]))
                row = cursor.fetchone()
                id = 0
                if row is None:
                    cursor.execute(addBroker.format(tokens[1]))
                    id = cursor.lastrowid
                else:
                    (id, brokername) = row
                cursor.execute(addBrokerGame.format(id, idgame, tokens[2]))
        cnx.commit()
                    
    except mysql.connector.Error as err:
        print(err)

    # wait for subprocess to exit
    print('status(', gameId, ') =', mvn.wait())
    cnx.close()
    stream.close()

def processTournament (tournamentDir):
    '''
    Iterates through game logs found in tournamentDir, extracting data
    from each.
    '''
    for statelog in ti.stateLogIter(tournamentDir):
        print(statelog)
        extractData(str(statelog))
