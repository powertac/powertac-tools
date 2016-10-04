'''
Holder for game data, including sim and boot data. Aggregates data by
game, by week, by day, by weekday, by weekend-day. Allows retrieval by
name.
'''

from pathlib import Path
import DatafileIterator as di

class GameData:
    '''
    dataType is one of 'net-demand', 'consumption', 'solar'
    logType is one of 'sim', 'boot'
    '''

    def __init__ (self, tournament='../../games/finals-201504',
                  dataType='net-demand'):
        self.tournament = tournament
        self.reset(dataType)

    def reset (self, dataType):
        # data store
        self.dataType = dataType
        self.gameData = []
        self.bootData = []
        self.gameDict = {}
        self.bootDict = {}
        self.weekData = [[] for x in range(168)]
        self.weekdayData = [[] for x in range(24)]
        self.weekendData = [[] for x in range(24)]
        self.dayData = [[] for x in range(24)]
        self.dataMap = {'Weekly':self.weekData, 'Weekday':self.weekdayData,
                        'Weekend':self.weekendData, 'Daily':self.dayData,
                        'Game':self.gameData}

    # data-type parameters
    logtoolClass = {'net-demand':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'consumption':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'production':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'solar':
                    'org.powertac.logtool.example.SolarProduction'}
    dataPrefix = {'net-demand': 'data/prod-cons-',
                  'consumption': 'data/prod-cons-',
                  'production': 'data/prod-cons-',
                  'solar': 'data/solar-prod-'}

    def collectData (self, logType='sim', force=False):
        '''
        Processes data from sim data files in the specified directory.
        Use force=True to force re-analysis of the data. Otherwise the logtool
        code won't be run if its output is already in place. '''
        actualPrefix = self.dataPrefix[self.dataType]
        if actualPrefix == '':
            print('Bad dataType: {}'.format(self.dataType))
            return
        if logType != 'sim':
            actualPrefix = '{}{}-'.format(self.dataPrefix[self.dataType],
                                           logType)
        for [gameId, dataFile] in di.datafileIter(self.tournament,
                                                  self.logtoolClass[self.dataType],
                                                  actualPrefix,
                                                  logtype=logType,
                                                  force = force):
            # note that dataFile is a Path, not a string
            if logType == 'sim':
                self.processFile(gameId, str(dataFile))
            else:
                self.processBootFile(gameId, str(dataFile))


    def processFile (self, gameId, dataFile):
        '''
        Collects data from a data file into a global 168-column array,
        one column per hour-of-week.
        Note that the day-of-week numbers are in the range [1-7]. '''
        data = open(dataFile, 'r')
        gameSeries = []
        self.gameData.append(gameSeries)
        self.gameDict[gameId] = gameSeries
        junk = data.readline() # skip first line
        for line in data.readlines():
            row = line.split(', ')
            dow = int(row[1])
            hod = int(row[2])
            how = (dow - 1) * 24 + hod # hour-of-week
            prod = self.floatMaybe(row[3])
            cons = self.floatMaybe(row[4])
            net = self.extractNet(prod, cons)
            gameSeries.append([how, net])
            self.weekData[how].append(net)
            self.dayData[hod].append(net)
            if dow <= 5:
                #weekday
                self.weekdayData[hod].append(net)
            else:
                #weekend
                self.weekendData[hod].append(net)

    def processBootFile (self, gameId, dataFile):
        '''
        Collects data from a bootstrap data file into a global 168-column array,
        one column per hour-of-week.
        Note that the day-of-week numbers are in the range [1-7]. '''
        data = open(dataFile, 'r')
        gameSeries = []
        self.bootData.append(gameSeries)
        self.bootDict[gameId] = gameSeries
        junk = data.readline() # skip first line
        for line in data.readlines():
            row = line.split(', ')
            #dow = int(row[1])
            #hod = int(row[2])
            #how = (dow - 1) * 24 + hod # hour-of-week
            prod = self.floatMaybe(row[3])
            cons = self.floatMaybe(row[4])
            #if cons < 0.0: # omit rows where cons == 0
            net = self.extractNet(prod, cons)
            gameSeries.append(net)

    def extractNet (self, prod, cons):
        net = -cons
        if self.dataType == 'net-demand':
            net -= prod
        elif self.dataType == 'solar' or self.dataType == 'production':
            net = prod
        return net


    def dataArray (self, interval):
        '''
        Returns the array corresponding to the specified interval, which must be
        one of 'Weekly', 'Weekday', 'Weekend', 'Daily', 'Game'. '''
        self.ensureGameData()
        return self.dataMap[interval]

    def ensureGameData (self):
        if len(self.gameData) == 0:
            self.collectData()

    def bootArray (self):
        '''
        Returns the array for the boot interval.
        '''
        self.ensureBootData()
        return self.bootData

    def ensureBootData (self):
        if len(self.bootData) == 0:
            self.collectData(logType='boot')

    def imputeBootData (self):
        '''
        Generates "fake" boot data by sampling the game data at 
        '''
        self.ensureGameData()
        self.bootData = []
        interval = 5 # must be odd number
        for gameId in self.gameDict.keys():
            data = []
            gd = self.gameDict[gameId]
            self.bootDict[gameId] = data
            for i in range(int(len(gd) / interval)):
                data.append(gd[i][1])

    def floatMaybe (self, str):
        '''returns the float representation of a string, unless the string is
         empty, in which case return 0. Should have been a lambda, but not
         with Python. '''
        result = 0.0
        if str != '' :
            result = float(str)
        else:
            print('failed to float', str)
        return result
