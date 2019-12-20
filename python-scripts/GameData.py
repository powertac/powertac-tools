'''
Holder for game data, including sim and boot data. Aggregates data by
game, by week, by day, by weekday, by weekend-day. Allows retrieval by
name.
'''

from pathlib import Path
import TournamentLogtoolProcessor as tl

class GameData:
    '''
    dataType is one of 'net-demand', 'consumption', 'solar'
    logType is one of 'sim', 'boot'
    '''

    def __init__ (self, tournamentYear = '2018', dataType='net-demand'):
        self.reset(dataType, tournamentYear)

    def reset (self, dataType, tournamentYear):
        # data store
        self.dataType = dataType
        self.tournamentYear = tournamentYear
        self.gameData = []  # row per game
        self.bootData = []
        self.gameDict = {}
        self.bootDict = {}
        self.gameSeq = [[] for x in range(1680)]
        self.weekData = [[] for x in range(168)]
        self.weekdayData = [[] for x in range(24)]
        self.weekendData = [[] for x in range(24)]
        self.dayData = [[] for x in range(24)]
        self.dataMap = {'Weekly':self.weekData, 'Weekday':self.weekdayData,
                        'Weekend':self.weekendData, 'Daily':self.dayData,
                        'Game':self.gameSeq}
        self.processor = {'net-demand': 'extractProdCons',
                          'consumption': 'extractProdCons',
                          'production': 'extractProdCons',
                          'residualImbalance': 'extractResidualImbalance'}
        self.columnMap = {'imbalance': 'imb',
                          'imbalanceCost': 'cost',
                          'upregAvail': 'upa',
                          'upregUsed': 'upu',
                          'downregAvail': 'dna',
                          'downregUsed': 'dnu'}


    # data-type parameters
    logtoolClass = {'net-demand':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'consumption':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'production':
                    'org.powertac.logtool.example.ProductionConsumption',
                    'solar':
                    'org.powertac.logtool.example.SolarProduction',
                    'mktPrice':
                    'org.powertac.logtool.example.MktPriceStats',
                    'mktVolume':
                    'org.powertac.logtool.example.MktPriceStats',
                    'imbalance':
                    'org.powertac.logtool.example.DemandResponseStats',
                    'upregCapacity':
                    'org.powertac.logtool.example.DemandResponseStats',
                    'downregCapacity':
                    'org.powertac.logtool.example.DemandResponseStats',
                    'residualImbalance':
                    'org.powertac.logtool.example.DemandResponseStats',
                    'imbalanceCost':
                    'org.powertac.logtool.example.DemandResponseStats'}
    dataDir = 'data'
    dataPrefix = {'net-demand': 'pc',
                  'consumption': 'pc',
                  'production': 'pc',
                  'solar': 'solar-prod-',
                  'mktPrice': 'mktPr',
                  'mktVolume': 'mktPr',
                  'imbalance': 'drs',
                  'upregCapacity': 'drs',
                  'downregCapacity': 'drs',
                  'residualImbalance': 'drs',
                  'imbalanceCost': 'drs'}

    tournamentUrl = {'2019': 'file:./finals-2019/finals_2019_07.games_.csv',
                     '2018': 'file:./finals-2018/finals_2018_07.games_.csv',
                     '2017': 'file:./finals-2017/finals_2017_06.games.csv',
                     '2016': 'file:./finals-2016/finals_2016_07.games.csv'}

    tournamentDir = {'2019': 'finals-2019,
                     '2018': 'finals-2018',
                     '2017': 'finals-2017',
                     '2016': 'finals-2016'}

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
        for info in tl.dataFileIter(self.tournamentUrl[self.tournamentYear],
                                    self.tournamentDir[self.tournamentYear],
                                    self.logtoolClass[self.dataType],
                                    actualPrefix,
                                    logtype=logType,
                                    force = force):
            # note that dataFile is a Path, not a string
            if logType == 'sim':
                #print(info['gameId'], str(info['path']))
                self.processFile(info['gameId'], str(info['path']))
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
        columns = data.readline().strip().split(',') # store first line
        extractor = getattr(self, 'extractByColName')
        if self.dataType in self.processor:
            extractor = getattr(self, self.processor[self.dataType])
        hour = 1
        for line in data.readlines():
            row = line.split(',')
            dow = int(row[1])
            hod = int(row[2])
            how = (dow - 1) * 24 + hod # hour-of-week
            val = extractor(row, columns)
            gameSeries.append(val)
            if hour < len(self.gameSeq):
                self.gameSeq[hour].append(val)
                hour = hour + 1
            self.weekData[how].append(val)
            self.dayData[hod].append(val)
            if dow <= 5:
                #weekday
                self.weekdayData[hod].append(val)
            else:
                #weekend
                self.weekendData[hod].append(val)

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

    def extractByColName (self, row, columns):
        idx = columns.index(self.columnMap[self.dataType])
        return self.floatMaybe(row[idx]) / 1000

    def extractResidualImbalance (self, row, columns):
        imb = float(row[columns.index('imb')])
        residual = 0
        if imb < 0:
            dr = float(row[columns.index('upa')])
            residual = min(0, (imb + dr))
        else:
            dr = float(row[columns.index('dna')])
            residual = max(0, (imb + dr))
        return residual / 1000
        

    def extractProdCons (self, row, columns):
        prod = self.floatMaybe(row[3])
        cons = self.floatMaybe(row[4])
        return self.extractNet(prod, cons)
  

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
        Generates "fake" boot data by sampling the game data at intervals
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
