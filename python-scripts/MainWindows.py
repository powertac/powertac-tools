# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 15:39:07 2015

@author: Mohammad Ansarin (ansarin@rsm.nl)

Base code for iterating logtool-example scripts on Power TAC tournament logs.
Normally creates a .data text file for each log file, then gathers all .data
files into one .csv file.
Edit input and output paths and misc. below to suit system setup and data.
"""
import os, subprocess, re
import DatafileIterator as di

if os.name == 'nt':
    wd = 'C:/Users/Mohammad/Documents/GitHub/powertac-tools/python-scripts/'
    os.chdir(wd)
    logtoolDir = r"C:/Users/Mohammad/Documents/GitHub/powertac-tools/logtool-examples"
    logdir = 'E:/PowerTAC/Logs/2014/log/'
    tournamentDir = 'E:/PowerTAC/Logs/2014/'
elif os.name == 'posix':
    '''ADD LINUX CODE'''

logtoolClass = 'org.powertac.logtool.example.EnergyMixStats'
outdir = os.path.join('C:/Users/Mohammad/Documents/Google Drive/PhD/PowerTAC Analysis/Plotting/output csvs/2014/')
dataPrefix = 'data/energy-mix-stats-'  
output = os.path.join(outdir, "energymixstats.csv")
f = open(output,'w')
f.write("game-id, slot, import, cost, cons, revenue, prod, cost, up-reg, cost, down-reg, revenue, imbalance, cost\n")
options = '--with-gameid'


def collectData (tournamentDir):
    '''
    Processes data from data files in the specified directory.
    '''
    for dataFile in di.datafileIter(tournamentDir,
                                    logtoolClass, dataPrefix,
                                    options, logtype='sim',
                                    force=False, logtoolDir = logtoolDir):
        # note that dataFile is a Path, not a string
        processFile(str(dataFile[1]))
        #print(str(dataFile))


def processFile (dataFile):
    '''
    Collects data from a data file and writes 
    into the (already opened) CSV file.
    '''
    data = open(dataFile, 'r')
    #data.readline() # skip first line. Remove if unneeded
    for line in data.readlines():
        '''Define export style here:
            '''
        row = line.split(', ')
        if ((row[0] != "Summary") & (row[0] != "game-id")):
            f.write(line)
        '''row[0] = ''.join(re.findall(r'\d+',row[0]))
        row1tmp = int(float(row[1]))
        row2tmp = floatMaybe(row[2])
        row3tmp = floatMaybe(row[3])
        row4tmp = floatMaybe(row[4])
        row5tmp = floatMaybe(row[5])
        row6tmp = floatMaybe(row[6])
        row7tmp = floatMaybe(row[7])
        row8tmp = floatMaybe(row[8])
        row9tmp = floatMaybe(row[9])
        row10tmp = floatMaybe(row[10])
        row11tmp = floatMaybe(row[11])
        row12tmp = floatMaybe(row[12])
        row13tmp = floatMaybe(row[13])
        
        tmp = str("%s,\n" % (row[0],
                row1tmp-360,row2tmp,row3tmp,row4tmp,row5tmp,row6tmp,row7tmp,
                row8tmp,row9tmp,row10tmp,row11tmp,row12tmp,row13tmp))
        f.write(tmp)'''
        
          
def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str != '' :
        result = float(str)
    else:
        print('failed to float', str)
    return result
    

def main():  
    print("Collecting data from %s" %tournamentDir)
    collectData(tournamentDir)
    print("Data extraction complete!")
    
    f.close()

if __name__ == "__main__":
    main()
