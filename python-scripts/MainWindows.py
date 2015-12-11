# -*- coding: utf-8 -*-
"""
Created on Tue Nov 17 15:39:07 2015

@author: Mohammad
"""
import os, subprocess, re
#import numpy as np
#import scipy as sp
#import matplotlib.pyplot as plt
#import matplotlib.lines as mline
#import pylab as pl
#from scipy import stats
#from pathlib import Path

import DatafileIterator as di
if os.name == 'nt':
    wd = 'C:/Users/Mohammad/Documents/GitHub/Powertac-tools/python-scripts/'
    os.chdir(wd)
    logtoolDir = r"C:/Users/Mohammad/Documents/GitHub/powertac-tools/logtool-examples"
    logdir = 'F:/PowerTAC/Logs/2015/log/'
    tournamentDir = 'F:/PowerTAC/Logs/2015/'
elif os.name == 'posix':
    '''ADD LINUX CODE'''

'''Edit these parameters to suit data.'''
logtoolClass = 'org.powertac.logtool.example.ImbalanceSummary'
dataPrefix = 'data/imbalance-summary-'   
outdir = os.path.join('C:/Users/Mohammad/Documents/Google Drive/PhD/PowerTAC Analysis/Plotting/', 'output/')
output = os.path.join(outdir, "imbalancesummary.csv")
f = open(output,'w')
#f.write("game-id-broker, slot, import, cost, cons, revenue, prod, cost, up-reg, cost, down-reg, revenue, imbalance, cost\n")
options = ''
'''For future personal use:
    Line 1: game-id,n_brokers,c_total,cr_total,p_total,pr_total,i_total,i_rms,ir_total
    Line 2-end: broker-name,c_broker,cr_broker,p_broker,pr_broker,i_broker,i_rms-broker,ir_broker
    '''
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
        '''DEFINE EXPORTING PARAMETERS HERE: '''
        #row = line.split(', ')
        f.write(line)
        '''if (row[0] == "Summary"):
            break
        row[0] = ''.join(re.findall(r'\d+',row[0]))
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
    
    '''for i in range(1, 2):
        file_name = "powertac-sim-" + str(i) + ".state"
        file_path = os.path.join(logdir, file_name)
        print ("Processing", file_path)
        print ("Processing complete!")'''
        
    f.close()

if __name__ == "__main__":
    main()
