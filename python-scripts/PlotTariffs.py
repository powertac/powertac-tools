#!/usr/bin/python

'''
Reads files produced by the TariffAnalysis logtool in profile mode, and plots
the timeseries of individual tariffs. It's really only useful on TOU tariffs.

Depends on a Python 3 installation with the SciPy/NumPy libraries.
'''

from numpy import *
import scipy, pylab, matplotlib
from pylab import *
from scipy import stats
import string,re,os

tariffs = {}

def init ():
    tariffs = {}

def readTariffs (datafile):
    data = open(datafile)
    for line in data.readlines():
        info = eval(line)
        tariffs[info['tariffId']] = info
            

def floatMaybe (str):
    '''returns the float representation of a string, unless the string is
     empty, in which case return 0. Should have been a lambda, but not
     with Python.'''
    result = 0.0
    if str not in ['', '-']:
        result = float(str)
    return result
    

def plotRates (tariffIds, size=168, save=''):
    for tid in tariffIds:
        data = tariffs[tid]['rate']
        repeat = int(size / len(data))
        series = []
        for i in range(repeat):
            series.extend(data)
        xy = plt.plot(series, label=tariffs[tid]['broker'])
    plt.title('Tariff Rates')
    plt.xlabel('Hour')
    plt.ylabel('price')
    plt.xticks(np.arange(0, size + 1, 12))
    plt.legend()
    plt.grid()
    if save == '':
        plt.show()
    else:
        plt.savefig(save, dpi=300)
    plt.cla()
