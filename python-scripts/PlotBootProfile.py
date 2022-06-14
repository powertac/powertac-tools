#!/usr/bin/python3

'''
Reads a boot record, extracts and plots the daily capacity of selected
customer models.
'''

from numpy import *
import matplotlib, matplotlib.pyplot as pp
from pylab import *
import string,re,os
import xml.etree.ElementTree as ET

def floatMaybe (str):
    result = 0.0
    if str != '' :
        result = float(str)
    return result

def plotProfile (bootfile, title):
    '''
    Plots the mean daily consumption profile during a boot session
    '''
    data = processFile(bootfile)
    means = mean(data, 0)
    
    fig = figure()
    ax = fig.add_subplot(1,1,1)
    ax.boxplot(-data, showfliers=False)
    ax.set_title(title)
    ax.yaxis.grid(True)
    ax.set_ylabel("kWh")
    ax.set_xlabel("hour of day")
    pp.savefig('plots/box-' + title)
    #fig.show()

def makeArrayFromString (valueString, period):
    '''
    Turns valueString into a period x n array
    '''
    result = []
    tokens = valueString.split(',')
    row = []
    rowCount = 0
    for token in tokens:
        row.append(floatMaybe(token))
        rowCount += 1
        if rowCount == period:
            result.append(row)
            row = []
            rowCount = 0
    return array(result)

def processFile (bootfile):
    '''
    PUlls out the capacity values for the given customer from a boot record
    '''
    tree = ET.parse(bootfile)
    root = tree.getroot()
    valueString = root.findtext(".//bootstrap/customer-bootstrap-data[@customerName=\"residential_ev\"]/netUsage")
    values = makeArrayFromString(valueString, 24)
    return values


#values = bp.processFile('server-distribution/evc2.xml')
