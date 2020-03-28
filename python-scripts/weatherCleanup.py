#!/usr/bin/python3

# Extracts weather data from json file as produced by the Aeris service.

import json
import datetime
from dateutil import parser

def extractReports (inputFile, outputFile, location, start, end):
    obsString = '-- observation: '
    locString = '-- location: '
    time = ''

    startTime = parser.parse(start)
    endTime = parser.parse(end)
    collecting = False
    with open(inputFile, 'r') as weather:
        with open(outputFile, 'w') as csv:
            csv.write('day,hrEnd,tempC,dewpointC,pressureMB,windKPH\n')
            thisLocation = False
            lineCount = 0
            lastGoodRow = []
            missingRowCount = 0
            for line in weather.readlines():
                lineCount += 1
                if line.rstrip == '':
                    continue
                if line.startswith(obsString):
                    # pick off time, update state
                    time = parser.parse(line.rstrip()[len(obsString):])
                    if not collecting and time >= startTime and time <= endTime:
                        collecting = True
                    elif collecting and time > endTime:
                        return
                elif collecting and line.startswith(locString):
                    # pick off location
                    loc = line.rstrip()[len(locString):]
                    if loc == location:
                        thisLocation = True
                elif collecting and thisLocation:
                    # this should be the observation
                    thisLocation = False
                    try:
                        w = json.loads(line.rstrip())
                    except:
                        print(line.rstrip())
                        continue
                    success = True
                    if w.get('success') == False:
                        print('api fail line {}'.format(lineCount))
                        success = False
                    elif w.get('error') != None:
                        print('api error {} line {}'.format(w.get('error'),
                                                            lineCount))
                        success = False
                    # extract tempC, dewpointC, pressureMB, windKPH
                    response = w.get('response')
                    if response == None or response == []:
                        if not success:
                            print('response not found line {}: {}'
                                  .format(lineCount, line))
                            success = False
                    else:
                        obs = response.get('ob')
                        if obs == None:
                            print('ob not found line {}: {}'.format(lineCount,
                                                                    line))
                            success = False
                    if not success:
                        missingRowCount += 1
                    else:
                        data = [time, obs.get('tempC'), obs.get('dewpointC'),
                                obs.get('pressureMB'), obs.get('windKPH')]
                        if missingRowCount > 0:
                            print("{} missing rows", missingRowCount)
                            missingRows = interpolate(missingRowCount,
                                                      lastGoodRow, data)
                            for row in missingRows:
                                csv.write('{},{},{},{},{},{}\n'
                                          .format(row[0].strftime('%m/%d/%Y'),
                                                  row[0].hour + 1,
                                                  row[1], row[2],
                                                  row[3], row[4]))
                            missingRowCount = 0
                        lastGoodRow = data
                        csv.write('{},{},{},{},{},{}\n'
                                  .format(data[0].strftime('%m/%d/%Y'),
                                          data[0].hour + 1,
                                          data[1], data[2], data[3], data[4]))

def interpolate (count, prevRow, followingRow):
    ''' returns a list of length count containing values interpolated between
        prevRow and followingRow'''
    result = []
    for i in range(1, count + 1):
        frac = i / (count + 1)
        row = [prevRow[0] + datetime.timedelta(hours = i),
               prevRow[1] + (followingRow[1] - prevRow[1]) * frac,
               prevRow[2] + (followingRow[2] - prevRow[2]) * frac,
               prevRow[3] + (followingRow[3] - prevRow[3]) * frac,
               prevRow[4] + (followingRow[4] - prevRow[4]) * frac]
        result.append(row)
    print('count {}: result {}'.format(count, result))
    return result
