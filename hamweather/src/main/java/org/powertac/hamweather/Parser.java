/*
 * Copyright 2014 John E. Collins.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS,  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.powertac.hamweather;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * This is the hamweather parser.
 * Usage:
 *   Parser --location loc        // one of the locations in the input file
 *          --start-hour hour     // if given, constrains batch start hour
 *          --json input-filename
 *          --xml output-filename
 * @author John Collins
 */
public class Parser
{
  
  private String inputFile;
  private String location;

  private enum State {OBS, LOC, JSON_OB, JSON_FCST}
  private static final int MAX_INTERVAL = 60 * 65 * 1000;
  private static final int FORECAST_HORIZON = 24;
  private static final int HOUR = 3600 * 1000;

  private DateTimeFormatter iso;
  private OutputStructure output;

  private Integer batchStartHour = null; // if non-null, restricts batch start

  /**
   * Reads the command-line, then the input JSON file. Extracted weather data
   * is then dumped to the output file in xml format in batches.
   */
  public static void main (String[] args)
  {
    Parser instance = new Parser();
    instance.processCli(args);
    instance.processFiles();
  }
  
  public void processCli (String[] args)
  {
    OptionParser parser = new OptionParser();
    OptionSpec<String> locationOption =
      parser.accepts("location").withRequiredArg().ofType(String.class)
          .required();
    OptionSpec<Integer> startOption =
      parser.accepts("start-hour").withRequiredArg().ofType(Integer.class);
    OptionSpec<String> jsonOption =
      parser.accepts("json").withRequiredArg().ofType(String.class).required();
    OptionSpec<String> xmlOption =
      parser.accepts("xml").withRequiredArg().ofType(String.class).required();
    OptionSpec<String> errOption =
        parser.accepts("err").withRequiredArg().ofType(String.class).required();

    OptionSet options = parser.parse(args);
    location = options.valueOf(locationOption);
    inputFile = options.valueOf(jsonOption);
    if (options.has("start-hour")) {
      batchStartHour = options.valueOf(startOption);
    }
    if (options.has(xmlOption)) {
      output = new XmlOutputStructure();
      output.setOutputFile(options.valueOf(xmlOption));
      output.setBatchStartHour(batchStartHour);
    }
    else if (options.has(errOption)) {
      output = new WindErrOutput();
    }
  }
  
  public void processFiles ()
  {
    try {
      BufferedReader in = new BufferedReader(new FileReader(inputFile));
      Pattern observation =
        Pattern.compile("^-- observation: ([SMTWF][a-z]+ [A-Za-z]+ \\d+ [-0-9: ]+)");
      Pattern loc = Pattern.compile("^-- location: " + location);
      String line;
      State state = State.OBS;
      Matcher m;
      JSONParser jparser = new JSONParser();
      DateTimeFormatter dtf = DateTimeFormat.forPattern("E MMM d HH:mm:ss Z YYYY");
      iso = ISODateTimeFormat.dateTimeNoMillis();
      DateTime lastObs = null;
      DateTime obsHour = null;
      while (true) {
        line = in.readLine();
        if (null == line || 0 == line.length())
          break;
        switch (state) {
        case OBS:
          m = observation.matcher(line);
          if (m.matches()) {
            DateTime obsTime = dtf.parseDateTime(m.group(1));
            if (null == lastObs) {
              lastObs = obsTime;
            }
            else if (obsTime.isAfter(lastObs.plus(MAX_INTERVAL))) {
              System.out.println("Missed obs - last = " + iso.print(lastObs)
                                 + ", current = " + iso.print(obsTime));
            }
            lastObs = obsTime;
            obsHour =
              obsTime.plus(HOUR / 2).withMillisOfSecond(0).withSecondOfMinute(0)
                  .withMinuteOfHour(0);
            state = State.LOC;
          }
          break;
        case LOC:
          m = loc.matcher(line);
          if (m.matches()) {
            //System.out.println("Location: " + location);
            state = State.JSON_OB;
          }
          break;
        case JSON_OB:
          // process new observation
          JSONObject obs = (JSONObject)jparser.parse(line);
          // check for errors
          Boolean success = (Boolean)obs.get("success");
          if (!success) {
            System.out.println("Observation retrieval failed at "
                               + iso.print(obsHour));
            state = State.OBS;
          }
          else {
            JSONObject err = (JSONObject)obs.get("error");
            if (null != err) {
              // error at server end
              String msg = (String) err.get("description");
              System.out.println("Observation error: " + msg
                                 + " at " + iso.print(obsHour));
              state = State.OBS;
            }
            else {
              try {
                JSONObject response = (JSONObject) obs.get("response");
                JSONObject ob = (JSONObject) response.get("ob");
                extractObservation(ob);
                state = State.JSON_FCST;
              }
              catch (ClassCastException cce) {
                System.out.println("Faulty observation " + obs.toString());
                state = State.OBS;
              }
            }
          }
          break;
        case JSON_FCST:
          // process new forecast
          JSONObject fcst = (JSONObject)jparser.parse(line);
          // check for errors
          Boolean success1 = (Boolean)fcst.get("success");
          if (!success1) {
            // could not retrieve forecast
            System.out.println("Forecast retrieval failed at "
                               + iso.print(obsHour));
            output.forecastMissing();
          }
          else {
            JSONObject err = (JSONObject)fcst.get("error");
            if (null != err) {
              // error at server end
              String msg = (String) err.get("description");
              System.out.println("Forecast error: " + msg
                                 + " at " + iso.print(obsHour));
              output.forecastMissing();
            }
            else {
              JSONArray response = (JSONArray) fcst.get("response");
              if (response.size() == 0) {
                // should never get here
                System.out.println("Empty forecast at " + iso.print(obsHour));
              }
              JSONObject periods = (JSONObject) response.get(0);
              JSONArray fcsts = (JSONArray) periods.get("periods");
              if (fcsts.size() != FORECAST_HORIZON) {
                System.out.println("Missing forecasts (" + fcsts.size()
                                   + ") at " + iso.print(lastObs));
              }
              for (int i = 0; i < fcsts.size(); i++) {
                JSONObject forecast = (JSONObject) fcsts.get(i);
                extractForecast(forecast, i + 1, obsHour);
              }
            }
          }
          state = State.OBS;
          break;
        }
      }
      output.write();
      in.close();
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (org.json.simple.parser.ParseException e) {
      e.printStackTrace();
    }
  }

  private void extractObservation (JSONObject ob)
  {
    String timeString = (String) ob.get("dateTimeISO");
    DateTime obTime = iso.parseDateTime(timeString);
    Long temp = (Long) ob.get("tempC");
    Long dewpoint = (Long) ob.get("dewpointC");
    Long pressure = (Long) ob.get("pressureMB");
    Long windKPH = (Long) ob.get("windKPH");
    if (null == windKPH) {
      // no wind data - don't use
      System.out.println("null wind at " + iso.print(obTime));
      return;
    }
    output.addObservation(obTime, temp, dewpoint, pressure, windKPH);
  }

  private void extractForecast (JSONObject forecast, int index, DateTime hour)
  {
    String timeString = (String) forecast.get("dateTimeISO");
    DateTime fcTime = iso.parseDateTime(timeString);
    Long temp = (Long) forecast.get("tempC");
    Long dewpoint = (Long) forecast.get("dewpointC");
    Long sky = (Long) forecast.get("sky");
    Long windKPH = (Long) forecast.get("windSpeedKPH");
    output.addForecast(fcTime, index, hour, temp, dewpoint, sky, windKPH);
  }

}
