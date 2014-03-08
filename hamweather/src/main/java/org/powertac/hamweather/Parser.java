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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

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
  private String outputFile;
  private String location;
  private Integer batchStartHour = null; // if non-null, restricts batch start
  
  private enum State {OBS, LOC, JSON_OB, JSON_FCST}
  private static final int MAX_INTERVAL = 60 * 65 * 1000;
  private static final int FORECAST_HORIZON = 24;

  private DateTimeFormatter iso;
  private OutputStructure output;
  
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

    OptionSet options = parser.parse(args);
    location = options.valueOf(locationOption);
    inputFile = options.valueOf(jsonOption);
    outputFile = options.valueOf(xmlOption);
    if (options.has("start-hour")) {
      batchStartHour = options.valueOf(startOption);
    }
  }
  
  public void processFiles ()
  {
    output = new OutputStructure();
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
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    catch (org.json.simple.parser.ParseException e) {
      // TODO Auto-generated catch block
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

  /**
   * Builds batches of weather reports and corresponding forecasts.
   * Each batch is 24h of data and the corresponding 24h of 24h forecasts.
   * Incomplete batches are discarded.
   * @author jcollins
   */
  static enum XmlState {WAITING, GATHERING}
  static final double KPH_MPS = 1000.0 / 3600.0;
  static final int HOUR = 3600 * 1000;
  static final int BLOCKSIZE = 24;

  class OutputStructure
  {
    XmlState state = XmlState.WAITING;
    DateTime start = null;
    ArrayList<Observation> observations;
    ArrayList<Forecast> forecasts;
    
    Document doc;
    Element rootElement;

    OutputStructure ()
    {
      observations = new ArrayList<Observation>();
      forecasts = new ArrayList<Forecast>();
      DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder;
      try {
        docBuilder = docFactory.newDocumentBuilder();

        // Root element
        doc = docBuilder.newDocument();
        doc.setXmlStandalone(true);
        rootElement = doc.createElement("data");
        doc.appendChild(rootElement);
      }
      catch (ParserConfigurationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    /**
     * adds an observation, starting a new block if necessary.
     * discards accumulated data and starts over in case there's a gap
     * in the sequence.
     */
    void addObservation (DateTime when, Long temp, Long dewpoint,
                         Long pressure, Long windKPH)
    {
      startMaybe(when);
      if (state == XmlState.GATHERING) {
        if (observations.size() == BLOCKSIZE) {
          // write out and re-initialize
          // this is here because we have to check after gathering up the
          // forecasts from the previous observation.
          buildBlock();
          state = XmlState.WAITING;
          startMaybe(when);
        }
        else if (skipped(when)) {
          // we've lost data - start over
          state = XmlState.WAITING;
          startMaybe(when);
          if (state == XmlState.WAITING)
            // discard and wait for start of next block
            return;
        }
        observations.add(new Observation(when, temp, dewpoint, pressure,
                                         windKPH * KPH_MPS));
      }
    }

    /**
     * Adds a forecast to the current block.
     */
    void addForecast (DateTime when, int index, DateTime hour, Long temp,
                      Long dewpoint, Long sky, Long windKPH)
    {
      if (state == XmlState.GATHERING) {
        forecasts.add(new Forecast(when, index, hour, temp,
                                   dewpoint, windKPH * KPH_MPS));
      }
    }

    /**
     * Aborts a set due to missing forecast
     */
    void forecastMissing ()
    {
      state = XmlState.WAITING;
    }

    /**
     * Writes the xml to the output file
     */
    void write()
    {
      FileOutputStream out;
      try {
        out = new FileOutputStream(new File(outputFile));
        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer = tFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(out);
        transformer.transform(source, result);
        out.close();
      }
      catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      catch (TransformerConfigurationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      catch (TransformerException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    // If it's time to start, then re-initialize
    private void startMaybe (DateTime current)
    {
      if (state == XmlState.GATHERING)
        return;
      if (state == XmlState.WAITING) {
        if (timeToStart(current)) {
          start = current;
          state = XmlState.GATHERING;
          observations.clear();
          forecasts.clear();
        }
      }
      else {
        System.out.println("Bogus state " + state);
      }
    }

    private boolean timeToStart (DateTime current)
    {
      if (null == batchStartHour)
        return true;
      int hour = current.hourOfDay().get();
      if (current.minuteOfHour().get() >= 30)
        hour += 1;
      if (hour == batchStartHour)
        return true;
      return false;
    }

    private boolean skipped(DateTime when)
    {
      if (observations.size() == 0)
        return false;
      Observation last = observations.get(observations.size() - 1);
      DateTime fence = last.when.plus(HOUR + HOUR/2);
      if (when.isAfter(fence)) {
        System.out.println("Observation skipped at " + iso.print(fence));
        return true;
      }
      return false;
    }

    private void buildBlock ()
    {
      // weatherReports elements
      Element weatherReports = doc.createElement("weatherReports");
      rootElement.appendChild(weatherReports);

      for (Observation weather: observations) {
        Element weatherReport = doc.createElement("weatherReport");
        weatherReport.setAttribute("date", weather.when.toString(iso));
        weatherReport.setAttribute("windspeed", weather.windMPS.toString());
        weatherReports.appendChild(weatherReport);
      }

      // weatherForecasts elements
      Element weatherForecasts = doc.createElement("weatherForecasts");
      rootElement.appendChild(weatherForecasts);

      for (Forecast forecast: forecasts) {
        Element weatherForecast = doc.createElement("weatherForecast");
        weatherForecast.setAttribute("date", forecast.when.toString(iso));
        weatherForecast.setAttribute("id", forecast.id.toString());
        weatherForecast.setAttribute("origin", forecast.origin.toString(iso));
        weatherForecast.setAttribute("temp", forecast.temp.toString());
        weatherForecast.setAttribute("windspeed", forecast.windMPS.toString());
        weatherForecasts.appendChild(weatherForecast);
      }
    }

    // Data structures
    class Observation
    {
      DateTime when;
      Long temp;
      Long dewpoint;
      Long pressure;
      Double windMPS;

      Observation (DateTime when, Long temp, Long dewpoint,
                          Long pressure, double windMPS)
      {
        this.when = when;
        this.temp = temp;
        this.dewpoint = dewpoint;
        this.pressure = pressure;
        this.windMPS = windMPS;
      }
    }

    class Forecast
    {
      DateTime when;
      Integer id;
      DateTime origin;
      Long temp;
      Long dewpoint;
      Double windMPS;

      Forecast (DateTime when, int id, DateTime origin, Long temp,
                Long dewpoint, Double windMPS)
      {
        this.when = when;
        this.id = id;
        this.origin = origin;
        this.temp = temp;
        this.dewpoint = dewpoint;
        this.windMPS = windMPS;
      }
    }
  }
}
