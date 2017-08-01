/*
 * Copyright (c) 2012 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.logtool.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.Instant;
import org.powertac.common.WeatherForecast;
import org.powertac.common.WeatherForecastPrediction;
import org.powertac.common.WeatherReport;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Pulls out weather reports and forecasts, creates two output files, one
 * for temperature data and one for wind data. Each file consists of lines
 * with the following format
 * timeslot obs fc-1 fc-2 ... fc-24
 * 
 * Invoke as WeatherForecastStats 
 * 
 * @author John Collins
 */
public class WeatherForecastStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(WeatherForecastStats.class.getName());

  // data output file
  private PrintWriter temp = null;
  private PrintWriter wind = null;
  private String tempFilename = "temp.txt";
  private String windFilename = "wind.txt";

  // data accumulation
  private int tsOffset = 0; 
  private ArrayList<Element> data;

  /**
   * Default constructor
   */
  public WeatherForecastStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new WeatherForecastStats().cli(args);
  }
  
  /**
   * Takes three args:
   *     input_filename (or -) temp-data_filename, wind-data_filename 
   * In each output file, each line starts with an observation followed by
   * 24 predictions, with the first being 1h out, the last 24h out.
   */
  private void cli (String[] args)
  {
    if (args.length != 3) {
      System.out.println("Usage: <analyzer> input-file temp-data wind-data");
      return;
    }
    tempFilename = args[1];
    windFilename = args[2];
    super.cli(args[0], this);
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    try {
      temp = new PrintWriter(new File(tempFilename));
      wind= new PrintWriter(new File(windFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file");
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    for (Element el : data) {
      wind.format("%d %.3f ", el.timeslot, el.reportedWind);
      for (double fcw : el.forecastWind) {
        wind.format("%.3f ", fcw);
      }
      wind.println();
      temp.format("%d %.3f ", el.timeslot, el.reportedTemp);
      for (double fct : el.forecastTemp) {
        temp.format("%.3f ", fct);
      }
      temp.println();
    }
    wind.close();
    temp.close();
    return;
  }

  // Returns the Element corresponding to timeslot n
  private Element getElementForTs (int n)
  {
    if (null == data) {
      // need to initialize
      data = new ArrayList<Element>();
      tsOffset = n;
    }
    // compute index into array
    int index = n - tsOffset;
    // populate array if necessary
    while (data.size() < index + 1) {
      data.add(new Element(n));
      System.out.println("add " + n);
    }
    return data.get(index);
  }

  // -------------------------------
  // catch WeatherReports
  Instant currentDay = null;

  public void handleMessage (WeatherReport rpt)
  {
    Element el = getElementForTs(rpt.getTimeslotIndex());
    el.reportedTemp = rpt.getTemperature();
    el.reportedWind = rpt.getWindSpeed();
  }

  public void handleMessage (WeatherForecast fcst)
  {
    int ts = fcst.getTimeslotIndex();
    int index = ts + 1;
    for (WeatherForecastPrediction pred : fcst.getPredictions()) {
      Element el = getElementForTs(index);
      int lead = index - ts - 1;
      el.forecastTemp[lead] = pred.getTemperature();
      el.forecastWind[lead] = pred.getWindSpeed();
      index += 1;
    }
  }

  // Data storage element
  class Element
  {
    int timeslot;
    double reportedTemp;
    double reportedWind;
    double[] forecastTemp;
    double[] forecastWind;

    Element (int ts)
    {
      super();
      timeslot = ts;
      forecastTemp = new double[24];
      forecastWind = new double[24];
    }
  }
}
