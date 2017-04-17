/*
 * Copyright (c) 2014, 2017 by the original author
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
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.Instant;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherForecast;
import org.powertac.common.WeatherForecastPrediction;
import org.powertac.common.WeatherReport;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Pulls out weather reports, generates wind prediction reports.
 * Each line in the file contains a wind speed number, followed by
 * the predictions of wind speed from one hour earlier through 24
 * hours earlier.
 * 
 * @author John Collins
 */
public class WindStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(WindStats.class.getName());

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "winds.txt";

  // data collector
  private TreeMap<Integer, Double[]> wind;
  private int firstIndex = -1;
  private int horizon = 24;

  /**
   * Default constructor
   */
  public WindStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new WindStats().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> state-file output-file");
      return;
    }
    dataFilename = args[1];
    super.cli(args[0], this);
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    wind = new TreeMap<Integer, Double[]>();
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    for (Integer index : wind.keySet()) {
      Double[] speeds = wind.get(index);
      if (null == speeds) {
        System.out.println("null array for index " + index);
      }
      String delim = "";
      for (int i = 0; i < speeds.length; i++) {
        data.format("%s%.2f", delim, speeds[i]);
        delim = " ";
      }
      data.println();
    }
    data.close();
  }

  // -------------------------------
  // catch WeatherReports
  public void handleMessage (WeatherReport rpt)
  {
    if (firstIndex < 0) return;
    int tsIndex = rpt.getTimeslotIndex();
    Double[] speeds = wind.get(tsIndex);
    if (null == speeds) {
      System.out.println("Cannot find array for ts " + tsIndex);
    }
    else {
      speeds[horizon] = rpt.getWindSpeed();
    }
  }

  // -------------------------------
  // catch WeatherReports
  public void handleMessage (WeatherForecast fcst)
  {
    int ts = fcst.getTimeslotIndex();
    if (firstIndex < 0) {
      // initialization
      horizon = fcst.getPredictions().size();
      System.out.println("Horizon = " + horizon);
      firstIndex = ts + horizon;
    }
    for (WeatherForecastPrediction wfp : fcst.getPredictions()) {
      int index = ts + wfp.getForecastTime();
      if (index >= firstIndex) {
        Double[] speeds = wind.get(index);
        if (null == speeds) {
          speeds = new Double[horizon + 1];
          wind.put(index, speeds);
        }
        speeds[horizon - wfp.getForecastTime()] = wfp.getWindSpeed();
      }
    }
  }
}
