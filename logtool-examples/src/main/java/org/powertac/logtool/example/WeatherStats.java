/*
 * Copyright (c) 2012, 2017 by John E. Collins
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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.Instant;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherReport;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Pulls out weather reports, generates daily temperature reports.
 * 
 * @author John Collins
 */
public class WeatherStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(WeatherStats.class.getName());

  private TimeService timeService;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "temps.txt";

  /**
   * Default constructor
   */
  public WeatherStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new WeatherStats().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> input-file output-file");
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
    timeService = (TimeService) SpringApplicationContext.getBean("timeService");
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
    data.close();
  }

  // -------------------------------
  // catch WeatherReports
  Instant currentDay = null;

  public void handleMessage (WeatherReport rpt)
  {
    Instant midnight = timeService.getCurrentTime();
    if (null == currentDay || midnight.isAfter(currentDay)) {
      currentDay = midnight;
      data.format("%n%s%n", currentDay.toString());        
    }
    data.format("%.2f ", rpt.getTemperature());
  } 
}
