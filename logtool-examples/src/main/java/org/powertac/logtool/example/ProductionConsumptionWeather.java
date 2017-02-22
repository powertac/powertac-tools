/*
 * Copyright (c) 2016 by John E. Collins
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
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.TariffTransaction;
import org.powertac.common.WeatherReport;
import org.powertac.common.msg.TimeslotUpdate;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers customer production and consumption data
 * By default, output is one row per timeslot:
 *   timeslot index, day of week, hour of day,
 *   total production, total consumption,
 *   temperature, wind speed, sky cover
 *
 * @author John Collins
 */
public class ProductionConsumptionWeather
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(ProductionConsumptionWeather.class.getName());

  private DomainObjectReader dor;

  // option flag
  //private boolean byBroker = false;
  //private String gameId = null;

  // data collectors for current timeslot
  private int timeslot;
  private double used = 0.0;
  private double produced = 0.0;
  private HashMap<Integer, WeatherReport> weatherReports;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public ProductionConsumptionWeather ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    System.out.println(String.format("Args = {}", (Object[])args));
    new ProductionConsumptionWeather().cli(args);
  }
  
  /**
   * Takes at least two args, input filename and output filename.
   * The --by-broker option changes the output format.
   */
  private void cli (String[] args)
  {
    System.out.println("ProductionConsumptionWeather.cli()");
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> input-file output-file");
      return;
    }
    dataFilename = args[1];
    super.cli(args[0], this);
  }

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    dor = (DomainObjectReader)getBean("domainObjectReader");
    dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    dor.registerNewObjectListener(new TariffTxHandler(),
                                  TariffTransaction.class);
    dor.registerNewObjectListener(new WeatherReportHandler(),
                                  WeatherReport.class);
    try {
      System.out.println("Writing to " + dataFilename);
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      System.out.println("Data output failed: " + e.toString());
      e.printStackTrace();
    }
    weatherReports = new HashMap<>();
    dataInit = false;
  }

  @Override
  public void report ()
  {
    data.close();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot (Instant instant)
  {
    if (!dataInit) {
      // first time through nothing to but print header
      data.println("slot, dow, hour, production, consumption, temp, wind, cloud");
      //gameId = Competition.currentCompetition().getName();
      dataInit = true;
      return;
    }

    // reject rows with zero prod, cons
    if (0.0 == produced && 0.0 == used)
      return;

    // print timeslot, dow, hod,
    data.print(String.format("%d, %d, %d, ",
                             timeslot,
                             instant.get(DateTimeFieldType.dayOfWeek()),
                             instant.get(DateTimeFieldType.hourOfDay())));
    // print customer production, consumption, 
    data.print(String.format("%.3f, %.3f, ",
                             produced, used));
    // look up the weather report, print the data
    WeatherReport wr = weatherReports.get(timeslot);
    data.println(String.format("%.3f, %.3f, %.3f",
                               wr.getTemperature(), wr.getWindSpeed(),
                               wr.getCloudCover()));
    weatherReports.remove(timeslot);
    produced = 0.0;
    used = 0.0;
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  class TimeslotUpdateHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      TimeslotUpdate msg = (TimeslotUpdate) thing;
      timeslot = msg.getFirstEnabled() - 1;
      log.info("Timeslot " + timeslot);
      summarizeTimeslot(msg.getPostedTime());
    }
  }

  // -----------------------------------
  // catch TariffTransactions
  class TariffTxHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      TariffTransaction tx = (TariffTransaction)thing;
      //Broker broker = tx.getBroker();

      if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
        used += tx.getKWh() / 1000.0;
      }
      else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
        produced += tx.getKWh() / 1000.0;
      }
    }
  }

  // -----------------------------------
  // catch WeatherReports
  class WeatherReportHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      WeatherReport wr = (WeatherReport)thing;
      weatherReports.put(wr.getTimeslotIndex(), wr);
    }
  }
}
