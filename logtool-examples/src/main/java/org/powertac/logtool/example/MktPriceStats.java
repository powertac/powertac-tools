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
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.powertac.common.ClearedTrade;
import org.powertac.common.TimeService;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Logtool Analyzer that reads ClearedTrade instances as they arrive and
 * builds an array for each timeslot giving all the market clearings for
 * that timeslot, indexed by leadtime. The output data file has one 
 * line/timeslot formatted as<br>
 * timeslot,day-of-week,hour-of-day,[mwh price],[mwh price] ...<br>
 * Each line has 24 entries, assuming that each timeslot is open for trading
 * 24 times.
 * 
 * If the option '--no-headers' is given, the first three fields are omitted.
 * 
 * Usage: MktPriceStats [--no-headers] state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class MktPriceStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(MktPriceStats.class.getName());

  // service references
  private TimeslotRepo timeslotRepo;
  private TimeService timeService;

  // Data
  private TreeMap<Integer, ClearedTrade[]> data;
  private int ignoreInitial = 5; // timeslots to ignore at the beginning
  private int ignoreCount = 0;
  private int indexOffset = 0; // should be Competition.deactivateTimeslotsAhead - 1

  private boolean omitHeaders = false;
  private PrintWriter output = null;
  private String dataFilename = "clearedTrades.data";
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new MktPriceStats().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> [--no-headers] input-file output-file");
      return;
    }
    int argOffset = 0;
    if (args[0].equalsIgnoreCase("--no-headers")) {
      argOffset = 1;
      omitHeaders = true;
    }
    dataFilename = args[argOffset + 1];
    super.cli(args[argOffset], this);
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
    timeService = (TimeService) getBean("timeService");
    registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    registerNewObjectListener(new ClearedTradeHandler(),
                                  ClearedTrade.class);
    ignoreCount = ignoreInitial;
    data = new TreeMap<Integer, ClearedTrade[]>();
    try {
      output = new PrintWriter(new File(dataFilename));
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
    for (Map.Entry<Integer, ClearedTrade[]> entry : data.entrySet()) {
      String delim = "";
      if (!omitHeaders) {
        // add ts,dow,hod,
        DateTime dt = timeslotRepo.getDateTimeForIndex(entry.getKey());
        output.format("%d,%d,%d,", entry.getKey(),
                      dt.get(DateTimeFieldType.dayOfWeek()),
                      dt.get(DateTimeFieldType.hourOfDay()));
      }
      ClearedTrade[] trades = entry.getValue();
      if (trades.length != 24)
        log.error("short array " + trades.length);
      for (int i = 0; i < trades.length; i++) {
        if (null == trades[i]) {
          output.print(delim + "[0.0 0.0]");
        }
        else {
          output.format("%s[%.4f %.4f]", delim,
                        trades[i].getExecutionMWh(),
                        trades[i].getExecutionPrice());
        }
        delim = ",";
      }
      output.println();
    }
    output.close();
  }

  // -----------------------------------
  // catch ClearedTrade messages
  class ClearedTradeHandler implements NewObjectListener
  {

    @Override
    public void handleNewObject (Object thing)
    {
      if (ignoreCount > 0) {
        return; // nothing to do yet
      }
      ClearedTrade ct = (ClearedTrade) thing;
      int target = ct.getTimeslotIndex();
      int now = timeslotRepo.getTimeslotIndex(timeService.getCurrentTime());
      int offset = target - now - indexOffset;
      if (offset < 0 || offset > 23) {
        // problem
        log.error("ClearedTrade index error: " + offset);
      }
      else {
        ClearedTrade[] targetArray = data.get(target);
        if (null == targetArray) {
          targetArray = new ClearedTrade[24];
          data.put(target, targetArray);
        }
        targetArray[offset] = ct;
      }
    }
  }
  

  // -----------------------------------
  // catch TimeslotUpdate events
  class TimeslotUpdateHandler implements NewObjectListener
  {

    @Override
    public void handleNewObject (Object thing)
    {
      if (ignoreCount-- <= 0) {
        int timeslotSerial = timeslotRepo.currentSerialNumber();
        if (null == data.get(timeslotSerial)) {
          data.put(timeslotSerial, new ClearedTrade[24]);
        }
      }
    }
  }
}
