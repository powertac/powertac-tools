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

import org.apache.log4j.Logger;
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
 * @author jcollins
 */
public class MktPriceStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

  // service references
  private DomainObjectReader dor;
  private TimeslotRepo timeslotRepo;
  private TimeService timeService;

  // Data
  private TreeMap<Integer, ClearedTrade[]> data;
  private int ignoreInitial = 5; // timeslots to ignore at the beginning
  private int ignoreCount = 0;
  private int indexOffset = 0; // should be Competition.deactivateTimeslotsAhead - 1
  
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
    dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
    timeslotRepo = (TimeslotRepo) SpringApplicationContext.getBean("timeslotRepo");
    timeService = (TimeService) SpringApplicationContext.getBean("timeService");
    dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    dor.registerNewObjectListener(new ClearedTradeHandler(),
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
      for (ClearedTrade trade : entry.getValue()) {
        if (null == trade) {
          output.print(delim + "[0.0 0.0]");
        }
        else {
          output.format("%s[%.4f %.4f]", delim,
                        trade.getExecutionMWh(), trade.getExecutionPrice());
        }
        delim = " ";
      }
      output.println();
    }
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
      int target = ct.getTimeslot().getSerialNumber();
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
