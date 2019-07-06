/*
 * Copyright (c) 2019 by John E. Collins
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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.CapacityTransaction;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers CapacityTransactions, reports threshold, total excess demand,
 * and capacity cost for each assessment interval.
 * Output is one row for each assessment:
 *   timeslot, theshold, excess demand, capacity cost
 * 
 * NOTE: Numeric data is formatted using the US locale in order to avoid confusion over
 * the meaning of the comma character when used in other locales.
 *
 * @author John Collins
 */
public class CapacityAnalysis
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(CapacityAnalysis.class.getName());

  // Game & state data
  //private Competition competition = null;
  //private String gameId = null;
  private boolean started = false;
  private int skip = 1;

  // data collectors for current timeslot
  private int timeslot;
  private double threshold = 0.0;
  private double excessDemand = 0.0;
  private double capacityCost = 0.0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public CapacityAnalysis ()
  {
    super();
  }

  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CapacityAnalysis().cli(args);
  }

  /**
   * Takes at least two args, input filename and output filename.
   * The --by-broker option changes the output format.
   */
  private void cli (String[] args)
  {
    if (args.length < 2) {
      System.out.println("Usage: <analyzer> input-file output-file");
      return;
    }
    dataFilename = args[1];
    super.cli(args[0], this);
  }

  /**
   * Opens output file.
   */
  @Override
  public void setup ()
  {
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  // one-time initialization happens on SimStart
  private void initData ()
  {
    // first time through nothing to but print header
    data.println("slot, threshold, excess, cost");
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
    // Do nothing unless the threshold > 0
    if (threshold == 0.0)
      return;
    data.format("%d, %s, %s, %s\n",
                timeslot,
                df.format(threshold),
                df.format(excessDemand),
                df.format(capacityCost));
    threshold = 0.0;
    excessDemand = 0.0;
    capacityCost = 0.0;
  }

  // -----------------------------------
  // Catch the SimStart event to start things up.
  // This avoids seeing the first timeslot twice.
  public void handleMessage (SimStart ss)
  {
    started = true;
    initData();
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    if (!started)
      return;
    if (skip > 0) {
      skip -= 1;
    }
    else {
      summarizeTimeslot(msg.getPostedTime());
    }
    timeslot = msg.getFirstEnabled() - 1;
    log.info("Start timeslot " + timeslot);
  }

  // -----------------------------------
  // catch CapacityTransactions, accumulate data
  public void handleMessage (CapacityTransaction tx)
  {
    threshold = tx.getThreshold();
    excessDemand += tx.getKWh();
    capacityCost += tx.getCharge();
  }
}
