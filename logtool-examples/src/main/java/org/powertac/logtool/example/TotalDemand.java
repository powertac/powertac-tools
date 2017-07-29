/*
 * Copyright (c) 2017 by John E. Collins
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
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers customer net demand (consumption - production)
 * and external wholesale demand (the MisoBuyer).
 * Output is one row per timeslot:
 *   timeslot index, day of week, hour, total consumption, external demand
 *
 * @author John Collins
 */
public class TotalDemand
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(TotalDemand.class.getName());

  // Game & state data
  private Competition competition = null;
  //private String gameId = null;
  private boolean started = false;
  private int skip = 1;

  // data collectors for current timeslot
  private int timeslot;
  private double intDemand = 0.0;
  HashMap<Broker, HashMap<Integer, Double>> wholesalePosn;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public TotalDemand ()
  {
    super();
  }

  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new TotalDemand().cli(args);
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
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
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
    data.println("slot, dow, hour, int_demand, ext_demand");
    competition = Competition.currentCompetition();
    BrokerRepo brokerRepo = (BrokerRepo)getBean("brokerRepo");
    wholesalePosn = new HashMap<>();
    brokerRepo.findWholesaleBrokers().forEach((b) -> {
      wholesalePosn.put(b, new HashMap<>());
    });
    //gameId = competition.getName();
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
    // output format depends on options
    // print timeslot, dow, hod, production, consumption
    data.printf("%d, %d, %d",
                timeslot,
                instant.get(DateTimeFieldType.dayOfWeek()),
                instant.get(DateTimeFieldType.hourOfDay()));
    double extDemand = 0.0;
    for (Broker b: wholesalePosn.keySet()) {
      HashMap<Integer, Double> tsMap = wholesalePosn.get(b);
      Double qty = tsMap.get(timeslot);
      if (null != qty && qty > 0.0)
        extDemand += qty;
    }
    // print customer usage
    data.printf(", %.3f, %.3f\n", intDemand, extDemand);
    intDemand = 0.0;
  }

  // -----------------------------------
  // Catch the SimStart event to start things up.
  // This avoids seeing the first timeslot twice.
  public void handleMessage (SimStart ss)
  {
    competition = Competition.currentCompetition();
    initData();
    started = true;
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    if (!started)
      return;
    if (skip > 0) {
      skip -= 1;
      timeslot = msg.getFirstEnabled() - 1;
    }
    else {
      summarizeTimeslot(msg.getPostedTime());
      timeslot = msg.getFirstEnabled() - 1;
      log.info("Start timeslot " + timeslot);
    }
  }

  // -----------------------------------
  // catch TariffTransactions, accumulate net demand
  public void handleMessage (TariffTransaction tx)
  {
    if (!started)
      return;
    if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
        intDemand -= tx.getKWh() / 1000.0;
    }
    else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
        intDemand -= tx.getKWh() / 1000.0;
    }
  }

  // ------------------------------------
  // catch MarketTransaction events from wholesale brokers, add up quantities
  // per timeslot
  public void handleMessage (MarketTransaction tx)
  {
    HashMap<Integer, Double> brokerMap = wholesalePosn.get(tx.getBroker());
    if (null != brokerMap) {
      int ts = tx.getTimeslotIndex();
      Double value = brokerMap.get(ts);
      if (null == value)
        brokerMap.put(ts, tx.getMWh());
      else
        brokerMap.put(ts, value + tx.getMWh());
    }
  }
}
