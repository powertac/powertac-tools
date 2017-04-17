/*
 * Copyright (c) 2015, 2017 by John E. Collins
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
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.Competition;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers customer production and consumption data
 * By default, output is one row per timeslot:
 *   timeslot index, day of week, hour, total production, total consumption
 * Given the command-line option --by-broker, output is broken down
 * by broker, formatted as:
 *   game-id, timeslot, broker-name, production, consumption
 *
 * @author John Collins
 */
public class ProductionConsumption
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(ProductionConsumption.class.getName());

  // option flag
  private boolean byBroker = false;
  private String gameId = null;

  // data collectors for current timeslot
  private int timeslot;
  private double used = 0.0;
  private double produced = 0.0;
  private HashMap<Broker, Double> brokerUsed;
  private HashMap<Broker, Double> brokerProduced;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public ProductionConsumption ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new ProductionConsumption().cli(args);
  }
  
  /**
   * Takes at least two args, input filename and output filename.
   * The --by-broker option changes the output format.
   */
  private void cli (String[] args)
  {
    if (args.length < 2) {
      System.out.println("Usage: <analyzer> [--by-broker] input-file output-file");
      return;
    }
    int argOffset = 0;
    if (args[0].equalsIgnoreCase("--by-broker")) {
      argOffset = 1;
      byBroker = true;
    }
    dataFilename = args[argOffset + 1];
    super.cli(args[argOffset], this);
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
      //data.println("slot, dow, hour, production, consumption");
      if (byBroker) {
        gameId = Competition.currentCompetition().getName();
        brokerUsed = new HashMap<Broker, Double>();
        brokerProduced = new HashMap<Broker, Double>();
        BrokerRepo brokerRepo = (BrokerRepo)getBean("brokerRepo");
        for (Broker broker: brokerRepo.findRetailBrokers()) {
          brokerUsed.put(broker, 0.0);
          brokerProduced.put(broker, 0.0);
        }
      }
      dataInit = true;
      return;
    }

    // output format depends on options
    if (byBroker) {
      // print game-id, timeslot, broker-name, production, consumption
      for (Broker broker: brokerUsed.keySet()) {
        data.print(String.format("%s, %d, %s, ",
                                 gameId, timeslot,
                                 broker.getUsername()));
        data.println(String.format("%.3f, %.3f", 
                                   brokerProduced.get(broker),
                                   brokerUsed.get(broker)));
        brokerProduced.put(broker, 0.0);
        brokerUsed.put(broker, 0.0);
      }
    }
    else {
      // print timeslot, dow, hod, production, consumption
      data.print(String.format("%d, %d, %d, ",
                               timeslot,
                               instant.get(DateTimeFieldType.dayOfWeek()),
                               instant.get(DateTimeFieldType.hourOfDay())));
      // print customer usage, production
      data.println(String.format("%.3f, %.3f", produced, used));
      produced = 0.0;
      used = 0.0;
    }
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    timeslot = msg.getFirstEnabled() - 1;
    log.info("Timeslot " + timeslot);
    summarizeTimeslot(msg.getPostedTime());
  }

  // -----------------------------------
  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    Broker broker = tx.getBroker();

    if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
      if (byBroker)
        brokerUsed.put(broker,
                       brokerUsed.get(broker) + tx.getKWh() / 1000.0);
      else
        used += tx.getKWh() / 1000.0;
    }
    else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
      if (byBroker)
        brokerProduced.put(broker,
                           brokerProduced.get(broker) + tx.getKWh() / 1000.0);
      else
        produced += tx.getKWh() / 1000.0;
    }
  }
}
