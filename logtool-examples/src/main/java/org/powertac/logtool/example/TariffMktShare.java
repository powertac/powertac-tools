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
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.powertac.common.Broker;
import org.powertac.common.CustomerInfo;
import org.powertac.common.CustomerInfo.CustomerClass;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Extracts TariffTransactions, looks for SIGNUP and WITHDRAW transactions,
 * computes market share by broker and tracks total customer count.
 * 
 * First line lists brokers. Remaining lines are emitted for each timeslot
 * in which SIGNUP or WITHDRAW transactions occur, format is
 *   timeslot, customer-count, ..., total-customer-count
 * with one customer-count field for each broker.
 * 
 * If the --size option is given, then the counts are pairs [small, large]
 * giving the numbers of SMALL and LARGE customers in their portfolios
 * 
 * @author John Collins
 */
public class TariffMktShare
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(TariffMktShare.class.getName());

  private BrokerRepo brokerRepo;
  private int skip = 1;
  private boolean started = false;

  private ArrayList<Broker> brokers; // need to report in order
  private HashMap<Broker, Integer> customerCounts;
  private HashMap<Broker, Integer> largeCustomerCounts;
  private boolean txActivity = false;
  private boolean countSize = false;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public TariffMktShare ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new TariffMktShare().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    if (args.length < 2 || args.length > 3) {
      System.out.println("Usage: TariffMktShare [--size] input-file output-file");
      return;
    }
    int offset = 0;
    if (args[0].equals("--size")) {
      countSize = true;
      offset = 1;
    }
    dataFilename = args[1 + offset];
    super.cli(args[0 + offset], this);
    skip = 1;
  }

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");
    //ttx = new ArrayList<TariffTransaction>();
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  // This is run when the first tariff transaction arrives, which is during
  // server initialization while customers are subscribing to the default broker.
  public void init()
  {
    // first time through
    brokers = new ArrayList<>();
    customerCounts = new HashMap<>();
    largeCustomerCounts = new HashMap<>();
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      brokers.add(broker);
      customerCounts.put(broker, 0);
      largeCustomerCounts.put(broker, 0);
    }
    started = true;
  }

  // at sim-start we know who the rest of the brokers are
  public void addRemainingBrokers ()
  {
    data.print("ts");
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      if (!brokers.contains(broker)) {
        brokers.add(broker);
        customerCounts.put(broker, 0);
        largeCustomerCounts.put(broker, 0);
      }
      data.format(",%s", broker.getUsername());
    }
    data.println();
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
  private void summarizeTimeslot (TimeslotUpdate ts)
  {
    int currentTimeslot = ts.getFirstEnabled() - 1;
    if (!txActivity)
      return;
    // print results for this timeslot
    data.print(currentTimeslot);
    data.print(",");
    for (Broker broker: brokers) {
      if (countSize)
        data.print("[");
      int count = customerCounts.get(broker);
      data.format("%d,", count);
      if (countSize) {
        data.format("%d],", largeCustomerCounts.get(broker));
      }
    }
    data.println();
    txActivity = false;
  }

  // -----------------------------------
  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    if (!started)
      init();
    // only include SIGNUP and WITHDRAW
    int pop = 0;
    if (tx.getTxType() == TariffTransaction.Type.SIGNUP)
      pop = tx.getCustomerCount();
    else if (tx.getTxType() == TariffTransaction.Type.WITHDRAW)
      pop = -tx.getCustomerCount();
    else 
      return;

    Broker broker = tx.getBroker();
    txActivity = true;
    if (!(tx.getCustomerInfo().getCustomerClass() == CustomerClass.SMALL))
      System.out.format("customer %s not small", tx.getCustomerInfo().getName());
    if (!countSize || tx.getCustomerInfo().getCustomerClass() == CustomerClass.SMALL) {
      customerCounts.put(broker, customerCounts.get(broker) + pop);          
    }
    else {
      largeCustomerCounts.put(broker,  largeCustomerCounts.get(broker) + pop);
    }
  } 

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate tu)
  {
    if (skip > 0)
      skip -= 1;
    else
      summarizeTimeslot(tu);
  }

  // -------------------------------------
  // catch SimStart and SimEnd events
  public void handleMessage (SimStart ss)
  {
    System.out.println("Sim start");
    addRemainingBrokers();
  }

  public void handleMessage (SimEnd se)
  {
    System.out.println("Sim end");
  }
}
