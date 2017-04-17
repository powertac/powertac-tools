/*
 * Copyright (c) 2015-2017 by John E. Collins
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

import org.powertac.common.Broker;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.BankTransaction;
import org.powertac.common.CapacityTransaction;
import org.powertac.common.Competition;
import org.powertac.common.DistributionTransaction;
import org.powertac.common.MarketTransaction;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers non-tariff transactions that represent broker debits and credits,
 * including market transactions, balancing transactions, distribution
 * transactions, capacity transactions, and bank transactions. Produces a
 * summary report giving the totals of these categories for each broker.
 *
 * @author John Collins
 */
public class BrokerCosts
extends LogtoolContext
implements Analyzer
{
  //static private Logger log = Logger.getLogger(BrokerCosts.class.getName());

  private String gameId;

  // data collectors for various tx quantities
  private HashMap<Broker, Double> brokerMkt;
  private HashMap<Broker, Double> brokerBal;
  private HashMap<Broker, Double> brokerDist;
  private HashMap<Broker, Double> brokerCap;
  private HashMap<Broker, Double> brokerBank;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public BrokerCosts ()
  {
    super();
  }

  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerCosts().cli(args);
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
    brokerMkt = new HashMap<>();
    brokerBal = new HashMap<>();
    brokerDist = new HashMap<>();
    brokerCap = new HashMap<>();
    brokerBank = new HashMap<>();
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
    data.println(String.format("Game %s", gameId));
    data.println("broker-name, market, balancing, distribution, capacity, bank");
    for (Broker broker: brokerMkt.keySet()) {
      data.println(String.format("%s, %.3f, %.3f, %.3f, %.3f, %.3f",
                                 broker.getUsername(),
                                 brokerMkt.get(broker),
                                 brokerBal.get(broker),
                                 brokerDist.get(broker),
                                 brokerCap.get(broker),
                                 brokerBank.get(broker)));
    }
    data.close();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void initMaybe ()
  {
    if (!dataInit) {
      // first time through nothing to but print header
      //data.println("slot, dow, hour, production, consumption");
      gameId = Competition.currentCompetition().getName();
      BrokerRepo brokerRepo = (BrokerRepo)getBean("brokerRepo");
      for (Broker broker: brokerRepo.findRetailBrokers()) {
        brokerMkt.put(broker, 0.0);
        brokerBal.put(broker, 0.0);
        brokerDist.put(broker, 0.0);
        brokerCap.put(broker, 0.0);
        brokerBank.put(broker, 0.0);
      }
      dataInit = true;
    }
  }

  // -----------------------------------
  // catch transactions
  public void handleMessage (MarketTransaction msg)
  {
    initMaybe();
    // contains MWh and price/MWh
    Broker broker = msg.getBroker();
    if (brokerMkt.keySet().contains(broker)) {
      double amt =
          -msg.getMWh() * msg.getPrice() * Math.signum(msg.getPrice());
      brokerMkt.put(broker, brokerMkt.get(broker) + amt);
    }
  }

  public void handleMessage (BalancingTransaction msg)
  {
    initMaybe();
    // contains kWH and charge
    Broker broker = msg.getBroker();
    if (brokerBal.keySet().contains(broker)) {
      double amt = msg.getCharge();
      brokerBal.put(broker, brokerBal.get(broker) + amt);
    }
  }

  public void handleMessage (DistributionTransaction msg)
  {
    initMaybe();
    // contains meter counts and charge
    Broker broker = msg.getBroker();
    if (brokerDist.keySet().contains(broker)) {
      double amt = msg.getCharge();
      brokerDist.put(broker, brokerDist.get(broker) + amt);
    }
  }

  public void handleMessage (CapacityTransaction msg)
  {
    initMaybe();
    // contains kWH and charge
    Broker broker = msg.getBroker();
    if (brokerCap.keySet().contains(broker)) {
      double amt = msg.getCharge();
      brokerCap.put(broker, brokerCap.get(broker) + amt);
    }
  }

  public void handleMessage (BankTransaction msg)
  {
    initMaybe();
    // contains interest amt
    Broker broker = msg.getBroker();
    if (brokerBank.keySet().contains(broker)) {
      double amt = msg.getAmount();
      brokerBank.put(broker, brokerBank.get(broker) + amt);
    }
  }
}
