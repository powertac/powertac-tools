/*
 * Copyright (c) 2012,2017 by John Collins
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

import org.powertac.common.BalancingTransaction;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.util.Pair;

/**
 * Example analysis class.
 * Computes total and per-broker imbalance and costs, and per-broker contributions
 * to imbalance.
 * 
 * Summary data is printed to System.out in a readable format,
 * and a data file with one row/timeslot. A header row lists the brokers.
 * Subsequent rows show imbalance for each broker, followed by overall
 * imbalance (from balancing transactions) and overall consumption (from
 * tariff transactions).
 * 
 * The data file contains per-timeslot imbalance for each broker, along with
 * aggregate imbalance and overall consumption.
 * 
 * @author John Collins
 */
public class ImbalanceStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(ImbalanceStats.class.getName());

  private BrokerRepo brokerRepo;

  // list of BalancingTransactions for current timeslot
  private HashMap<Broker, BalancingTransaction> btx;
  private HashMap<Broker, ArrayList<TariffTransaction>> ttx;

  // daily total imbalances, indexed by timeslot
  private int timeslot = 0;
  private int tsIndex = 0; // actual timeslot index 
  private ArrayList<Double> dailyImbalance;

  // daily per-broker imbalance, cost
  private HashMap<Broker, ArrayList<Pair<Double, Double>>> dailyBrokerImbalance;

  // daily tariff transactions
  private HashMap<Broker, ArrayList<ArrayList<TariffTransaction>>> dailyTraffic;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;

  private Competition competition;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public ImbalanceStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new ImbalanceStats().cli(args);
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

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    brokerRepo = (BrokerRepo) getBean("brokerRepo");
    btx = new HashMap<Broker, BalancingTransaction>();
    ttx = new HashMap<Broker, ArrayList<TariffTransaction>>();
    dailyImbalance = new ArrayList<Double>();
    dailyBrokerImbalance = new HashMap<Broker, ArrayList<Pair<Double, Double>>>();
    dailyTraffic = new HashMap<Broker, ArrayList<ArrayList<TariffTransaction>>>();

    try {
      data = new PrintWriter(new File(dataFilename));
      dataInit = false;
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  @Override
  public void report ()
  {
    // compute RMS imbalance
    double sum = 0.0;
    double sumsq = 0.0;
    for (Double imbalance : dailyImbalance) {
      sum += imbalance;
      sumsq += imbalance * imbalance;
    }
    System.out.println("Game " + Competition.currentCompetition().getName()
                       + ", " + timeslot + " timeslots");
    System.out.println("Total imbalance = " + sum);
    System.out.println("RMS imbalance = "
                       + Math.sqrt(sumsq / dailyImbalance.size()));
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      reportBrokerImbalance(broker);
    }
  }
  
  // Reports individual broker imbalance stats
  // Results include RMS imbalance, average imbalance,
  // total imbalance cost, and mean contribution to total
  // imbalance
  private void reportBrokerImbalance (Broker broker)
  {
    double sumsq = 0.0;
    double imbalanceSum = 0.0;
    double deliveredSum = 0.0;
    double contributionSum = 0.0;
    double cost = 0.0;
    ArrayList<Pair<Double, Double>> brokerRecord =
            dailyBrokerImbalance.get(broker);
    ArrayList<ArrayList<TariffTransaction>> deliveries =
            dailyTraffic.get(broker);
    for (int i = 0; i < dailyImbalance.size(); i++) {
      double total = dailyImbalance.get(i);
      double individual = brokerRecord.get(i).car();
      sumsq += individual * individual;
      imbalanceSum += individual;
      double sgn = Math.signum(individual) * Math.signum(total);
      contributionSum += Math.abs(individual) * sgn;
      cost += brokerRecord.get(i).cdr();
      for (TariffTransaction tx : deliveries.get(i))
        deliveredSum += tx.getKWh();
    }
    int count = dailyImbalance.size();
    System.out.println("Broker " + broker.getUsername()
                       + "\n  RMS imbalance = " + Math.sqrt(sumsq / count)
                       + "\n  mean imbalance = " + imbalanceSum / count
                       + "\n  imbalance ratio = " + imbalanceSum / deliveredSum
                       + "\n  mean contribution = " + contributionSum / count
                       + "\n  mean cost = " + cost / count
                       + "(" + cost / imbalanceSum + "/kwh)");
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    // skip initial timeslot(s) without data, initialize data structures
    if (0 == btx.size() && 0 == dailyImbalance.size()) {
      initTxList();
      initData();
      for (Broker broker : brokerRepo.findRetailBrokers()) {
        dailyBrokerImbalance.put(broker,
                                 new ArrayList<Pair<Double, Double>>());
        dailyTraffic.put(broker,
                         new ArrayList<ArrayList<TariffTransaction>>());
      }
      return;
    }

    // iterate through the balancing and tariff transactions
    double totalImbalance = 0.0;
    double totalConsumption = 0.0;
    // start with game ID and timeslot
    data.printf("%s,%d", competition.getName(), tsIndex);
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      // capture summary data for printout
      double balancingQty = 0.0;
      double consumptionQty = 0.0;
      // balancing tx first
      BalancingTransaction bx = btx.get(broker);
      ArrayList<Pair<Double, Double>> entries = dailyBrokerImbalance.get(broker);
      if (null == bx) {
        // zero entries
        entries.add(new Pair<Double, Double>(0.0, 0.0));
        data.print("," + "0.0");
      }
      else {
        entries.add(new Pair<Double, Double>(bx.getKWh(), bx.getCharge()));
        balancingQty = bx.getKWh();
        data.print("," + balancingQty);
        totalImbalance += bx.getKWh();
      }
      // tariff tx next
      ArrayList<TariffTransaction> txs = ttx.get(broker);
      ArrayList<ArrayList<TariffTransaction>> dailyTxs = 
              dailyTraffic.get(broker);
      if (null == txs) {
        dailyTxs.add(new ArrayList<TariffTransaction>());
      }
      else {
        for (TariffTransaction consumption : txs)
          consumptionQty += consumption.getKWh();
        totalConsumption += consumptionQty;
        dailyTxs.add(txs);
        ttx.put(broker, null);
      }
      //log.info("ts " + timeslot + ", broker " + broker.getUsername()
      //         + ": consumption = " + consumptionQty
      //         + ", balance qty = " + balancingQty);
    }
    dailyImbalance.add(totalImbalance);
    data.println("," + totalImbalance + "," + totalConsumption);
    timeslot += 1;
    initTxList();
  }

  private void initTxList ()
  {
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      btx.put(broker, null);
      ArrayList<TariffTransaction> txList = ttx.get(broker);
      if (null == txList) {
        txList = new ArrayList<TariffTransaction>();
        ttx.put(broker, txList);
      }
      txList.clear();
    }
  }
  
  private void initData ()
  {
    if (dataInit || null == data)
      return;
    data.print("game-id,timeslot");
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      data.print("," + broker.getUsername());
    }
    data.println(",imbalance,consumption");
    dataInit = true;
  }

  // -------------------------------
  // catch BalancingTransactions
  public void handleMessage (BalancingTransaction tx)
  {
    btx.put(tx.getBroker(), tx);
  } 

  // -----------------------------------
  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    // only include consumption
    if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
      //|| tx.getTxType() == TariffTransaction.Type.PRODUCE) {
      ArrayList<TariffTransaction> txList = ttx.get(tx.getBroker());
      if (null == txList) {
        txList = new ArrayList<TariffTransaction>();
        ttx.put(tx.getBroker(), txList);
      }
      txList.add(tx);
    }
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate ts)
  {
    if (null == competition)
      competition = Competition.currentCompetition();
    tsIndex = ts.getFirstEnabled()
        - competition.getDeactivateTimeslotsAhead();
    summarizeTimeslot();
  }
}
