/*
 * Copyright (c) 2015, 2017 by John Collins
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
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Computes total and per-broker imbalance and costs, and per-broker contributions
 * to imbalance.
 * 
 * Summary data is printed to System.out in a readable format,
 * and additional summary detail is written to the specified
 * output file in two sections. The first row summarizes game info as
 *  game-id,n_brokers,c_total,cr_total,p_total,pr_total,i_total,i_rms,ir_total
 * where n_brokers is the number of competing brokers (not including the
 * default broker), c_total and p_total are the total consumption and
 * production recorded by tariff transactions, cr_total and pr_total are
 * revenue (or cost) associated with consumption and production,
 * i_total is the total imbalance recorded by balancing transactions, 
 * i_rms is the rms imbalance, ir_total is the overall imbalance cost
 * paid by all brokers. Signs are from the viewpoint of the broker; positive
 * values represent incoming cash or energy.
 * 
 * The second section is per-broker summary information, formatted as one
 * line per broker
 *  broker-name,c_broker,cr_broker,p_broker,pr_broker,i_broker,i_rms-broker,ir_broker
 * where the fields are per-broker versions of the aggregate data.
 * 
 * @author John Collins
 */
public class ImbalanceSummary
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(ImbalanceSummary.class.getName());

  private BrokerRepo brokerRepo;

  // Transactions for current timeslot
  private HashMap<Broker, BalancingTransaction> btx = null;
  private HashMap<Broker, ArrayList<TariffTransaction>> ttx = null;

  // hourly data, indexed by timeslot
  private int timeslot = 0;
  private ArrayList<TimeslotData> aggregateData;
  private HashMap<Broker, ArrayList<TimeslotData>> hourlyData;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public ImbalanceSummary ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new ImbalanceSummary().cli(args);
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
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  @Override
  public void report ()
  {
    TimeslotData totals = new TimeslotData();
    for (TimeslotData tsData: aggregateData) {
      totals.add(tsData);
    }
    double rms = Math.sqrt(totals.imbalanceSumSq / aggregateData.size());
    System.out.println("Game " + Competition.currentCompetition().getName()
                       + ", " + timeslot + " timeslots");
    System.out.println("Total imbalance = " + totals.imbalance);
    System.out.println("RMS imbalance = " + rms);
    data.print(String.format("%s,%d,",
                             Competition.currentCompetition().getName(),
                             hourlyData.size() - 1));
    data.println(totals.formatWithRms(rms));

    for (Broker broker : brokerRepo.findRetailBrokers()) {
      reportByBroker(broker);
    }
    data.close();
  }

  // Reports individual broker imbalance stats
  // Results include RMS imbalance, average imbalance,
  // total imbalance cost, and mean contribution to total
  // imbalance
  private void reportByBroker (Broker broker)
  {
    TimeslotData total = new TimeslotData();
    ArrayList<TimeslotData> brokerData = hourlyData.get(broker);
    for (TimeslotData tsData: brokerData) {
      total.add(tsData);
    }
    double rms = Math.sqrt(total.imbalanceSumSq / brokerData.size());
    data.print(broker.getUsername() + ",");
    data.println(total.formatWithRms(rms));
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    // skip initial timeslot(s) without data, initialize data structures
    if (null == btx) {
      initData();
      initTxList();
      return;
    }

    // iterate through the balancing and tariff transactions for each broker
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      TimeslotData tsData = new TimeslotData();
      hourlyData.get(broker).add(tsData);
      TimeslotData aggregate = new TimeslotData();
      aggregateData.add(aggregate);
      // balancing tx first
      BalancingTransaction bx = btx.get(broker);
      if (null != bx) {
        tsData.imbalance = bx.getKWh();
        aggregate.imbalance += bx.getKWh();
        tsData.imbalanceCost = bx.getCharge();
        aggregate.imbalanceCost += bx.getCharge();
      }
      for (TariffTransaction tx: ttx.get(broker)) {
        if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
          tsData.consumption += tx.getKWh();
          aggregate.consumption += tx.getKWh();
          tsData.income += tx.getCharge();
          aggregate.income += tx.getCharge();
        }
        else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
          tsData.production += tx.getKWh();
          aggregate.production += tx.getKWh();
          tsData.expense += tx.getCharge();
          aggregate.expense += tx.getCharge();
        }
      }
    }
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
    btx = new HashMap<Broker, BalancingTransaction>();
    ttx = new HashMap<Broker, ArrayList<TariffTransaction>>();
    initTxList();
    hourlyData = new HashMap<Broker, ArrayList<TimeslotData>>();
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      hourlyData.put(broker, new ArrayList<TimeslotData>());
    }
    aggregateData = new ArrayList<TimeslotData>();
  }

  // -------------------------------
  // catch BalancingTransactions
  // We assume there is at most one balancing tx per broker in each timeslot.
  public void handleMessage (BalancingTransaction tx)
  {
    btx.put(tx.getBroker(), tx);
  } 

  // -----------------------------------
  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    // only include consumption
    if (tx.getTxType() == TariffTransaction.Type.CONSUME
        || tx.getTxType() == TariffTransaction.Type.PRODUCE) {
      ArrayList<TariffTransaction> txList = ttx.get(tx.getBroker());
      if (null == txList) {
        System.err.println("Error: null txList");
      }
      txList.add(tx);
    }
  } 

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate upd)
  {
    summarizeTimeslot();
  }

  // --------------------------------------
  // data holder
  class TimeslotData
  {
    double production = 0.0;
    double expense = 0.0;
    double consumption = 0.0;
    double income = 0.0;
    double imbalance = 0.0;
    double imbalanceSumSq = 0.0;
    double imbalanceCost = 0.0;

    TimeslotData ()
    {
      super();
    }

    // Aggregates another item with this one
    void add (TimeslotData data)
    {
      production += data.production;
      expense += data.expense;
      consumption += data.consumption;
      income += data.income;
      imbalance += data.imbalance;
      imbalanceSumSq += data.imbalance * data.imbalance;
      imbalanceCost += data.imbalanceCost;
    }

    public String formatWithRms (double rms)
    {
      return String.format("%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f",
                           consumption, income, production, expense,
                           imbalance, rms, imbalanceCost);
    }
  }
}
