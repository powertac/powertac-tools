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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.util.Pair;

/**
 * Example analysis class.
 * Computes total and per-broker imbalance and costs, and per-broker contributions
 * to imbalance.
 * 
 * @author John Collins
 */
public class ImbalanceStats
implements Analyzer
{
  static private Logger log = Logger.getLogger(ImbalanceStats.class.getName());

  private DomainObjectReader dor;

  private BrokerRepo brokerRepo;

  // list of BalancingTransactions for current timeslot
  private HashMap<Broker, BalancingTransaction> btx;
  private HashMap<Broker, ArrayList<TariffTransaction>> ttx;

  // daily total imbalances, indexed by timeslot
  private int timeslot = 0;
  private ArrayList<Double> dailyImbalance;

  // daily per-broker imbalance, cost
  private HashMap<Broker, ArrayList<Pair<Double, Double>>> dailyBrokerImbalance;

  // daily tariff transactions
  private HashMap<Broker, ArrayList<ArrayList<TariffTransaction>>> dailyTraffic;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public ImbalanceStats ()
  {
    super();
  }

  @Override
  public void setup ()
  {
    dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
    brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");
    btx = new HashMap<Broker, BalancingTransaction>();
    ttx = new HashMap<Broker, ArrayList<TariffTransaction>>();
    dailyImbalance = new ArrayList<Double>();
    dailyBrokerImbalance = new HashMap<Broker, ArrayList<Pair<Double, Double>>>();
    dailyTraffic = new HashMap<Broker, ArrayList<ArrayList<TariffTransaction>>>();

    dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    dor.registerNewObjectListener(new BalancingTxHandler(),
                                  BalancingTransaction.class);
    dor.registerNewObjectListener(new TariffTxHandler(),
                                  TariffTransaction.class);
  }

  @Override
  public void report ()
  {
    // compute RMS imbalance
    double sumsq = 0.0;
    for (Double imbalance : dailyImbalance) {
      sumsq += imbalance * imbalance;
    }
    System.out.println("Game " + Competition.currentCompetition().getName()
                       + ", " + timeslot + " timeslots");
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
    double imbalanceRatioSum = 0.0;
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
      double delivered = 0.0;
      for (TariffTransaction tx : deliveries.get(i))
        delivered += tx.getKWh();
      if (individual != 0.0) {
        if (delivered == 0.0)
          log.error("individual = " + individual + ", delivered = 0.0");
        else
          imbalanceRatioSum += individual / delivered;
      }
    }
    int count = dailyImbalance.size();
    System.out.println("Broker " + broker.getUsername()
                       + "\n  RMS imbalance = " + Math.sqrt(sumsq / count)
                       + "\n  mean imbalance = " + imbalanceSum / count
                       + "\n  mean imbalance ratio = " + imbalanceRatioSum / count
                       + "\n  mean contribution = " + contributionSum / count
                       + "\n  mean cost = " + cost / count
                       + "(" + cost / imbalanceSum + "/kwh)");
  }

  // process daily balancing tx
  private void summarizeTimeslot ()
  {
    // skip initial timeslot(s) without data, initialize data structures
    if (0 == btx.size() && 0 == dailyImbalance.size()) {
      initTxList();
      for (Broker broker : brokerRepo.findRetailBrokers()) {
        dailyBrokerImbalance.put(broker,
                                 new ArrayList<Pair<Double, Double>>());
        dailyTraffic.put(broker,
                         new ArrayList<ArrayList<TariffTransaction>>());
      }
      return;
    }

    // iterate through the balancing and tariff transactions
    double total = 0.0;
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      // balancing tx first
      BalancingTransaction bx = btx.get(broker);
      ArrayList<Pair<Double, Double>> entries = dailyBrokerImbalance.get(broker);
      if (null == bx) {
        // zero entries
        entries.add(new Pair<Double, Double>(0.0, 0.0));
      }
      else {
        entries.add(new Pair<Double, Double>(bx.getKWh(), bx.getCharge()));
        total += bx.getKWh();
      }
      // tariff tx next
      ArrayList<TariffTransaction> txs = ttx.get(broker);
      ArrayList<ArrayList<TariffTransaction>> dailyTxs = 
              dailyTraffic.get(broker);
      if (null == txs) {
        dailyTxs.add(new ArrayList<TariffTransaction>());
      }
      else {
        dailyTxs.add(txs);
        ttx.put(broker, null);
      }
    }
    dailyImbalance.add(total);
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

  // -------------------------------
  // catch BalancingTransactions
  class BalancingTxHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      BalancingTransaction tx = (BalancingTransaction)thing;
      btx.put(tx.getBroker(), tx);
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
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  class TimeslotUpdateHandler implements NewObjectListener
  {

    @Override
    public void handleNewObject (Object thing)
    {
      summarizeTimeslot();
    }
  }
}
