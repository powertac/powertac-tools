/*
 * Copyright (c) 2017 by John Collins
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
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.BankTransaction;
import org.powertac.common.Broker;
import org.powertac.common.CapacityTransaction;
import org.powertac.common.CashPosition;
import org.powertac.common.Competition;
import org.powertac.common.DistributionTransaction;
import org.powertac.common.MarketTransaction;
import org.powertac.common.TariffTransaction;
import org.powertac.common.TariffTransaction.Type;
import org.powertac.common.msg.BalancingControlEvent;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Logtool Analyzer that reads various Transaction instances as they arrive,
 * sorting them by broker and target timeslot. The output data file has a header
 * line giving column names. The
 * remainder of the file has either one line/broker for each timeslot
 * (if the per-broker option is given) or one line per timeslot formatted as<br>
 * timeslot,day-of-week,hour-of-day,broker-name,<br>
 * followed by credit and debit amounts from transactions and other
 * interactions that affect a broker's cash account: <br>
 * <ul>
 * <li>TariffTransaction (status events)</li>
 * <li>TariffTransaction (produce/consume events)</li>
 * <li>MarketTransaction (aggregated by target timeslot)</li>
 * <li>BalancingTransaction</li>
 * <li>DistributionTransaction</li>
 * <li>CapacityTransaction</li>
 * <li>BalancingControlEvent</li>
 * <li>BankTransaction (interest payments)</li>
 * </ul>
 * followed by the broker's CashPosition. Line continues with the next broker
 * unless the per-broker option is given.
 * 
 * Usage: BrokerAccounting [--per-broker] state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class BrokerAccounting
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(BrokerAccounting.class.getName());

  // service references
  private BrokerRepo brokerRepo;
  private TimeslotRepo timeslotRepo;

  // Data
  private List<Broker> brokerList;
  private HashMap<Broker, TreeMap<Integer, ArrayList<MarketTransaction>>> data;
  private HashMap<Broker, BrokerData> brokerData;

  private boolean started = false;
  private boolean firstTx = false;
  private int timeslot = 0;
  private boolean perBroker = false;
  private PrintWriter output = null;
  private String dataFilename = "broker-accounting.data";
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerAccounting().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    int offset = 0;
    if (args.length == 3 && "--per-broker".equals(args[0])) {
      perBroker = true;
      offset = 1;
    }
    else if (args.length != 2) {
      System.out.println("Usage: org.powertac.logtool.example.BrokerAccounting [--per-broker] input-file output-file");
      return;
    }
    dataFilename = args[1 + offset];
    super.cli(args[offset], this);
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    brokerRepo = (BrokerRepo) getBean("brokerRepo");
    timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
    brokerList = new ArrayList<>();
    data = new HashMap<>();
    try {
      output = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  private void firstLine ()
  {
    brokerList = brokerRepo.findRetailBrokers();
    brokerData = new HashMap<>();
    for (Broker broker: brokerList) {
      brokerData.put(broker, new BrokerData());
    }
    if (perBroker) {
      output.println("ts,dow,hod,broker,ttx-sc,ttx-sd,ttx-uc,ttx-ud,mtx-c,mtx-d,btx-c,btx-d,dtx-c,dtx-d,ctx-c,ctx-d,bce-c,bce-d,bank-c,bank-d,cash");
    }
    else {
      output.print("ts,dow,hod");
      for (int i = 0; i < brokerList.size(); i++) {
        output.printf(",broker%d,ttx-sc,ttx-sd,ttx-uc,ttx-ud,mtx-c,mtx-d,btx-c,btx-d,dtx-c,dtx-d,ctx-c,ctx-d,bce-c,bce-d,bank-c,bank-d,cash",
                      i);
      }
      output.println();
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    output.close();
  }

  // Dump collected data to output. Format depends on perBroker setting.
  private void summarizeTimeslot ()
  {
    if (perBroker) {
      for (Broker broker: brokerList) {
        dumpTS();
        dumpData(broker);
        output.println();
      }
    }
    else {
      dumpTS();
      for (Broker broker: brokerList)
        dumpData(broker);
      output.println();
    }
  }

  private void dumpData (Broker broker)
  {
    output.format(",%s",broker.getUsername());
    BrokerData bd = brokerData.get(broker);
    // TariffTransaction, state and usage
    output.format(",%.4f,%.4f,%.4f,%.4f",
                  bd.ttxSC, bd.ttxSD, bd.ttxUC, bd.ttxUD);
    // Handle deferred market transactions for this timeslot
    TreeMap<Integer, ArrayList<MarketTransaction>> brokerTxMap =
        data.get(broker);
    double mtxD = 0.0;
    double mtxC = 0.0;
    if (null != brokerTxMap) {
      List<MarketTransaction> txList = brokerTxMap.get(timeslot);
      if (null != txList) {
        for (MarketTransaction tx: txList) {
          double money = Math.abs(tx.getMWh()) * tx.getPrice();
          if (money >= 0.0)
            mtxC += money;
          else
            mtxD += money;
        }
      }
    }
    output.format(",%.4f,%.4f", mtxC, mtxD);
    // balancing, distribution, capacity
    output.format(",%.4f,%.4f,%.4f,%.4f,%.4f,%.4f",
                  bd.btxC, bd.btxD, bd.dtxC, bd.dtxD, bd.ctxC, bd.ctxD);
    // balancing control, bank
    output.format(",%.4f,%.4f,%.4f,%.4f,%.4f",
                  bd.bceC, bd.bceD, bd.bankC, bd.bankD, bd.cash);
    bd.clear();
  }

  private void dumpTS ()
  {
    // print ts,dow,hod
    DateTime dt = timeslotRepo.getDateTimeForIndex(timeslot);
    output.format("%d,%d,%d", timeslot,
                  dt.get(DateTimeFieldType.dayOfWeek()),
                  dt.get(DateTimeFieldType.hourOfDay()));
  }

  // -----------------------------------
  // catch SimStart to start things off
  public void handleMessage (SimStart start)
  {
    System.out.println("SimStart");
    started = true;
  }

  // -----------------------------------
  // use SimEnd to print the last line
  public void handleMessage (SimEnd end)
  {
    System.out.println("SimEnd");
    summarizeTimeslot();
  }

  // -----------------------------------
  // catch TariffTransaction messages
  public void handleMessage (TariffTransaction tx)
  {
    Broker broker = tx.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = tx.getCharge();
    // separate state from produce/consume tx
    if (tx.getTxType() == Type.PRODUCE || tx.getTxType() == Type.CONSUME) {
      if (amount < 0.0)
        bd.ttxUD += amount;
      else
        bd.ttxUC += amount;
    }
    else {
      if (amount < 0.0)
        bd.ttxSD += amount;
      else
        bd.ttxSC += amount;
    }
  }

  // -----------------------------------
  // catch MarketTransaction messages
  public void handleMessage (MarketTransaction tx)
  {
    Broker broker = tx.getBroker();
    if (!brokerList.contains(broker))
      return;
    //System.out.printf("Market tx %s, ts %d, mwh %.4f, price %.4f\n",
    //                  tx.getBroker().getUsername(), tx.getTimeslotIndex(),
    //                  tx.getMWh(), tx.getPrice());
    Integer target = tx.getTimeslotIndex();
    TreeMap<Integer, ArrayList<MarketTransaction>> brokerData =
        data.get(broker);
    if (null == brokerData) {
      brokerData = new TreeMap<>();
      data.put(broker, brokerData);
    }
    ArrayList<MarketTransaction> list = brokerData.get(target);
    if (null == list) {
      list = new ArrayList<>();
      brokerData.put(target, list);
    }
    list.add(tx);
  }

  // -----------------------------------
  // catch BalancingTransaction events
  public void handleMessage (BalancingTransaction bt)
  {
    Broker broker = bt.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = bt.getCharge();
    if (amount < 0.0)
      bd.btxD += amount;
    else
      bd.btxC += amount;
  }

  // -----------------------------------
  // catch DistributionTransaction events
  public void handleMessage (DistributionTransaction dt)
  {
    Broker broker = dt.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = dt.getCharge();
    if (amount < 0.0)
      bd.dtxD += amount;
    else
      bd.dtxC += amount;
  }

  // -----------------------------------
  // catch DistributionTransaction events
  public void handleMessage (CapacityTransaction ct)
  {
    Broker broker = ct.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = ct.getCharge();
    if (amount < 0.0)
      bd.ctxD += amount;
    else
      bd.ctxC += amount;
  }

  // -----------------------------------
  // catch DistributionTransaction events
  public void handleMessage (BalancingControlEvent bce)
  {
    Broker broker = bce.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = bce.getPayment();
    if (amount < 0.0)
      bd.bceD += amount;
    else
      bd.bceC += amount;
  }

  // -----------------------------------
  // catch DistributionTransaction events
  public void handleMessage (BankTransaction bt)
  {
    Broker broker = bt.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    double amount = bt.getAmount();
    if (amount < 0.0)
      bd.bankD += amount;
    else
      bd.bankC += amount;
  }

  // -----------------------------------
  // catch CashPosition messages
  public void handleMessage (CashPosition cp)
  {
    Broker broker = cp.getBroker();
    if (!brokerList.contains(broker))
      return;
    BrokerData bd = brokerData.get(broker);
    bd.cash = cp.getBalance();
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  private int skip = 1; // Skip the first one
  public void handleMessage (TimeslotUpdate tu)
  {
    if (started)
      if (skip == 0) {
        summarizeTimeslot();
      }
      else {
        skip -= 1;
        firstLine();
      }
    timeslot = tu.getFirstEnabled() -
        Competition.currentCompetition().getDeactivateTimeslotsAhead();
  }

  class BrokerData
  {
    double ttxSC;
    double ttxSD;
    double ttxUC;
    double ttxUD;
    double btxC;
    double btxD;
    double dtxC; // should always be zero
    double dtxD;
    double ctxC;
    double ctxD;
    double bceC;
    double bceD;
    double bankC;
    double bankD;
    double cash;

    BrokerData ()
    {
      super();
    }

    // clean up data collectors
    void clear ()
    {
      ttxSC = 0.0;
      ttxSD = 0.0;
      ttxUC = 0.0;
      ttxUD = 0.0;
      btxC = 0.0;
      btxD = 0.0;
      dtxC = 0.0;
      dtxD = 0.0;
      ctxC = 0.0;
      ctxD = 0.0;
      bceC = 0.0;
      bceD = 0.0;
      bankC = 0.0;
      bankD = 0.0;
      cash = 0.0;
    }
  }
}
