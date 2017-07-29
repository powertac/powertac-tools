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
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Order;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Logtool Analyzer that reads MarketTransaction instances as they arrive,
 * sorting them by broker and target timeslot. The output data file has header
 * line giving column names, the last n of which are broker names. The
 * remainder of the file has one line/timeslot formatted as<br>
 * timeslot,day-of-week,hour-of-day,[mwh price],[mwh price] ...<br>
 * Each line has entries for each broker.
 * 
 * Usage: BrokerMktPrices state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class BrokerMktPrices
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(BrokerMktPrices.class.getName());

  // service references
  private BrokerRepo brokerRepo;
  private TimeslotRepo timeslotRepo;

  // Data
  private HashMap<Broker, TreeMap<Integer, ArrayList<MarketTransaction>>> data;
  private ArrayList<Broker> brokerList;

  private boolean started = false;
  private boolean firstTx = false;
  private int timeslot = 0;
  private PrintWriter output = null;
  private String dataFilename = "broker-market-price.data";
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerMktPrices().cli(args);
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
    output.print("ts, dow, hod");
    for (Broker broker: brokerRepo.findRetailBrokers()) {
      brokerList.add(broker);
      output.printf(", %s", broker.getUsername());
    }
    output.println();
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    output.close();
  }

  private void summarizeTimeslot ()
  {
    // print ts,dow,hod,
    DateTime dt = timeslotRepo.getDateTimeForIndex(timeslot);
    output.format("%d, %d, %d", timeslot,
                  dt.get(DateTimeFieldType.dayOfWeek()),
                  dt.get(DateTimeFieldType.hourOfDay()));
    for (Broker broker: brokerList) {
      double mwh = 0.0;
      double money = 0.0;
      double price = 0.0;
      TreeMap<Integer, ArrayList<MarketTransaction>> brokerData =
          data.get(broker);
      if (null != brokerData) {
        List<MarketTransaction> txList = brokerData.get(timeslot);
        if (null != txList) {
          for (MarketTransaction tx: txList) {
            double m = tx.getMWh();
            double p = tx.getPrice();
            mwh += m;
            money += Math.abs(m) * p;
          }
        }
      }
      if (0 != mwh) {
        price = money / Math.abs(mwh);
      }
      output.format(", [%.4f, %.4f]", mwh, price);
    }
     output.println();
  }

  // -----------------------------------
  // catch SimStart to start things off
  public void handleMessage (SimStart start)
  {
    System.out.println("SimStart");
    firstLine();
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
  // catch MarketTransaction messages
  public void handleMessage (MarketTransaction tx) {
    if (!started)
      return;
    firstTx = true;
    checkSignAnomaly(tx);
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

  // Print out transactions that seem to have backward prices (or quantities)
  private void checkSignAnomaly (MarketTransaction tx)
  {
    if (tx.getMWh() > 0.0 && tx.getPrice() > 0.0) {
      System.out.format("mtx+: %d, %s buys %.4f, %.4f\n",
                        tx.getId(), tx.getBroker().getUsername(),
                        tx.getMWh(), tx.getPrice());
    }
    else if (tx.getMWh() < 0.0 && tx.getPrice() < 0.0) {
      System.out.format("mtx-: %d, %s sells %.4f, %.4f\n",
                        tx.getId(), tx.getBroker().getUsername(),
                        tx.getMWh(), tx.getPrice());
    }
  }

  // catch Order events to look for anomalous orders
  public void handleMessage (Order order)
  {
    if (!started)
      return;
    if ((order.getMWh() > 0.0 &&
         order.getLimitPrice() != null && order.getLimitPrice() > 0.0) ||
        (order.getMWh() < 0.0 &&
         order.getLimitPrice() != null && order.getLimitPrice() < 0.0)) {
      System.out.format("order %d from %s, %.4f MWh at %.4f\n",
                        order.getId(), order.getBroker().getUsername(),
                        order.getMWh(), order.getLimitPrice());
    }
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate tu)
  {
    if (firstTx) {
      summarizeTimeslot();
    }
    timeslot = tu.getFirstEnabled() -
        Competition.currentCompetition().getDeactivateTimeslotsAhead();
  }
}
