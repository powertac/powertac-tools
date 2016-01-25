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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Orderbook;
import org.powertac.common.OrderbookOrder;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Computes total and per-broker net demand from tariff transactions, 
 * market cost, imbalance, and imbalance cost.
 * When broker imbalance is negative (shortage), computes price the broker
 * would have had to pay to purchase the shortage from the market in the
 * last timeslot.
 * 
 * Data format per timeslot, one line per broker:
 * gameid, timeslot, broker, netDemand, marketQty, marketCost, imbalance, balancingCost, MktImbalanceCost
 *
 * @author John Collins
 */
public class BrokerImbalanceCost
extends LogtoolContext
implements Analyzer
{
  static private Logger log = Logger.getLogger(BrokerImbalanceCost.class.getName());

  private DomainObjectReader dor;

  private BrokerRepo brokerRepo;

  // state and timeslot info
  private int timeslot = -1;
  private int tsOffset = 0;
  private Map<Broker, RingArray> rings;
  private Orderbook orderbook = null;
  private double totalImbalance = 0.0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  private Competition competition;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public BrokerImbalanceCost ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerImbalanceCost().cli(args);
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
    dor = (DomainObjectReader) getBean("reader");
    brokerRepo = (BrokerRepo) getBean("brokerRepo");
    dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    dor.registerNewObjectListener(new BalancingTxHandler(),
                                  BalancingTransaction.class);
    dor.registerNewObjectListener(new TariffTxHandler(),
                                  TariffTransaction.class);
    dor.registerNewObjectListener(new MarketTxHandler(),
                                  MarketTransaction.class);
    dor.registerNewObjectListener(new OrderbookHandler(),
                                  Orderbook.class);
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
    data.close();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    // Examine the orderbook to extract imbalance mkt price
    Double finalClearing = 0.0; // can be null temporarily
    if (null == orderbook) {
      log.error("No orderbook at ts " + timeslot);
    }
    else if (null == orderbook.getAsks() || 0 == orderbook.getAsks().size()) {
      log.error("No asks in orderbook at ts " + timeslot);
    }
    else if (totalImbalance < 0.0) {
      finalClearing = orderbook.getClearingPrice();
      if (null == finalClearing) {
        // no trades in this timeslot - use first ask instead
        for (OrderbookOrder order: orderbook.getAsks()) {
          Double price = order.getLimitPrice();
          if (null != price) {
            // skip market orders
            finalClearing = order.getLimitPrice();
            break;
          }
        }
      }
      finalClearing /= 1000.0; // convert to per-kWh
      Iterator<OrderbookOrder> asks = orderbook.getAsks().iterator();
      while (totalImbalance < 0.0) {
        if (!asks.hasNext()) {
          log.error(String.format("Ran out of asks at %.3f with %.3f kWh remaining",
                                  finalClearing, totalImbalance));
          break;
        }
        OrderbookOrder ask = asks.next();
        Double price = ask.getLimitPrice();
        if (null != price)
          // can be market order in the first position
          finalClearing = price / 1000.0;
        totalImbalance -= ask.getMWh() * 1000.0;
      }
    }
    else if (totalImbalance > 0.0) {
      // ignore case where total imbalance == 0.0
      finalClearing = orderbook.getClearingPrice();
      if (null == finalClearing) {
        // no trades in this timeslot - use first bid instead
        for (OrderbookOrder order: orderbook.getBids()) {
          Double price = order.getLimitPrice();
          if (null != price) {
            // skip market orders
            finalClearing = order.getLimitPrice();
            break;
          }
        }
      }
      finalClearing /= 1000.0; // convert to per-kWh
      Iterator<OrderbookOrder> bids = orderbook.getBids().iterator();
      while (totalImbalance > 0.0) {
        if (!bids.hasNext()) {
          log.error(String.format("Ran out of bids at %.3f with %.3f kWh remaining",
                                  finalClearing, totalImbalance));
          break;
        }
        OrderbookOrder bid = bids.next();
        Double price = bid.getLimitPrice();
        if (null != price)
          // can be market order in the first position
          finalClearing = price / 1000.0;
        totalImbalance -= bid.getMWh() * 1000.0;
      }
    }
    // iterate through the balancing and tariff transactions
    for (Broker broker: rings.keySet()) {
      BrokerData bt = rings.get(broker).get();
      double mktImbalance = 0.0;
      //if (bt.imbalance < 0.0)
        mktImbalance = finalClearing * bt.imbalance;
      data.printf("%s,%d,%s,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                  competition.getName(), timeslot,
                  broker.getUsername(),
                  bt.netDemand,
                  bt.marketQty, bt.marketCost,
                  bt.imbalance, bt.balancingCost,
                  mktImbalance);
    }
  }

  private void initData (int tsIndex)
  {
    data.println("game, timeslot, broker, netDemand, mktQty, mktCost, imbalance, imbalanceCost, mktImbCost");
    rings = new HashMap<>();
    for (Broker broker: brokerRepo.findRetailBrokers()) {
      RingArray ring =
          new RingArray(competition.getTimeslotsOpen()
                        + competition.getDeactivateTimeslotsAhead());
      rings.put(broker, ring);
    }
    System.out.println("first ts sn = " + tsIndex);
  }

  // Clear out the data for the timeslot just past
  private void newTimeslot ()
  {
    for (Broker broker: rings.keySet()) {
      rings.get(broker).clearCurrent();
    }
    orderbook = null;
    totalImbalance = 0.0;
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  class TimeslotUpdateHandler implements NewObjectListener
  {
    // first one is a dummy - use it to initialize
    @Override
    public void handleNewObject (Object thing)
    {
      if (null == competition)
        competition = Competition.currentCompetition();
      TimeslotUpdate ts = (TimeslotUpdate) thing;
      int tsIndex = ts.getFirstEnabled()
          - competition.getDeactivateTimeslotsAhead();
      if (-1 == timeslot) {
        // first time through
        timeslot = tsIndex;
        tsOffset = tsIndex;
        initData(tsIndex);
      }
      else if (tsIndex > tsOffset) {
        summarizeTimeslot();
        newTimeslot();
        timeslot = tsIndex;
      }
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
      BrokerData bd = rings.get(tx.getBroker()).get();
      bd.imbalance = tx.getKWh();
      totalImbalance += tx.getKWh();
      bd.balancingCost = tx.getCharge();
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
      if (tx.getTxType() == TariffTransaction.Type.CONSUME
          || tx.getTxType() == TariffTransaction.Type.PRODUCE) {
        rings.get(tx.getBroker()).get().addDemand(tx.getKWh());
      }
    } 
  }

  // -----------------------------------------
  // market transactions tell us what the brokers are buying and how much
  // they are paying.
  class MarketTxHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      MarketTransaction tx = (MarketTransaction)thing;
      RingArray ring = rings.get(tx.getBroker());
      if (null != ring) {
        // might not be a retail broker
        ring.addMarketTx(tx.getTimeslotIndex(),
                         tx.getMWh(), tx.getPrice());
      }
    }
  }

  // --------------------------------------------
  // Orderbooks tell us what the prices would have been for additional energy.
  // We just have to capture the one for the current timeslot
  class OrderbookHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      Orderbook book = (Orderbook) thing;
      if (book.getTimeslotIndex() == timeslot) {
        orderbook = book;
      }
    }
  }

  // data holder
  class BrokerData
  {
    int timeslot = -1;
    double netDemand;
    double imbalance;
    double balancingCost;
    double marketQty;
    double marketCost;

    BrokerData ()
    {
      super();
    }

    void clear ()
    {
      timeslot = -1;
      netDemand = 0.0;
      imbalance = 0.0;
      balancingCost = 0.0;
      marketQty = 0.0;
      marketCost = 0.0;
    }

    // Updates net demand
    void addDemand (double kWh)
    {
      netDemand += kWh;
    }

    // Updates market data
    void addMarketTx (double mWh, double price)
    {
      marketQty += mWh * 1000.0;
      marketCost += price * mWh;
    }
  }

  // rings array
  class RingArray
  {
    private ArrayList<BrokerData> data;
    private int size = 0;
    //private int index = 0;

    // Creates and populates a ring
    RingArray (int size)
    {
      super();
      this.size = size;
      data = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        data.add(i, new BrokerData());
      }
    }

    // Returns the current data holder
    BrokerData get ()
    {
      return data.get((timeslot - tsOffset) % size);
    }

    // Returns the data holder for a future timeslot
    BrokerData get (int timeslot)
    {
      return data.get((timeslot - tsOffset) % size);
    }

    // Clears out the current data holder after processing, making it
    // the new end element of the array
    void clearCurrent ()
    {
      get().clear();
    }

    // Updates market data for a future timeslot
    void addMarketTx (int timeslot, double mWh, double price)
    {
      get(timeslot).addMarketTx(mWh, price);
    }
  }
}
