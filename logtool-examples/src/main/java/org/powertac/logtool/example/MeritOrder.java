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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.powertac.common.Competition;
import org.powertac.common.Order;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Extracts the full merit order and total demand in the wholesale market.
 * Merit order is extracted from the first trading timeslot for each timeslot
 * in the game. In other words, after the 24th timeslot, it will be the one
 * 24h in the future. The demand quantity and price are taken from the final
 * timeslot in which these numbers are non-null.
 * 
 * The output data file has a header line giving the game ID and pom ID,
 * followed by a pair of lines for each timeslot formatted as
 * Qty, timeslot, qty, qty, ...
 * Price, timeslot, price, price, ...
 *
 * @author John Collins
 */
public class MeritOrder
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(MeritOrder.class.getName());

  // state and timeslot info
  private int timeslot = -1;
  // merit order comes from initial offers
  private HashMap<Integer, TreeSet<OrderData>> initialOffers = new HashMap<>();
  private HashSet<Integer> initialCollecting = new HashSet<>();
  private boolean started = false;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  private Competition competition;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public MeritOrder ()
  {
    super();
  }

  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new MeritOrder().cli(args);
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

  // Called at the end of each "real" timeslot (the ones after SimStart).
  private void summarizeTimeslot ()
  {
    // initial offers
    TreeSet<OrderData> offers = initialOffers.get(timeslot);
    if (null == offers) {
      log.warn("null offers ts {}", timeslot);
    }
    else {
      // quantities
      data.printf("Qty, %d", timeslot);
      for (Iterator<OrderData> ods = offers.iterator();
          ods.hasNext();) {
        OrderData od = ods.next();
        data.printf(", %.4f", od.quantity);
      }
      // prices
      data.printf("\nPrice, %d", timeslot);
      for (Iterator<OrderData> ods = offers.iterator();
          ods.hasNext();) {
        OrderData od = ods.next();
        data.printf(", %.4f", od.price);
      }
      data.println();
    }
  }

  private void initData (int tsIndex)
  {
    data.printf("game %s, pom_id %s\n",
                competition.getName(), competition.getPomId());
    System.out.println("first ts sn = " + tsIndex);
  }

  // -----------------------------------
  // Catch the SimStart event to start things up.
  // This avoids seeing the first timeslot twice.
  public void handleMessage (SimStart ss)
  {
    competition = Competition.currentCompetition();
    started = true;
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate ts)
  {
    if (!started)
      return;
    initialCollecting.clear();
    int tsIndex = ts.getFirstEnabled()
        - competition.getDeactivateTimeslotsAhead();
    if (-1 == timeslot) {
      // first time through
      log.info("First ts {}, first enabled={}, last enabled={}",
               tsIndex, ts.getFirstEnabled(), ts.getLastEnabled());
      initData(tsIndex);
      timeslot = tsIndex;
      for (int i = ts.getFirstEnabled(); i <= ts.getLastEnabled(); i += 1)
        initialCollecting.add(i);
    }
    else {
      summarizeTimeslot(); // this summarizes the last timeslot
      timeslot = tsIndex;
      initialCollecting.add(ts.getLastEnabled());
    }
  }

  // --------------------------------------------
  // Orders with negative quantities give the full merit order when sorted
  // by price.
  public void handleMessage (Order order)
  {
    if (order.getMWh() >= 0.0)
      return;
    Integer index = order.getTimeslotIndex();
    if (initialCollecting.contains(index)) {
      TreeSet<OrderData> orders = initialOffers.get(index);
      if (null == orders) {
        orders = new TreeSet<OrderData>();
        initialOffers.put(index, orders);
      }
      orders.add(new OrderData(order.getMWh(), order.getLimitPrice()));
    }
  }

  // data holder
  class OrderData implements Comparable<Object>
  {
    double quantity;
    Double price;

    OrderData (double qty, Double price)
    {
      super();
      this.quantity = qty;
      this.price = price;
    }


    @Override
    public int compareTo(Object o) {
      if (!(o instanceof OrderData)) 
        return 1;
      OrderData other = (OrderData) o;
      if (this.price == null)
        if (other.price == null)
          return 0;
        else
          return -1;
      else if (other.price == null)
        return 1;
      else
        return (this.price == (other.price) ? 0 :
          (this.price < other.price ? -1 : 1));
    }
  }
}
