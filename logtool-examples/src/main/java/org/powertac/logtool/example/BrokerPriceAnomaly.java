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
 * Logtool Analyzer that reads Order and MarketTransaction instances as they
 * arrive and looks for cases where the sign of the quantity matches the sign
 * of the money. The output data file has one line for each anomaly identifying
 * the message type and ID, the broker, the quantity, and the quoted price.
 * 
 * Usage: BrokerPriceAnomaly state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class BrokerPriceAnomaly
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(BrokerPriceAnomaly.class.getName());

  // Data
  private boolean started = false;
  private PrintWriter output = null;
  private String dataFilename = "broker-price-anomaly.data";
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerPriceAnomaly().cli(args);
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
    try {
      output = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
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
  }

  // -----------------------------------
  // catch MarketTransaction messages
  public void handleMessage (MarketTransaction tx)
  {
    if (!started)
      return;
    if (Math.signum(tx.getMWh()) == Math.signum(tx.getPrice())) {
      output.printf("mtx: %d, %s %s %.4f at %.4f\n",
                    tx.getId(), tx.getBroker().getUsername(),
                    tx.getMWh() < 0.0? "sells": "buys",
                    tx.getMWh(), tx.getPrice());
    }
  }

  // catch Order events to look for anomalous orders
  public void handleMessage (Order order)
  {
    if (!started)
      return;
    if (order.getLimitPrice() != null &&
        Math.signum(order.getMWh()) == Math.signum(order.getLimitPrice())) {
      output.printf("order %d from %s, %.4f MWh at %.4f\n",
                    order.getId(), order.getBroker().getUsername(),
                    order.getMWh(), order.getLimitPrice());
    }
  }
}
