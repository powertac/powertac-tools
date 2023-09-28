/*
 * Copyright (c) 2023 by John E. Collins
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
import java.util.Map;
import java.time.ZonedDateTime;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.apache.logging.log4j.LogManager;
import org.powertac.common.CustomerInfo;
import org.powertac.common.Tariff;
import org.powertac.common.TariffSpecification;
import org.powertac.common.TariffTransaction;
import org.powertac.common.TimeService;
import org.powertac.common.TariffTransaction.Type;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.CustomerRepo;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers production, consumption, regulation, and cost data for a single customer in kWh.
 * Output is one row per timeslot:
 *   timeslot index, day of week, hour,
 *     followed by six fields for each tariff the customer has subscribed to:
 *       tariffId, production, consumption,
 *       production-consumption cost, regulation, and regulation cost.
 * 
 * NOTE: Numeric data is formatted using the US locale in order to avoid confusion over
 * the meaning of the comma character when used in other locales.
 *
 * Usage: CustomerPCR customer input output
 *
 * @author John Collins
 */
public class CustomerPCR
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(CustomerProductionConsumption.class.getName());

  // customer data
  private boolean single = false;
  private String customerName = "";
  private CustomerInfo customer = null;

  // data collectors for current timeslot, indexed by tariff Id
  private int timeslot;
  private Map<Long, Double> used;
  private Map<Long, Double> produced;
  private Map<Long, Double> pcCost;
  private Map<Long, Double> regulation;
  private Map<Long, Double> regCost;

  // stored transaction for the current timeslot that may be modified by regulation
  private Map<Long, TariffTransaction> currentTx;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean started = false; // wait for SimStart

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public CustomerPCR ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CustomerPCR().cli(args);
  }
  
  /**
   * Takes three args: customer name, input filename, and output filename.
   */
  private void cli (String[] args)
  {
    int offset = 0;
    if (args.length != 3) {
      System.out.println("Usage: <analyzer> customer input-file output-file");
      return;
    }
    customerName = args[0];
    dataFilename = args[2];
    super.cli(args[1], this);
  }

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    currentTx = new HashMap<>();
    used = new HashMap<>();
    produced = new HashMap<>();
    pcCost = new HashMap<>();
    regulation = new HashMap<>();
    regCost = new HashMap<>();
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void report ()
  {
    data.close();
  }

  // called on sim start
  private void initData ()
  {
    CustomerRepo repo = (CustomerRepo) this.getBean("customerRepo");
    List<CustomerInfo> customers = repo.findByName(customerName);
    // for now, assume the first entry for the customer name is the correct one
    customer = customers.get(0);
    data.println("ts, dow, hod, tid, prod, cons, pc-cost, reg, reg-cost");
  }
  
  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot (Instant instant)
  {
    if (!started)
      return;

    // print timeslot, dow, hod, production, consumption
    ZonedDateTime dt = instant.atZone(ZoneId.of("UTC+00:00"));
    data.print(String.format("%d, %d, %d",
                             timeslot,
                             dt.getDayOfWeek().getValue(),
                             dt.getHour()));
    // print customer usage, production
    for (Long key : pcCost.keySet()) {
      data.print(String.format(", %d, %s, %s, %s, %s, %s", key,
                               df.format(getDouble(produced, key)),
                               df.format(getDouble(used, key)),
                               df.format(getDouble(pcCost, key)),
                               df.format(getDouble(regulation, key)),
                               df.format(getDouble(regCost, key))));
      
    }
    data.println();
  }

  private void clearCustomerData ()
  {
    produced.clear();
    used.clear();
    pcCost.clear();
    regulation.clear();
    regCost.clear();
    currentTx.clear();
  }

  private int skip = 1;
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    timeslot = msg.getFirstEnabled() - 1;
    if (started) {
      if (skip > 0) {
        skip -= 1;
      }
      else {
        // TODO: is this one hour too late?
        System.out.println("ts " + timeslot);
        summarizeTimeslot(java.time.Instant.ofEpochMilli(msg.getPostedTime().getMillis()));
      }
    }
    clearCustomerData();
  }

  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    if (tx.getCustomerInfo() != customer)
      return;
    if (! (tx.getTxType() == TariffTransaction.Type.CONSUME
            || tx.getTxType() == TariffTransaction.Type.PRODUCE)) {
      return;
    }
    Long key = tx.getTariffSpec().getId();
    if (! tx.isRegulation()) {
      // non-regulation transaction
      // if this is an update to an original tx, we need to update info
      TariffTransaction originalTx = currentTx.get(key); 
      if (null != originalTx) {
        System.out.println("Tx " + tx.getId() + " update");
        if (tx.getTxType() == Type.CONSUME) {
          used.put(key, getDouble(used, key) - originalTx.getKWh());
        }
        else if (tx.getTxType() == Type.PRODUCE) {
          produced.put(key, getDouble(produced, key) - originalTx.getKWh());
        }
        pcCost.put(key, getDouble(pcCost, key) - originalTx.getCharge());
      }
      else {
        currentTx.put(key, tx);
      }
      // accumulate kWh and cost
      if (tx.getTxType() == Type.CONSUME) {
        used.put(key, getDouble(used, key) + tx.getKWh());
      }
      else if (tx.getTxType() == Type.PRODUCE) {
        produced.put(key, getDouble(produced, key) + tx.getKWh());
      }
      pcCost.put(key, getDouble(pcCost, key) + tx.getCharge());
    }
    else {
      // regulation transaction
      if (!(tx.getTxType() == Type.CONSUME || tx.getTxType() == Type.PRODUCE))
        return;
      regulation.put(key, getDouble(regulation, key) + tx.getKWh());
      regCost.put(key, getDouble(regCost, key) + tx.getCharge());
    }
  }

  // Converts null Double values to 0.0
  private double getDouble(Map map, Long key)
  {
    Double result = (Double) map.get(key);
    if (null == result)
      result = 0.0;
    return result;
  }

  // catch SimStart and SimEnd messages
  public void handleMessage (SimStart ss)
  {
    System.out.println("Sim start");
    started = true;
    initData();
  }

  public void handleMessage (SimEnd se)
  {
    System.out.println("Sim end");
    report();
  }
}
