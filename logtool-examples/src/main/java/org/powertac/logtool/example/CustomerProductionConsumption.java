/*
 * Copyright (c) 2018, 2019 by John E. Collins
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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.CustomerInfo;
import org.powertac.common.TariffTransaction;
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
 * Gathers production and consumption data for a single customer in kWh.
 * If the --single option is given, output is one row per timeslot:
 *   timeslot index, day of week, hour, total production, total consumption
 * Otherwise, output starts with a list of customer names, followed by one
 * row/timeslot giving the timeslot index followed by net (production - consumption)
 * for each customer.
 * 
 * NOTE: Numeric data is formatted using the US locale in order to avoid confusion over
 * the meaning of the comma character when used in other locales.
 *
 * Usage: CustomerProductionConsumption [-- single customer-name] input output
 *
 * @author John Collins
 */
public class CustomerProductionConsumption
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(CustomerProductionConsumption.class.getName());

  // customer data
  private boolean single = false;
  private String customerName = "";
  private CustomerInfo customer = null;

  // data collectors for current timeslot
  private int timeslot;
  private double used = 0.0;
  private double produced = 0.0;
  private List<CustomerInfo> customers;
  private Map<CustomerInfo, Double> customerNet;
  private Map<CustomerInfo, Double> customerCost;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean started = false; // wait for SimStart

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public CustomerProductionConsumption ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CustomerProductionConsumption().cli(args);
  }
  
  /**
   * Takes at least two args, input filename and output filename.
   * The --by-broker option changes the output format.
   */
  private void cli (String[] args)
  {
    int offset = 0;
    if (args.length < 2 || args.length == 3 || args.length > 4) {
      System.out.println("Usage: <analyzer> [--single customer-name] input-file output-file");
      return;
    }
    if (args.length == 4) {
      if (args[0].equals("--single")) {
        single = true;
        customerName = args[1];
        offset = 2;        
      }
      else {
        System.out.println("Usage: <analyzer> [--single customer-name] input-file output-file");
        return;
      }
    }
    dataFilename = args[1 + offset];
    super.cli(args[offset], this);
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
    if (single) {
      // one customer
      data.println("ts, dow, hod, prod, cons");
    }
    else {
      customers = new ArrayList<>();
      customerNet = new HashMap<>();
      customerCost = new HashMap<>();
      CustomerRepo repo = (CustomerRepo) this.getBean("customerRepo");
      for (CustomerInfo cust: repo.list()) {
        customers.add(cust);
        customerNet.put(cust, 0.0);
        customerCost.put(cust, 0.0);
      }
      for (CustomerInfo cust: customers) {
        data.format("{'name':%s,'type':%s,'pop':%d}, ",
                    cust.getName(), cust.getPowerType().toString(),
                    cust.getPopulation());
      }
      data.println();
    }
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

    if (single) {
      // print timeslot, dow, hod, production, consumption
      data.print(String.format("%d, %d, %d, ",
                               timeslot,
                               instant.get(DateTimeFieldType.dayOfWeek()),
                               instant.get(DateTimeFieldType.hourOfDay())));
      // print customer usage, production
      data.println(String.format("%s, %s", df.format(produced), df.format(used)));
      produced = 0.0;
      used = 0.0;
    }
    else {
      // iterate through the map
      for (CustomerInfo cust: customers) {
        data.format("{'name':%s,'net':%s,'cost':%s},",
                    cust.getName(),
                    df.format(customerNet.get(cust)),
                    df.format(customerCost.get(cust)));
      }
      data.println();
      clearCustomerData();
    }
  }

  private void clearCustomerData ()
  {
    customerNet.replaceAll((k, v) -> 0.0);
    customerCost.replaceAll((k, v) -> 0.0);
  }

  private int skip = 1;
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    if (started)
      if (skip > 0) {
        skip -= 1;
        clearCustomerData();
      }
      else
        summarizeTimeslot(msg.getPostedTime());
  }

  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    if (single) {
      if (tx.getCustomerInfo() != customer)
        return;
      if (tx.getTxType() == Type.CONSUME) {
        used += tx.getKWh();
      }
      else if (tx.getTxType() == Type.PRODUCE) {
        produced += tx.getKWh();
      }      
    }
    else {
      if (!(tx.getTxType() == Type.CONSUME || tx.getTxType() == Type.PRODUCE))
        return;
      CustomerInfo cust = tx.getCustomerInfo();
      customerNet.put(cust, customerNet.get(cust) + tx.getKWh());
      customerCost.put(cust, customerCost.get(cust) + tx.getCharge());
    }

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

  // find our customer
  public void handleMessage (CustomerInfo ci)
  {
    if (single && customerName.equals(ci.getName())) {
      customer = ci;
    }
  }
}
