/*
 * Copyright (c) 2018 by John E. Collins
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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.CustomerInfo;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers production and consumption data for a single customer in kWh.
 * Output is one row per timeslot:
 *   timeslot index, day of week, hour, total production, total consumption
 *
 * Usage: CustomerProductionConsumption customer-name input output
 *
 * @author John Collins
 */
public class CustomerProductionConsumption
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(CustomerProductionConsumption.class.getName());

  // customer data
  //private String gameId = null;
  private String customerName = "";
  private CustomerInfo customer = null;

  // data collectors for current timeslot
  private int timeslot;
  private double used = 0.0;
  private double produced = 0.0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;
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
    if (args.length < 3 || args.length > 3) {
      System.out.println("Usage: <analyzer> customer-name input-file output-file");
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
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    dataInit = false;
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
  private void summarizeTimeslot (Instant instant)
  {
    if (!started)
      return;

    if (!dataInit) {
      // first time through nothing to but print header
      data.println("slot, dow, hod, production, consumption");
      dataInit = true;
      return;
    }

    // print timeslot, dow, hod, production, consumption
    data.print(String.format("%d, %d, %d, ",
                             timeslot,
                             instant.get(DateTimeFieldType.dayOfWeek()),
                             instant.get(DateTimeFieldType.hourOfDay())));
    // print customer usage, production
    data.println(String.format("%.3f, %.3f", produced, used));
    produced = 0.0;
    used = 0.0;
  }

  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    timeslot = msg.getFirstEnabled() - 1;
    log.info("Timeslot " + timeslot);
    summarizeTimeslot(msg.getPostedTime());
  }

  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    if (tx.getCustomerInfo() != customer)
      return;

    if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
      used += tx.getKWh();
    }
    else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
      produced += tx.getKWh();
    }
  }

  // catch SimStart and SimEnd messages
  public void handleMessage (SimStart ss)
  {
    started = true;
  }

  public void handleMessage (SimEnd se)
  {
    report();
  }

  // find our customer
  public void handleMessage (CustomerInfo ci)
  {
    if (customerName.equals(ci.getName())) {
      customer = ci;
    }
  }
}
