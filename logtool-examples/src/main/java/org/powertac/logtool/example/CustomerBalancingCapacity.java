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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.CustomerInfo;
import org.powertac.common.RegulationCapacity;
import org.powertac.common.TariffSpecification;
import org.powertac.common.TariffSubscription;
import org.powertac.common.TariffTransaction;
import org.powertac.common.enumerations.PowerType;
import org.powertac.common.msg.BalanceReport;
import org.powertac.common.msg.BalancingOrder;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.TariffRepo;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers production, consumption, and balancing data in kWh for all customers,
 * or for a list of named customers, or for all customers of a given PowerType.
 * Output is one row per timeslot:
 *   ts, dow, hod, total production, total consumption, total imbalance,
 *   offered upreg, offered downreg, used upreg, used downreg
 *
 * Usage: CustomerBalancingCapacity
 *     [--customer-names n1,n2,...] ||
 *     [--power-type pt] ||
 *     [--with-bo]
 *     customer-name input output
 * where the options are filters on the data that's collected. Only one option
 * is allowed.
 *
 * @author John Collins
 */
public class CustomerBalancingCapacity
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(CustomerBalancingCapacity.class.getName());

  // need the tariff repo
  TariffRepo tariffRepo;

  // customer data
  //private String gameId = null;
  private List<String> customerNames = new ArrayList<>();
  private Set<CustomerInfo> namedCustomers = null;
  private PowerType powerType = null;
  private boolean withBO = false;
  private Set<Long> boTariffIds;

  // data collectors for current timeslot
  private int timeslot;
  private double consumed = 0.0;
  private double produced = 0.0;
  private double offerUp = 0.0;
  private double offerDown = 0.0;
  private double useUp = 0.0;
  private double useDown = 0.0;
  private double imbalance = 0.0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;
  private boolean started = false; // wait for SimStart

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public CustomerBalancingCapacity ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CustomerBalancingCapacity().cli(args);
  }
  
  /**
   * Takes at least two args, input filename and output filename.
   * The --power-type and customer-names options apply filters to data collection.
   */
  private void cli (String[] args)
  {
    int offset = 0;
    if (args.length == 4 && "--power-type".equals(args[0])) {
      powerType = PowerType.valueOf(args[1]);
      offset = 2;
    }
    else if (args.length == 3 && "--with-bo".equals(args[0])) {
      withBO = true;
      boTariffIds = new HashSet<>();
      offset = 1;
    }
    else if (args.length != 2) {
      System.out.println("Usage: <analyzer> [--customer-names n1,... || --power-type pt] input output");
      return;
    }
    dataFilename = args[offset + 1];
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
    tariffRepo = (TariffRepo) getBean("tariffRepo");
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
      data.println("slot, dow, hod, prod, cons, imb, offer-up, offer-down, use-up, use-down");
      dataInit = true;
      return;
    }

    // print timeslot, dow, hod,
    data.print(String.format("%d, %d, %d, ",
                             timeslot,
                             instant.get(DateTimeFieldType.dayOfWeek()),
                             instant.get(DateTimeFieldType.hourOfDay())));
    // print customer data
    data.println(String.format("%.3f, %.3f, %.3f, %.3f, %.3f, %.3f, %.3f",
                               produced, consumed, imbalance,
                               offerUp, offerDown, useUp, useDown));
    produced = 0.0;
    consumed = 0.0;
    imbalance = 0.0;
    offerUp = 0.0;
    offerDown = 0.0;
    useUp = 0.0;
    useDown = 0.0;
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
    // filter by powerType 
    if (null != powerType && tx.getTariffSpec().getPowerType() != powerType)
      return;

    if (withBO && !boTariffIds.contains(tx.getTariffSpec().getId()))
      return;

    if (!tx.isRegulation()) {
      // normal production/consumption
      if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
        consumed += tx.getKWh();
      }
      else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
        produced += tx.getKWh();
      }
    }
    else {
     // regulation 
      if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
        useDown += tx.getKWh();
      }
      else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
        useUp += tx.getKWh();
      }
    }
  }

  // RegulationCapacity is associated with TariffSubscriptions
  public void handleMessage (RegulationCapacity rc)
  {
    // filter by powerType
    if (null != powerType) {
      TariffSpecification spec =
              tariffRepo.findSpecificationById(rc.getSubscription().getTariffId());
      if (spec.getPowerType().canUse(powerType)) {
        return;
      }
    }

    offerUp += rc.getUpRegulationCapacity();
    offerDown += rc.getDownRegulationCapacity();
  }

  // total imbalance comes from BalanceReport
  public void handleMessage (BalanceReport br)
  {
    imbalance = br.getNetImbalance();
  }

  // catch BalancingOrders and keep track of which tariffs have them
  public void handleMessage (BalancingOrder bo)
  {
    if (!withBO)
      return;
    boTariffIds.add(bo.getTariffId());
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
    //if (customerName.equals(ci.getName())) {
    //  customer = ci;
    //}
  }
}
