/*
 * Copyright (c) 2019 by John E. Collins
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
import org.joda.time.DateTimeFieldType;
import org.joda.time.Instant;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.Competition;
import org.powertac.common.RegulationCapacity;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.BalanceReport;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
//import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers information on potential, needed, and used demand response per timeslot.
 * For each timeslot, prints a record
 * ts,hod,dow,upreg-avail,upreg-used,downreg-avail,downreg-used,
 * total-imbalance,unsatisfied-imbalance,settlement-cost
 *
 * Usage: DemandResponseStats input output
 *
 * @author John Collins
 */
public class DemandResponseStats
extends LogtoolContext
implements Analyzer
{
  //static private Logger log = LogManager.getLogger(DemandResponseStats.class.getName());

  //private TimeslotRepo timeslotRepo;
  private Competition competition;

  // data collectors for current timeslot
  private int timeslot = 360; // first timeslot of sim session under tournament conditions
  private double upCapacity = 0.0;
  private double downCapacity = 0.0;
  private double upUsed = 0.0;
  private double downUsed = 0.0;
  private double imbalance = 0.0;
  private double settlementCost = 0.0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private int skip = 1;
  private boolean started = false; // wait for SimStart

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public DemandResponseStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new DemandResponseStats().cli(args);
  }
  
  /**
   * Takes at least two args, input filename and output filename.
   * The --by-broker option changes the output format.
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
      e.printStackTrace();
    }
    //timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
    competition = Competition.currentCompetition();
  }

  @Override
  public void report ()
  {
    data.close();
  }

  // called on sim start
  private void initData ()
  {
    data.println("ts,dow,hod,upa,upu,dna,dnu,imb,cost");
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
     // print timeslot,dow,hod,upcap,upuse,dncap,dnuse,imbalance,cost
      data.print(String.format("%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                               timeslot,
                               instant.get(DateTimeFieldType.dayOfWeek()),
                               instant.get(DateTimeFieldType.hourOfDay()),
                               upCapacity, upUsed,
                               downCapacity, downUsed,
                               imbalance, settlementCost));
      upCapacity = 0.0;
      upUsed = 0.0;
      downCapacity = 0.0;
      downUsed = 0.0;
      imbalance = 0.0;
      settlementCost = 0.0;
  }

  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    if (started)
      if (skip > 0)
        skip -= 1;
      else
        summarizeTimeslot(msg.getPostedTime());
        timeslot = msg.getFirstEnabled() - competition.getDeactivateTimeslotsAhead();
  }

  // catch RegulationCapacity messages
  public void handleMessage (RegulationCapacity rc)
  {
    upCapacity += rc.getUpRegulationCapacity();
    downCapacity += rc.getDownRegulationCapacity();
  }

  // catch ImbalanceReport messages
  public void handleMessage (BalanceReport ir)
  {
    imbalance = ir.getNetImbalance();
  }

  // catch regulation TariffTransactions
  public void handleMessage (TariffTransaction tt)
  {
    if (tt.isRegulation()) {
      double kwh = tt.getKWh();
      if (kwh < 0.0)
        downUsed += kwh;
      else
        upUsed += kwh;
    }
  }

  // get cost from BalancingTransactions
  public void handleMessage (BalancingTransaction bt)
  {
    settlementCost += bt.getCharge();
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
    //report();
  }
}
