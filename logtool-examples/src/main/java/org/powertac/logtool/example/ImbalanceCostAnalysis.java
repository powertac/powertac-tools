/**
 * Copyright (c) 2020 by John E. Collins
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
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.Competition;
import org.powertac.common.msg.BalanceReport;
import org.powertac.common.msg.MarketBootstrapData;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BootstrapDataRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * <p>
 * First reads the boot record, assuming it's the only xml file in the
 * directory above the log, to extract the market-bootstrap-data.
 * Then reads the state log, extracting the BalancingReport to see the size
 * and direction of the imbalance, and the BalancingTransactions to see who
 * paid what for how much. We assume the least-advantageous price to be the
 * clearing price we are interested in.</p>
 * <p>
 * Usage:<br/>
 * &nbsp;&nbsp; ImbalanceCostAnalysis --boot filename input output<br/>
 * where the boot option points to the boot record. Otherwise we assume
 * the boot record to be the only xml file in the directory above the
 * input file, since this is how tournament results are formatted.</p>
 * <p>
 * Output:<br/>
 * One line per timeslot, formatted as
 * &nbsp;&nbsp; ts; total imbalance; imbalance price; ratio
 * 
 * @author John Collins
 */
public class ImbalanceCostAnalysis
extends LogtoolContext
implements Analyzer
{
  static private Logger log =
          LogManager.getLogger(ImbalanceCostAnalysis.class.getName());

  // collected data
  //private BrokerRepo brokerRepo;
  private double meanBootPrice = 0.0;
  List<BalancingTransaction> balancingTxs;
  BalanceReport balanceReport;
  
  // current state
  private boolean started;
  private int timeslot;
  
  // boot filename if given
  private String logFilename;
  private boolean hasBootFilename = false;
  private String bootFilename;

  // data output file
  private String dataFilename = "data.txt";
  private PrintWriter data = null;

  /**
   * Constructor does nothing. main() must call setup()
   */
  public ImbalanceCostAnalysis ()
  {
    super();
  }

  /**
   * Main method creates an instance and uses it to process the cli.
   */
  public static void main (String[] args)
  {
    new ImbalanceCostAnalysis().cli(args);
  }

  /**
   * Two command-line args expected, input and output files
   */
  public void cli (String[] args)
  {
    int offset = 0;
    if (args.length == 4 && "--boot".equals(args[0])) {
      offset = 2;
      bootFilename = args[1];
      hasBootFilename = true;
    }
    else if (args.length != 2) {
      System.out.println("Usage: <analyzer> input output");
      return;
    }
    dataFilename = args[offset + 1];
    logFilename = args[offset];
    super.cli(args[offset], this);
  }

  /**
   * Creates data structures, reads the boot record, sets up the output file.
   */
  @Override
  public void setup () throws FileNotFoundException
  {
    // first make sure we can open the data file
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    // next -- find, open, and process the boot record
    if (!hasBootFilename) {
      Path bootPath = Paths.get(logFilename).getParent().getParent();
      //System.out.println("boot path = " + bootPath.toString());
      //System.exit(0);
      try (DirectoryStream<Path> stream =
              Files.newDirectoryStream(bootPath, "*.xml")) {
        for (Path entry: stream)
          bootPath = entry;
        bootFilename = bootPath.toString();
      }
      catch (IOException e) {
        System.out.println("Could not find boot record");
        e.printStackTrace();
        System.exit(1);
      }
    }
    processBootRecord(bootFilename);

    // finally, look up the broker repo and print the header to the output file
    //brokerRepo = (BrokerRepo)getBean("brokerRepo");
    balancingTxs = new ArrayList<>();
    data.println("ts; mbp; imb; imb-price; ratio");
    started = false;
  }

  private void processBootRecord (String filename)
  {
    BootstrapDataRepo bdr = (BootstrapDataRepo)getBean("bootstrapDataRepo");
    File file = new File(bootFilename);
    try {
      URL url = file.toURI().toURL();
      log.info("Boot record URL = {}", url);
      bdr.readBootRecord(url);
    }
    catch (MalformedURLException e) {
      e.printStackTrace();
      System.exit(1);
    }
    List<Object> data = bdr.getData(MarketBootstrapData.class);
    MarketBootstrapData mbd = (MarketBootstrapData)data.get(0);
    meanBootPrice = mbd.getMeanMarketPrice() / 1000.0; // convert to kWh
    log.info("Mean bootstrap market price = {}", meanBootPrice);
  }

  // per-timeslot reporting
  private void summarizeTimeslot ()
  {
    // get the BalancingTransactions on the "bad" side of the imbalance
    // and add up the cost/kWh
    double imbalanceSign = Math.signum(balanceReport.getNetImbalance());
    double totalKWH = 0.0;
    double totalCost = 0.0;
    for (BalancingTransaction bt : balancingTxs) {
      if (Math.signum(bt.getKWh()) == imbalanceSign) {
        totalKWH += bt.getKWh();
        totalCost += bt.getCharge();
      }
    }
    double perKWH = totalCost / totalKWH; // negative if broker pays

    // Now we compute the ratio, as done in TariffEvaluationHelper.
    // Note that the kWh in the BalanceReport is positive for surplus
    // (down-regulation>, negative for shortage (up-regulation).
    double ratio = 0.0;
    if (imbalanceSign == -1.0) {
      // up-regulation
      ratio = perKWH / (-1.0 * meanBootPrice); // should be positive
    }
    else {
      // down-regulation
      ratio = (perKWH - 2.0 * meanBootPrice) / (-1.0 * meanBootPrice);
    }
    // print the per-timeslot record
    data.format("%d; %.4f; %.4f; %.6f; %.5f\n",
                timeslot, meanBootPrice, balanceReport.getNetImbalance(),
                perKWH, ratio);
    // clean up for next timeslot
    balancingTxs.clear();
  }

  @Override
  public void report ()
  {
    data.close();
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
    //summarizeTimeslot();
  }


  // -----------------------------------
  // catch BalancingTransaction events
  public void handleMessage (BalancingTransaction bt)
  {
    balancingTxs.add(bt);
  }

  // -----------------------------------
  public void handleMessage (BalanceReport br)
  {
    balanceReport = br;
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
        //firstLine();
      }
    timeslot = tu.getFirstEnabled() -
        Competition.currentCompetition().getDeactivateTimeslotsAhead();
  }
}
