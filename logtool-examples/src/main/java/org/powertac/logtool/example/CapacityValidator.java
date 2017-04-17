/*
 * Copyright (c) 2016. 2017 by John E. Collins
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
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.powertac.common.Broker;
import org.powertac.common.repo.BootstrapDataRepo;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.CapacityTransaction;
import org.powertac.common.Competition;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.CustomerBootstrapData;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers customer production and consumption data,
 * along with capacity transactions. Computes difference,
 * by broker, between recorded CapacityTransactions and
 * re-computed CapacityTransactions.
 * 
 * Output is one line/broker, formatted as
 *   gameId, broker-name, capacity charge variance
 * where the variance is the amount that should be added to the broker's score.
 * 
 * To run with maven, use
 *   mvn exec:exec -Dexec.args="org.powertac.logtool.example.CapacityValidator dir data"
 * where dir is a directory containing the boot record and state log
 * for a single game, and data is the name of the output data file
 *
 * @author John Collins
 */
public class CapacityValidator
extends LogtoolContext
implements Analyzer
{
  static Logger log = LogManager.getLogger(CapacityValidator.class.getSimpleName());

  //private TimeService timeService;
  private BootstrapDataRepo bootstrapRepo;
  //private TimeslotRepo timeslotRepo;
  private BrokerRepo brokerRepo;
  private URL bootLoc;
  //private Competition competition;

  // option flag
  private String gameId = null;

  // data collectors for current timeslot
  private int timeslot = 360;
  //private double used = 0.0;
  //private double produced = 0.0;
  private HashMap<Broker, Double> brokerUsed;
  private HashMap<Broker, Double> brokerProduced;
  private HashMap<Broker, List<CapacityTransaction>> brokerCapacityTx;
  private HashMap<Broker, Double> brokerVariance;

  // Capacity accounting info from DistributionUtilityService
  //@ConfigurableValue(valueType = "Boolean",
  //        publish = true,
  //        description = "If true, DU should assess transmission capacity fees")
  //private boolean useCapacityFee = false;

  //@ConfigurableValue(valueType = "Integer",
  //        publish = true,
  //        description = "Assessment interval in hours")
  private int assessmentInterval = 168;
  private Integer timeslotOffset = null;

  //@ConfigurableValue(valueType = "Double",
  //        publish = true,
  //        description = "Std deviation coefficient (nu)")
  private double stdCoefficient = 1.2;

  //@ConfigurableValue (valueType = "Double",
  //        publish = true,
  //        description = "Per-point fee (lambda)")
  private double feePerPoint = -180.0;

  // peak-demand dataset
  private double[] netDemand;
  private HashMap<Broker, double[]> brokerNetDemand = null;
  private double runningMean = 0.0;
  private double runningVar = 0.0;
  private double runningSigma = 0.0;
  private int runningCount = 0;
  //private int lastAssessmentTimeslot = 0;


  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";
  private boolean dataInit = false;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public CapacityValidator ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CapacityValidator().cli(args);
  }
  
  /**
   * Takes two args, a directory containing the boot.xml and state log
   * files, and an output filename.
   */
  private void cli (String[] args)
  {
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> dir output-file");
      return;
    }
    dataFilename = args[1];
    Path dir = Paths.get(args[0]);
    // find the boot record
    Path bootPath = null;
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, "*boot.xml")) {
      for (Path entry: stream)
        bootPath = entry;
      bootLoc = new URL("file://" + bootPath.toString());
    }
    catch (IOException e) {
      System.out.println("Could not find boot record");
      e.printStackTrace();
    }

    // find the state file
    Path statefile = null;
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(dir, "powertac-sim*.state")) {
      for (Path entry: stream)
        statefile = entry;
    }
    catch (IOException e) {
      System.out.println("Could not find state file");
      e.printStackTrace();
    }
    if (null != bootPath && null != statefile) {
      
      super.cli(statefile.toString(), this);
    }
  }

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    // Read the boot record
    bootstrapRepo = (BootstrapDataRepo)getBean("bootstrapDataRepo");
    bootstrapRepo.readBootRecord(bootLoc);
    Competition bootCompetition =
        bootstrapRepo.getBootstrapCompetition();
    timeslot = bootCompetition.getBootstrapTimeslotCount() +
        bootCompetition.getBootstrapDiscardedTimeslots();

    //timeService = (TimeService)getBean("timeService");
    //timeslotRepo = (TimeslotRepo)getBean("timeslotRepo");
    brokerRepo = (BrokerRepo)getBean("brokerRepo");

    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    netDemand = new double[assessmentInterval];
    processBootstrapRecord();
    dataInit = false;
  }

  // copied from DistributionUtilityService
  private void processBootstrapRecord ()
  {
    List<Object> usage = bootstrapRepo.getData(CustomerBootstrapData.class);
    if (null == usage || 0 == usage.size()) {
      // boot session, ignore
      return;
    }
    // data contains usage array for each customer. Should be 14 days, 336 hrs
    double[] first = ((CustomerBootstrapData) usage.get(0)).getNetUsage();
    if (336 != first.length) {
      // note error but use it
      log.warn("First item in customer bootstrap data is {} hrs long",
               first.length);
    }
    // aggregate the usage numbers across all customers
    double[] result = new double[first.length];
    for (Object item: usage) {
      double[] data = ((CustomerBootstrapData) item).getNetUsage();
      if (data.length != first.length) {
        log.warn("Length inconsistency for record {}, length = {}",
                 ((CustomerBootstrapData) item).getCustomerName(),
                 data.length);
      }
      for (int i = 0; i < Math.min(first.length, data.length); i += 1) {
        result[i] -= data[i];
      }
    }
    // Initialize running mean, sigma
    for (int i = 0; i < result.length; i++) {
      updateStats(result[i]);
    }
    log.info("Bootstrap data: n = {}, mean = {}, sigma = {}",
             runningCount, runningMean, runningSigma);
  }

  // Runs the recurrence formula for computing mean, sigma
  private void updateStats (double netConsumption)
  {
    double lastM = runningMean;
    runningCount += 1;
    runningMean = lastM + (netConsumption - lastM) / runningCount;
    runningVar = runningVar +
        (netConsumption - lastM) * (netConsumption - runningMean);
    runningSigma = Math.sqrt(runningVar / (runningCount - 1.0));
  }

  @Override
  public void report ()
  {
    for (Broker broker: brokerRepo.findRetailBrokers()) {
      data.println(String.format("%s, %s, %.4f",
                                 gameId, broker.getUsername(),
                                 brokerVariance.get(broker)));
    }
    data.close();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    List<Broker> brokerList = brokerRepo.findRetailBrokers();
    if (!dataInit) {
      // first time through print header, set up data collectors
      //data.println("game, broker, variance");
      gameId = Competition.currentCompetition().getName();
      brokerUsed = new HashMap<>();
      brokerProduced = new HashMap<>();
      brokerCapacityTx = new HashMap<>();
      brokerNetDemand = new HashMap<>();
      brokerVariance = new HashMap<>();
      for (Broker broker: brokerList) {
        brokerUsed.put(broker, 0.0);
        brokerProduced.put(broker, 0.0);
        brokerCapacityTx.put(broker, new ArrayList<CapacityTransaction>());
        brokerNetDemand.put(broker, new double[assessmentInterval]);
        brokerVariance.put(broker, 0.0);
      }
      dataInit = true;
      return;
    }

    // adapted from DistributionUtility.assessCapacityFees()
    if (null == timeslotOffset) {
      timeslotOffset = timeslot;
      //lastAssessmentTimeslot = timeslot;
      log.info("Start timeslot {}, timeslotOffset = {}",
               timeslot, timeslotOffset);
    }
    //else if (timeslot == timeslotOffset) {
      // Nothing happens in ts 0
    //  return;
    //}
    else if (0 == (timeslot - timeslotOffset) % assessmentInterval) {
      // do the assessment
      log.info("Peak-demand assessment at timeslot {}", timeslot);
      HashMap<Broker, Double> recordedCharge = new HashMap<>();
      HashMap<Broker, Double> computedCharge = new HashMap<>();
      for (Broker broker: brokerList) {
        computedCharge.put(broker,  0.0);
        double recorded = 0.0;
        for (CapacityTransaction ctx: brokerCapacityTx.get(broker)) {
          recorded += ctx.getCharge();
          log.info("ctx: ts {}, peak-ts {}, broker {}, charge {}, threshold {}",
                   timeslot, ctx.getPeakTimeslot(), broker.getUsername(),
                   ctx.getCharge(), ctx.getThreshold());
        }
        recordedCharge.put(broker,  recorded);
      }
      // discover the over-threshold peaks in the netDemand array
      double threshold = runningMean + stdCoefficient * runningSigma;
      List<PeakEvent> peaks = new ArrayList<PeakEvent>();
      for (int i = 0; i < netDemand.length; i++) {
        if (netDemand[i] >= threshold) {
          // gather peak
          peaks.add(new PeakEvent(netDemand[i], i));
        }
      }
      log.info("{} peaks found above threshold {}", peaks.size(), threshold);
      if (peaks.size() > 0) {
        // sort the peak events and assess charges
        peaks.sort(null);
        for (PeakEvent peak: peaks.subList(0, Math.min(3, peaks.size()))) {
          double excess = peak.value - threshold;
          double charge = excess * feePerPoint;
          for (Broker broker: brokerList) {
            // charge for broker comes from broker_usage/peak.value
            double[] brokerDemand = brokerNetDemand.get(broker);
            double cost = charge * brokerDemand[peak.index]
                / netDemand[peak.index];
            //brokerCharge.put(broker, cost);
            computedCharge.put(broker,
                               cost + computedCharge.get(broker));
            //double brokerExcess = 
            //    excess * brokerDemand[peak.index]/ netDemand[peak.index];
            //accounting.addCapacityTransaction(broker,
            //                                  lastAssessmentTimeslot + peak.index,
            //                                  threshold, brokerExcess, cost);
          }
          if (log.isInfoEnabled()) {
            double pts = peak.value - threshold;
            StringBuilder sb =
                new StringBuilder(String.format("Peak at ts %d, pts=%.3f, charge=%.3f (",
                                                peak.index + timeslot - assessmentInterval,
                                                pts, charge));
            for (Broker broker: brokerList) {
              sb.append(String.format("%s:%.3f, ",
                                      broker.getUsername(),
                                      computedCharge.get(broker)));
            }
            sb.append(")");
            log.info(sb.toString());
          }
        }
      }
      // Record variance and dump to data output
      for (Broker broker: brokerList) {
        double variance =
            computedCharge.get(broker) - recordedCharge.get(broker);
        log.info("ts {} variance for broker {} = {}",
                 timeslot, broker.getUsername(), variance);
        //data.println(String.format("%s, %d, %s, %.4f",
        //                           gameId, timeslot,
        //                           broker.getUsername(),
        //                           variance));
        //data.flush();
        brokerVariance.put(broker, variance + brokerVariance.get(broker));
        brokerCapacityTx.get(broker).clear();
        brokerNetDemand.put(broker, new double[assessmentInterval]);
      }

      // record time of last assessment
      //lastAssessmentTimeslot = timeslot;
    }
    // keep track of demand peaks for next assessment
    recordNetDemand(timeslot, brokerList);

    // Reset for next timeslot
    for (Broker broker: brokerList) {
      brokerProduced.put(broker, 0.0);
      brokerUsed.put(broker, 0.0);
    }
    //produced = 0.0;
    //used = 0.0;
  }

  // Records hourly net demand, updates running stats
  private void recordNetDemand (int timeslot, List<Broker> brokerList)
  {
    int index = (timeslot - timeslotOffset) % assessmentInterval;
    double totalConsumption = 0.0;
    for (Broker broker: brokerList) {
      // pull up the netDemand array for this broker
      double[] brokerDemand = brokerNetDemand.get(broker);
      if (null == brokerDemand) {
        log.warn("Broker {} not in brokerNetDemand map", broker.getUsername());
        brokerDemand = new double[assessmentInterval];
        brokerNetDemand.put(broker, brokerDemand);
      }
      // update net demand for this ts
      double netConsumption =
          -(brokerProduced.get(broker) + brokerUsed.get(broker));
      brokerDemand[index] = netConsumption;
      totalConsumption += netConsumption;
    }
    if (0.0 == totalConsumption) {
      // skip this one
      return;
    }
    log.info("Total net consumption for ts {} = {}",
             timeslot, totalConsumption);
    netDemand[index] = totalConsumption;
    // Update running mean and var
    if (runningCount == 0) {
      // first time through, assume this is a boot session
      runningMean = totalConsumption;
      runningVar = 0.0;
      runningCount = 1;
    }
    else {
      // use recurrence formula to update mean, sigma
      updateStats(totalConsumption);
      log.info("Net demand k = {}, mean = {}, sigma = {}",
               runningCount, runningMean, runningSigma);
    }
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    summarizeTimeslot();
    timeslot = msg.getFirstEnabled() - 1;
    log.info("Timeslot " + timeslot);
  }

  // -----------------------------------
  // catch TariffTransactions
  public void handleMessage (TariffTransaction tx)
  {
    Broker broker = tx.getBroker();

    if (tx.getTxType() == TariffTransaction.Type.CONSUME) {
      brokerUsed.put(broker,
                     brokerUsed.get(broker) + tx.getKWh());
      //used += tx.getKWh();
    }
    else if (tx.getTxType() == TariffTransaction.Type.PRODUCE) {
      brokerProduced.put(broker,
                         brokerProduced.get(broker) + tx.getKWh());
      //produced += tx.getKWh();
    }
  }

  // -------------------------------------
  // catch CapacityTransactions
  public void handleMessage (CapacityTransaction tx)
  {
    Broker broker = tx.getBroker();
    brokerCapacityTx.get(broker).add(tx);
  }

  // --------------------------------------------------------
  // Sortable data structure for tracking peak-demand events
  class PeakEvent implements Comparable<PeakEvent>
  {
    double value = 0.0;
    int index = 0;

    PeakEvent (double val, int idx)
    {
      super();
      value = val;
      index = idx;
    }

    @Override
    public int compareTo (PeakEvent o)
    {
      if (this.value > o.value)
        return 1;
      else if (this.value < o.value)
        return -1;
      else
        // make comparison consistent with equals
        return this.index - o.index;
    }
  }
}
