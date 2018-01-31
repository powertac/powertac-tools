/*
 * Copyright (c) 2015, 2017 by John E. Collins
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.powertac.balancemkt.ChargeInfo;
import org.powertac.balancemkt.SettlementContext;
import org.powertac.balancemkt.StaticSettlementProcessor;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.RegulationAccumulator;
import org.powertac.common.TariffSpecification;
import org.powertac.common.interfaces.CapacityControl;
import org.powertac.common.msg.BalancingOrder;
import org.powertac.common.msg.EconomicControlEvent;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TariffRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Example analysis class.
 * Gathers info on broker imbalance, broker balancing actions, and prices.
 * This requires input from both the state log and the trace log. Once the
 * data is gathered for a given timeslot, the server's VCG algorithm is run
 * to determine imbalance payments, and the tariffs determine the payments
 * to/from customers, giving broker profit/loss data, and the value of
 * regulation for customers and broker.
 * 
 * Reading from these files is synchronized by timeslot
 * 
 * Output is 1 (long) row/timeslot. The first six columns are
 *   ts;pPlus;pMinus;totalImbalance;rmBase;rmActual 
 * where baseCost is the rm cost in the absence of broker-provided balancing
 * capacity, and rmActual is the actual rm cost. The sign of totalImbalance
 * is negative for deficit, positive for surplus.
 * This is followed by per-broker data formatted as
 *   ;broker;(netLoad;regOffered;regUsed;baseCost;p1;p2) ...
 * where netLoad is the broker's individual imbalance, regOffered is the
 * amount of regulating capacity offered by the broker,
 * regUsed is the amount actually used, and baseCost is what the broker's
 * imbalance would have cost in the absence of exercised customer capacity.
 * 
 * @author John Collins
 */
public class BrokerBalancingActions
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(BrokerBalancingActions.class.getName());

  private BrokerRepo brokerRepo;
  private TariffRepo tariffRepo;
  private CapacityControlSvc capacityControl;
  private LocalSettlementContext settlementContext;
  private StaticSettlementProcessor settlementProcessor;

  // command-line options
  private Integer gameId = null;
  private String competitionId = null;

  // captured parameters
  private double balancingCost = 0.0;
  private double pPlusPrime = 0.0;
  private double pMinusPrime = 0.0;
  private double defaultSpotPrice = -50.0;

  // data collectors for current timeslot
  private int timeslot;

  // map tariff ID to balancing orders
  // TODO: this is an incomplete solution, since in general a tariff
  // could have any number of BalancingOrders, presumably with different
  // ratios and prices.
  private HashMap<TariffSpecification, BalancingOrder> balancingOrdersUp;
  private HashMap<TariffSpecification, BalancingOrder> balancingOrdersDown;

  // trace input file
  private String traceFilename;
  private BufferedReader trace;
  private int traceLineNumber = 0;

  // data output file
  private PrintWriter data = null;
  private String dataFilename;
  private boolean printHeader = false;

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public BrokerBalancingActions ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new BrokerBalancingActions().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename. The input filename
   * is the location of a state log; we expect to find the trace log in the
   * same location. 
   */
  private void cli (String[] args)
  {
    // set up command-line options
    OptionParser parser = new OptionParser();
    OptionSpec<Integer> gameIdOption = 
        parser.accepts("game").withRequiredArg().ofType(Integer.class);
    OptionSpec<String> competitionIdOption = 
        parser.accepts("competition").withRequiredArg().ofType(String.class);
    OptionSet options = parser.parse(args);
    gameId = options.valueOf(gameIdOption);
    competitionId = options.valueOf(competitionIdOption);
    String[] fileArgs = options.nonOptionArguments().toArray(new String[0]);
    if (fileArgs.length != 2) {
      System.out.println("Usage: <analyzer> [--game g] [--competition c] state-log output-file");
      return;
    }

    String stateFilename = fileArgs[0];
    int ext = stateFilename.indexOf(".state");
    if (-1 == ext) {
      System.out.println("Usage: first file arg must be a .state log");
      return;
    }
    traceFilename = stateFilename.replace(".state", ".trace");
    dataFilename = fileArgs[1];
    super.cli(fileArgs[0], this);
  }

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    brokerRepo = (BrokerRepo) getBean("brokerRepo");
    tariffRepo = (TariffRepo) getBean("tariffRepo");

    capacityControl = new CapacityControlSvc();
    settlementContext = new LocalSettlementContext();
    settlementProcessor = new StaticSettlementProcessor(null, capacityControl);

    balancingOrdersUp =
        new HashMap<TariffSpecification, BalancingOrder>();
    balancingOrdersDown =
        new HashMap<TariffSpecification, BalancingOrder>();

    try {
      trace = new BufferedReader (new FileReader(traceFilename));
    }
    catch (FileNotFoundException e) {
      System.out.println("Cannot open trace file " + traceFilename);
    }

    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      System.out.println("Cannot open data file " + dataFilename);
    }
    //dataInit = false;
  }

  @Override
  public void report ()
  {
    System.out.println("Game " + Competition.currentCompetition().getName()
                       + ", " + timeslot + " timeslots");
    //data.print("Summary, ");
    data.close();
  }

  // Called once to print column headers
  private void printHeader ()
  {
    printHeader = true;
    data.print("ts;pPlus;pMinus;totalImbalance;rmBase;rmActual");
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      data.format(";%s;netLoad;regOffered;regUsed;baseCost;p1;p2",
                  broker.getUsername());
    }
    data.println();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    if (timeslot == 874) {
      // breakpoint location
      log.info("timeslot " + timeslot);
    }
    TraceData traceData = readTraceData(timeslot);
    if (null == traceData) {
      // something went wrong
      log.error("Bad trace file");
      return;
    }
    double rmBase = 0.0;
    double imbalance = traceData.getTotalImbalance();
    HashMap<TariffSpecification, BalancingOrder> balancingOrders = null;
    if (imbalance < 0.0) {
      // up-regulation
      double price = traceData.getPPlus() - imbalance * pPlusPrime;
      rmBase = -imbalance * price;
      balancingOrders = balancingOrdersUp;
    }
    else {
      // down-regulation
      double price = traceData.getPMinus() - imbalance * pMinusPrime;
      rmBase = -imbalance * price;
      balancingOrders = balancingOrdersDown;
    }
    if (competitionId != null)
      data.format("%s;", competitionId);
    if (gameId != null)
      data.format("%d;", gameId);
    // ts pPlus pMinus ti rmBase rmActual
    data.format("%d;%.4f;%.4f;%.4f;%.4f;%.4f",
                             traceData.getTimeslot(),
                             traceData.getPPlus(),
                             traceData.getPMinus(),
                             traceData.getTotalImbalance(),
                             rmBase, traceData.getRmCost());
    capacityControl.setTraceData(traceData);
    settlementContext.setTraceData(traceData);
    List<ChargeInfo> brokerData = generateBrokerData(traceData);
    settlementProcessor.settle(settlementContext, brokerData);
    for (ChargeInfo bd: brokerData) {
      // compute offered regulation and base cost for this broker
      double offeredReg = 0.0;
      for (TariffSpecification spec: balancingOrders.keySet()) {
        if (spec.getBroker() == bd.getBroker()) {
          BalancingOrder order = balancingOrders.get(spec);
          RegulationAccumulator cap =
              traceData.getRegulationCapacity(order.getId());
          if (null == cap)
            continue;
          if (imbalance < 0.0) {
            // up-regulation
            offeredReg += cap.getUpRegulationCapacity();
          }
          else {
            offeredReg += cap.getDownRegulationCapacity();
          }
        }
      }
      // compute per-broker rm-base cost
      double brokerBase = -rmBase * bd.getNetLoadKWh() / imbalance;
      data.format(";%s;%.4f;%.4f;%.4f;%.4f;%.4f;%.4f", bd.getBrokerName(),
                  bd.getNetLoadKWh(),
                  offeredReg,
                  bd.getCurtailment(),
                  brokerBase,
                  bd.getBalanceChargeP1(),
                  bd.getBalanceChargeP2());
    }
    data.format("%n");
  }

  private List<ChargeInfo> generateBrokerData (TraceData traceData)
  {
    HashMap<Broker, ChargeInfo> chargeInfoMap = new HashMap<>();

    // code stolen from BalancingMarketService.balanceTimeslot()
    // create the ChargeInfo instances for each broker
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      double imbalance = traceData.getBrokerBalance(broker);
      ChargeInfo info = new ChargeInfo(broker, imbalance);
      //report.addImbalance(imbalance);
      chargeInfoMap.put(broker, info);
    }
    
    // retrieve and allocate the balancing orders
    ArrayList<BalancingOrder> boc = new ArrayList<BalancingOrder>();
    boc.addAll(balancingOrdersUp.values());
    boc.addAll(balancingOrdersDown.values());
    for (BalancingOrder order : boc) {
      ChargeInfo info = chargeInfoMap.get(order.getBroker());
      info.addBalancingOrder(order);
    }

    // gather up the list of ChargeInfo instances and settle
    log.info("balancing prices: pPlus=" + traceData.getPPlus()
             + ", pMinus=" + traceData.getPMinus());
    List<ChargeInfo> brokerData = new ArrayList<>();
    for (Broker broker : brokerRepo.findRetailBrokers()) {
      brokerData.add(chargeInfoMap.get(broker));
    }
    return brokerData;
  }

  // Regular expressions for trace file
  // first, some patterns to capture numbers and names:
  private String floatCapture = "(-?\\d+\\.\\d*)";
  private String intCapture = "(\\d+)";
  private String expCapture = "(-?\\d+\\.\\d*(?:E-?\\d+)?)";
  private String bnCapture = "(\\w(?:\\w| )*\\w+)"; // broker names
  private Pattern configQual =
      Pattern.compile(String.format("Configured BM: balancing cost = %s, \\(pPlus',pMinus'\\) = \\(%s,%s\\)",
                                    floatCapture, expCapture, expCapture));
  private Pattern tsQual =
      Pattern.compile(String.format("Deactivated timeslot %s", intCapture));
  private Pattern mbQual =
      Pattern.compile(String.format("BalancingMarketService: market balance for %s: %s",
                                    bnCapture, floatCapture));
  private Pattern tiQual =
      Pattern.compile(String.format("SettlementProcessor: totalImbalance=%s",
                                    floatCapture));
  private Pattern bpQual =
      Pattern.compile(String.format("balancing prices: pPlus=%s, pMinus=%s",
                                    floatCapture, floatCapture));
  private Pattern boQual =
      Pattern.compile(String.format("BalancingOrder %s capacity = \\(%s,%s\\)",
                                    intCapture, floatCapture, floatCapture));
  private Pattern duQual =
      Pattern.compile(String.format("SettlementProcessor: DU budget: rm cost = %s, broker cost = %s",
                                    floatCapture, floatCapture));
  private enum Scan {INIT, TIMESLOT, PRICE, BALANCE, CAPACITY, NEXT, END}
  private Scan scanState = Scan.INIT;

  // Reads data for a single timeslot from the trace file. For each timeslot,
  // we enter the TIMESLOT state and find the start message specified by tsQual,
  // making sure the timeslot matches the current timeslot in the state file.
  // Then we enter the PRICE state and pick up the individual broker
  // imbalance data with mbQual, terminated by the spot prices as specified
  // by the bpQual pattern. Then, in the BALANCE state, we pick off the
  // total imbalance for the timeslot using the tiQual pattern.
  // In the CAPACITY state, we use the boQual pattern to pick off the
  // 
  private TraceData readTraceData(int timeslot)
  {
    if (scanState == Scan.END) {
      // should not happen
      log.error("Attempt to read past end of trace file at ts " + timeslot);
      return null;
    }
    if (scanState == Scan.NEXT) {
      scanState = Scan.TIMESLOT;
    }
    TraceData collector = null;
    while (!(scanState == Scan.NEXT || scanState == Scan.END)) {
      try {
        String line = trace.readLine();
        traceLineNumber += 1;
        if (null == line) {
          log.info("Reached EOF in ts " + timeslot);
          scanState = Scan.END;
        }
        else if (scanState == Scan.INIT) {
          // capture balancing market initialization
          Matcher cf = configQual.matcher(line);
          if (cf.find()) {
            balancingCost = Double.parseDouble(cf.group(1));
            pPlusPrime = Double.parseDouble(cf.group(2));
            pMinusPrime = Double.parseDouble(cf.group(3));
            scanState = Scan.TIMESLOT;
          }
        }
        if (scanState == Scan.TIMESLOT) {
          Matcher m = tsQual.matcher(line);
          if (m.find()) {
            int ts = Integer.parseInt(m.group(1));
            if (ts == timeslot) {
              // found the target timeslot
              collector = new TraceData(ts);
              scanState = Scan.PRICE;
            }
          }
        }
        else if (scanState == Scan.PRICE) {
          // pick up individual broker imbalance numbers
          // once we see the pPlus/pMinus line, capture and switch to BALANCE
          Matcher mb = mbQual.matcher(line);
          if (mb.find()) {
            //pull out broker and balance info
            String brokerName = mb.group(1);
            double balance = Double.parseDouble(mb.group(2));
            log.info("ts " + timeslot + ": broker " + brokerName
                     + " imbalance=" + balance);
            collector.addBroker(brokerName, balance);
          }
          else {
            mb = bpQual.matcher(line);
            if (mb.find()) {
              double pPlus = Double.parseDouble(mb.group(1));
              double pMinus = Double.parseDouble(mb.group(2));
              log.info("ts " + timeslot + " spot prices ("
                       + pPlus + ", " + pMinus + ")");
              collector.setPrices(pPlus, pMinus);
              scanState = Scan.BALANCE;
            }
          }
        }
        else if (scanState == Scan.BALANCE) {
          Matcher mt = tiQual.matcher(line);
          if (mt.find()) {
            // here we find the total imbalance number
            double imbalance = Double.parseDouble(mt.group(1));
            collector.setTotalImbalance(imbalance);
            // end of timeslot
            scanState = Scan.CAPACITY;
          }
        }
        else if (scanState == Scan.CAPACITY) {
          // pick up BalancingOrder capacity values, terminated by the
          // static settlement summary
          Matcher bc = boQual.matcher(line);
          if (bc.find()) {
            long id = Long.parseLong(bc.group(1));
            double up = Double.parseDouble(bc.group(2));
            double down = Double.parseDouble(bc.group(3));
            log.info("ts " + timeslot + " add BO capacity ("
                     + id + ", " + up + ", " + down + ")");
            collector.addRegulationCapacity(id, up, down);
          }
          else {
            bc = duQual.matcher(line);
            if (bc.find()) {
              double value = Double.parseDouble(bc.group(1));
              collector.setRmCost(value);
              value = Double.parseDouble(bc.group(2));
              collector.setBrokerCost(value);
              scanState = Scan.NEXT;
            }
          }
        }
      }
      catch (IOException e) {
        log.error("tracefile error line " + traceLineNumber);
        e.printStackTrace();
      }
    }
    return collector;
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    if (!printHeader)
      printHeader();
    timeslot = msg.getFirstEnabled() - 1;
    if (timeslot <= 361) {
      // nothing interesting in the first two timeslots
      return;
    }
    log.info("Timeslot " + timeslot);
    summarizeTimeslot();
    //initTimeslotData();
  }

  // -------------------------------
  // catch BalancingOrders
  public void handleMessage (BalancingOrder order)
  {
    log.info("New balancing order for spec " + order.getTariffId()
             + ", price=" + order.getPrice());
    TariffSpecification spec =
        tariffRepo.findSpecificationById(order.getTariffId());
    if (order.getExerciseRatio() > 0.0) {
      // up-regulation
      balancingOrdersUp.put(spec, order);
    }
    else {
      balancingOrdersDown.put(spec, order);
    }
  }

  class TraceData
  {
    int timeslot = 0;
    HashMap<Broker, Double> brokerBalance =
        new HashMap<Broker, Double>();
    HashMap<Long, RegulationAccumulator> boCapacity =
        new HashMap<Long, RegulationAccumulator>(); 
    double pPlus = 0.0;
    double pMinus = 0.0;
    double totalImbalance = 0.0;
    double rmCost = 0.0;
    double brokerCost = 0.0;

    TraceData (int timeslot)
    {
      super();
      this.timeslot = timeslot;
    }

    int getTimeslot ()
    {
      return timeslot;
    }

    void addBroker (String brokerName, double imbalance)
    {
      brokerBalance.put(brokerRepo.findByUsername(brokerName), imbalance);
    }

    double getBrokerBalance (Broker broker)
    {
      return brokerBalance.get(broker);
    }

    void addRegulationCapacity (Long balancingOrder, double up, double down)
    {
      boCapacity.put(balancingOrder, new RegulationAccumulator(up, down));
    }

    RegulationAccumulator getRegulationCapacity (Long boId)
    {
      return boCapacity.get(boId);
    }

    void setPrices (double pPlus, double pMinus)
    {
      this.pPlus = pPlus;
      this.pMinus = pMinus;
    }

    double getPPlus ()
    {
      return pPlus;
    }

    double getPMinus ()
    {
      return pMinus;
    }

    void setTotalImbalance (double value)
    {
      totalImbalance = value;
    }

    double getTotalImbalance ()
    {
      return totalImbalance;
    }

    void setRmCost (double value)
    {
      rmCost = value;
    }

    double getRmCost ()
    {
      return rmCost;
    }

    void setBrokerCost (double value)
    {
      brokerCost = value;
    }

    double getBrokerCost ()
    {
      return brokerCost;
    }

    @Override
    public String toString ()
    {
      StringBuffer result = new StringBuffer();
      result.append("ts ").append(timeslot);
      result.append(String.format(" pPlus=%.4f pMinus=%.4f imbalance=%.4f, rmCost=%.4f, brokerCost=%.4f",
                                  pPlus, pMinus, totalImbalance, rmCost, brokerCost));
      for (Broker broker: brokerBalance.keySet()) {
        result.append(String.format(" %s=%.4f",
                                    broker.getUsername(),
                                    brokerBalance.get(broker)));
      }
      if (!boCapacity.isEmpty()) {
        for (Long id: boCapacity.keySet()) {
          RegulationAccumulator rc = boCapacity.get(id);
          result.append(String.format(" %d(%.4f,%.4f)",
                                      id, rc.getUpRegulationCapacity(),
                                      rc.getDownRegulationCapacity()));
        }
      }
      return result.toString();
    }
  }

  // ------------------------------------
  // Satisfy settlement processor API

  class LocalSettlementContext implements SettlementContext
  {
    TraceData traceData;

    void setTraceData (TraceData data)
    {
      traceData = data;
    }

    @Override
    public double getMarketBalance (Broker broker)
    {
      return traceData.getBrokerBalance(broker);
    }

    @Override
    public double getRegulation (Broker broker)
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getPPlusPrime ()
    {
      return pPlusPrime;
    }

    @Override
    public double getPMinusPrime ()
    {
      return pMinusPrime;
    }

    @Override
    public Double getBalancingCost ()
    {
      return balancingCost;
    }

    @Override
    public double getDefaultSpotPrice ()
    {
      return defaultSpotPrice;
    }

    @Override
    public double getPPlus ()
    {
      return traceData.getPPlus();
    }

    @Override
    public double getPMinus ()
    {
      return traceData.getPMinus();
    }
    
  }

  class CapacityControlSvc implements CapacityControl
  {
    TraceData traceData;

    void setTraceData (TraceData data)
    {
      traceData = data;
    }

    @Override
    public void exerciseBalancingControl (BalancingOrder order, double kwh,
                                          double payment)
    {
      // TODO Auto-generated method stub
      
    }

    @Override
    public RegulationAccumulator getRegulationCapacity (BalancingOrder order)
    {
      return traceData.getRegulationCapacity(order.getId());
    }

    @Override
    public void postEconomicControl (EconomicControlEvent event)
    {
      // TODO Auto-generated method stub
      
    }
    
  }
}
