/*
 * Copyright (c) 2015 by John E. Collins
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.powertac.balancemkt.SettlementContext;
import org.powertac.balancemkt.StaticSettlementProcessor;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.Rate;
import org.powertac.common.RateCore;
import org.powertac.common.RegulationCapacity;
import org.powertac.common.RegulationRate;
import org.powertac.common.Tariff;
import org.powertac.common.TariffSpecification;
import org.powertac.common.interfaces.CapacityControl;
import org.powertac.common.msg.BalancingOrder;
import org.powertac.common.msg.EconomicControlEvent;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TariffRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
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
 * To gather this data, we run through a series of states in each timeslot:
 * 1. Wait for TimeslotUpdate
 * 2. Gather MarketTransaction instances until the first TariffTransaction
 *    shows up. Each MarketTransaction gives price and quantity for a
 *    commitment in the wholesale market.
 * 3. Gather TariffTransaction instances until a BalanceReport shows up.
 *    Each TariffTransaction of type PRODUCE or CONSUME gives local production
 *    and consumption numbers, and prices.
 * 4. Gather TariffTransaction instances produced by the balancing process
 *    until the first BalancingTransaction shows up. The TariffTransaction
 *    instances may be either PRODUCE for up-regulation or CONSUME for
 *    down-regulation. These numbers also modify the production and consumption
 *    data from state #3.
 * 5. Gather BalancingTransaction instances until a TimeslotUpdate arrives.
 *    These give total net imbalance (after the local balancing controls are
 *    applied) for each broker, and the cost to resolve it.
 * 
 * @author John Collins
 */
public class BrokerBalancingActions
extends LogtoolContext
implements Analyzer
{
  static private Logger log = Logger.getLogger(BrokerBalancingActions.class.getName());

  private DomainObjectReader dor;
  private BrokerRepo brokerRepo;
  private CapacityControlSvc capacityControl;
  private StaticSettlementProcessor settlementProcessor;

  // captured parameters
  private double balancingCost = 0.0;
  private double pPlusPrime = 0.0;
  private double pMinusPrime = 0.0;

  // data collectors for current timeslot
  private int timeslot;

  // summary data collectors
  private HashMap<Long, BalancingOrder> balancingOrders;

  // trace input file
  private String traceFilename;
  private BufferedReader trace;

  // data output file
  private PrintWriter data = null;
  private String dataFilename;
  private boolean dataInit = true;

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
    if (args.length != 2) {
      System.out.println("Usage: <analyzer> state-log output-file");
      return;
    }
    String stateFilename = args[0];
    int ext = stateFilename.indexOf(".state");
    if (-1 == ext) {
      System.out.println("Usage: first arg must be a .state log");
      return;
    }
    traceFilename = stateFilename.replace(".state", ".trace");
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
    dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
    brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");

    capacityControl = new CapacityControlSvc();
    settlementProcessor = new StaticSettlementProcessor(null, capacityControl);

    balancingOrders =
        new HashMap<Long, BalancingOrder>();

    dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                                  TimeslotUpdate.class);
    dor.registerNewObjectListener(new BalancingOrderHandler(),
                                  BalancingOrder.class);

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
    dataInit = false;
  }

  @Override
  public void report ()
  {
    System.out.println("Game " + Competition.currentCompetition().getName()
                       + ", " + timeslot + " timeslots");
    //data.print("Summary, ");
    data.close();
  }

  // Called on timeslotUpdate. Note that there are two of these before
  // the first "real" timeslot. Incoming tariffs are published at the end of
  // the second timeslot (the third call to this method), and so customer
  // consumption against non-default broker tariffs first occurs after
  // four calls.
  private void summarizeTimeslot ()
  {
    TraceData traceData = readTraceData(timeslot);
    data.println(traceData.toString());
    capacityControl.setTraceData(traceData);
    dataInit = true;
    return;
  }

  // Regular expressions for trace file
  // first, some patterns to capture numbers:
  private String floatCapture = "(-?\\d+\\.\\d*)";
  private String intCapture = "(//d+)";
  private String expCapture = "(-?\\d+\\.\\d*(?:E-?\\d+)?)";
  private Pattern configQual =
      Pattern.compile(String.format("Configured BM: balancing cost = %s, \\(pPlus',pMinus'\\) = \\(%s,%s\\)",
                                    floatCapture, expCapture, expCapture));
  private Pattern tsQual =
      Pattern.compile(String.format("Deactivated timeslot %s", intCapture));
  private Pattern mbQual =
      Pattern.compile(String.format("BalancingMarketService: market balance for (\\w+): %s",
                                    floatCapture));
  private Pattern tiQual =
      Pattern.compile(String.format("SettlementProcessor: totalImbalance=%s",
                                    floatCapture));
  private Pattern bpQual =
      Pattern.compile(String.format("balancing prices: pPlus=%s, pMinus=%s",
                                    floatCapture, floatCapture));
  private Pattern boQual =
      Pattern.compile(String.format("BalancingOrder %s capacity = \\(%s,%s\\)",
                                    intCapture, floatCapture, floatCapture));
  private Pattern ssQual =
      Pattern.compile("SettlementProcessor: DU static settlement");
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
            scanState = Scan.NEXT;
          }
        }
        else if (scanState == Scan.TIMESLOT) {
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
            collector.addRegulationCapacity(id, up, down);
          }
          else {
            bc = ssQual.matcher(line);
            if (bc.find()) {
              scanState = Scan.NEXT;
            }
          }
        }
      }
      catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return collector;
  }

  class TraceData
  {
    int timeslot = 0;
    HashMap<Broker, Double> brokerBalance =
        new HashMap<Broker, Double>();
    HashMap<Long, RegulationCapacity> boCapacity =
        new HashMap<Long, RegulationCapacity>(); 
    double pPlus = 0.0;
    double pMinus = 0.0;
    double totalImbalance = 0.0;

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
      boCapacity.put(balancingOrder, new RegulationCapacity(null, up, down));
    }

    RegulationCapacity getRegulationCapacity (Long boId)
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

    @Override
    public String toString ()
    {
      StringBuffer result = new StringBuffer();
      result.append("ts ").append(timeslot);
      result.append(String.format(" pPlus=%.4f pMinus=%.4f imbalance=%.4f",
                                  pPlus, pMinus, totalImbalance));
      for (Broker broker: brokerBalance.keySet()) {
        result.append(String.format(" %s=%.4f",
                                    broker.getUsername(),
                                    brokerBalance.get(broker)));
      }
      if (!boCapacity.isEmpty()) {
        for (Long id: boCapacity.keySet()) {
          RegulationCapacity rc = boCapacity.get(id);
          result.append(String.format(" %d(%.4f,%.4f)",
                                      id, rc.getUpRegulationCapacity(),
                                      rc.getDownRegulationCapacity()));
        }
      }
      return result.toString();
    }
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  class TimeslotUpdateHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      TimeslotUpdate msg = (TimeslotUpdate) thing;
      timeslot = msg.getFirstEnabled() - 1;
      if (timeslot <= 361) {
        // nothing interesting in the first two timeslots
        return;
      }
      log.info("Timeslot " + timeslot);
      summarizeTimeslot();
      //initTimeslotData();
    }
  }

  // -------------------------------
  // catch BalancingOrders
  class BalancingOrderHandler implements NewObjectListener
  {
    @Override
    public void handleNewObject (Object thing)
    {
      BalancingOrder order = (BalancingOrder) thing;
      log.info("New balancing order for spec " + order.getTariffId()
               + ", price=" + order.getPrice());
      balancingOrders.put(order.getId(), order);
    } 
  }

  class LocalSettlementContext implements SettlementContext
  {

    @Override
    public double getMarketBalance (Broker broker)
    {
      // TODO Auto-generated method stub
      return 0;
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
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getPMinusPrime ()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Double getBalancingCost ()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public double getDefaultSpotPrice ()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getPPlus ()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double getPMinus ()
    {
      // TODO Auto-generated method stub
      return 0;
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
    public RegulationCapacity getRegulationCapacity (BalancingOrder order)
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void postEconomicControl (EconomicControlEvent event)
    {
      // TODO Auto-generated method stub
      
    }
    
  }
}
