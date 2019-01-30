/*
 * Copyright (c) 2019 by John Collins
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
import org.joda.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.Rate;
import org.powertac.common.RegulationRate;
import org.powertac.common.Tariff;
import org.powertac.common.TariffSpecification;
import org.powertac.common.TariffTransaction;
import org.powertac.common.TariffTransaction.Type;
import org.powertac.common.TimeService;
import org.powertac.common.msg.SimEnd;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TariffRepo;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Logtool Analyzer that extracts tariffs and all associated data from a game log.
 * Output depends on whether the --narrative option is specified. In narrative mode,
 * the output is a sequence of events, including tariff publication, subscription
 * changes, and daily summaries of per-tariff earnings.
 * 
 * Without the --narrative option, output is in three parts. Part 1 is a list of
 * tariffs in the order they appear in the log. For each tariff, fields are:
 *   ID, PowerType, Broker, intro TS, minDuration, signup, withdraw, periodic pmt,
 *       rate-info, up-reg price, down-reg price.
 * where rate-info is a single number for a fixed rate, an array for daily TOU
 * rate, a dict for a tiered rate.
 * 
 * The second part is one row/timeslot, one column/tariff. Each cell is four numbers,
 *   [subscription count, tariff-payment, rate-payment, regulation-payment]
 * 
 * The third part is a summary row with one entry/tariff in the same format as the
 * second section. The subscription-count is given as the mean value over the entire
 * game, the rest are sums.
 * 
 * Usage: TariffAnalysis state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class TariffAnalysis
extends LogtoolContext
implements Analyzer
{
  static private Logger log = LogManager.getLogger(TariffAnalysis.class.getName());

  // service references
  private BrokerRepo brokerRepo;
  //private TimeslotRepo timeslotRepo;
  private TariffRepo tariffRepo;
  private TimeService timeService;

  // Data
  private List<Broker> brokers;
  private HashMap<Broker, List<TariffData>> tariffs;
  private HashMap<Long, TariffData> tariffData;
  private List<Tariff> newTariffs;
  private List<Tariff> revokes;
  private List<Long> activeTariffs;
  
  // TariffData indexed by Timeslot and by tariffID
  //private HashMap<Integer, HashMap<Tariff, TariffData>> tariffData;

  private boolean narrative = false;
  private boolean started = false;
  //private boolean firstTx = false;
  private int timeslot = 0;
  private int summaryInterval = 24;
  private PrintWriter output = null;
  private String dataFilename = "tariff-analysis.data";
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new TariffAnalysis().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
   */
  private void cli (String[] args)
  {
    int offset = 0;
    if (args.length == 3 && args[0].equals("--narrative")) {
      offset = 1;
      narrative = true;
    }
    else if (args.length != 2) {
      System.out.println("Usage: org.powertac.logtool.example.TariffAnalysis [--narrative] input-file output-file");
      return;
    }
    dataFilename = args[offset + 1];
    super.cli(args[offset], this);
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    brokerRepo = (BrokerRepo) getBean("brokerRepo");
    //timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
    tariffRepo = (TariffRepo) getBean("tariffRepo");
    timeService = (TimeService) getBean("timeService");
    brokers = new ArrayList<>();
    tariffs = new HashMap<>();
    tariffData = new HashMap<>();
    newTariffs = new ArrayList<>();
    revokes = new ArrayList<>();
    activeTariffs = new ArrayList<>();
    try {
      output = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  private void firstLine ()
  {
    if (narrative) {
      output.format("Game %s\n", Competition.currentCompetition().getName());
      //output.println("Tariff fields: ID, PowerType, minDuration, signup, withdraw, periodic, rates");
    }
    else {
      output.println("Id, PwrType, Broker, intro, minDur, signup, withdraw, periodic, " + 
                     "rate-info, up-reg, dwn-reg");
    }
    if (brokers.isEmpty()) {
      brokers = brokerRepo.findRetailBrokers();
      for (Broker broker: brokers) {
        tariffs.put(broker, new ArrayList<>());
      }
      Broker db = brokerRepo.findByUsername("default broker");
      if (null == db) {
        log.error("Could not find default broker");
      }
      else {
        tariffs.put(db, new ArrayList<>());
      }
    }    
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    output.println("-------------\nGame totals\n------------");
    for (Broker broker: brokers) {
      output.println(broker.getUsername());
      List<TariffData> dataList = tariffs.get(broker);
      for (TariffData data: dataList) {
        data.printFinalSummary();
      }
    }
    output.close();
  }

  // Dump collected data to output. Format depends on perBroker setting.
  private void summarizeTimeslot (int timeslot)
  {
    output.format("--- Tariff summary ts %d:\n", timeslot);
    for (Broker broker: brokers) {
      List<TariffData> dataList = tariffs.get(broker);
      for (TariffData data: dataList) {
        if (activeTariffs.contains(data.tariff.getId())) {
          data.printSummary(timeslot);
        }
      }
    }
    activeTariffs.clear();
  }

  // Dumps all the tariffs published in the current timeslot. We to this
  // all at once because Rates are not always in the TariffSpecification
  // instances when they show up in the data stream
  private void dumpNewTariffs ()
  {
    if (newTariffs.size() > 0) {
      //dumpTimeslotMaybe ();
      output.format("--- New tariffs ts %d:\n", timeslot);
      for (Tariff tariff: newTariffs)
        dumpTariff(tariff);
      newTariffs.clear();
    }
    if (revokes.size() > 0) {
      output.format("--- Revoked tariffs ts %d:\n", timeslot);
      for (Tariff tariff: revokes) {
        output.format(" %s:%d",
                      tariff.getBroker().getUsername(),
                      tariff.getSpecId());
        Tariff sup = tariff.getIsSupersededBy();
        if (null != sup) {
          output.format(" replaced by %d", sup.getSpecId());
        }
      }
      output.println();
      revokes.clear();
    }
  }

  //   Broker, ID, PowerType, intro TS, minDuration, signup, withdraw, periodic pmt,
  //       rate-info, up-reg price, down-reg price.
  // where rate-info is a single number for a fixed rate, an array for daily TOU
  // rate, a dict for a tiered rate.
  private void dumpTariff (Tariff tariff)
  {
    //if (tariff.getId() == 401598600) {
    //  System.out.println("rr tariff");
    //}
    output.format("%s %d %s:",
                  tariff.getBroker().getUsername(), tariff.getId(),
                  tariff.getPowerType().toString());
    if (tariff.getMinDuration() > 0) {
      output.format(" minDur=%dh", tariff.getMinDuration()/3600000);
    }
    if (tariff.getSignupPayment() != 0.0) {
      output.format(" sgnup=%.3f", tariff.getSignupPayment());
    }
    if (tariff.getEarlyWithdrawPayment() != 0.0) {
      output.format(" wthdrw=%.3f", tariff.getEarlyWithdrawPayment());
    }
    if (tariff.getPeriodicPayment() != 0.0) {
      output.format(" pp=%.3f", tariff.getPeriodicPayment());
    }

    List<Rate> rates = tariff.getTariffSpecification().getRates();
    List<RegulationRate> rrs = tariff.getTariffSpecification().getRegulationRates();
    if (rates.size() == 1) {
      // Simple tariff, single rate
      output.format(" fxp=%.3f", rates.get(0).getValue());
    }
    else {
      dumpComplexRates(tariff);
    }
    if (rrs.size() > 0) {
      RegulationRate rr = rrs.get(0);
      output.format(" upreg=%.3f dwnreg=%.3f",
                    rr.getUpRegulationPayment(), rr.getDownRegulationPayment());
    }
    output.println();
  }

  // prints complex rates in a readable form
  private void dumpComplexRates (Tariff tariff)
  {
    if (tariff.isTiered()) {
      output.print(" Tiered rates");
      return;
    }
    if (tariff.isWeekly()) {
      output.print(" Weekly rates");
      return;
    }
    if (tariff.isVariableRate()) {
      output.format(" Variable: mean=%.3f, realized=%.3f",
                    tariff.getMeanConsumptionPrice(), tariff.getRealizedPrice());
      return;
    }
    if (tariff.isTimeOfUse()) {
      output.print(" tou=[");
      Instant now = timeService.getCurrentTime();
      Instant midnight = now.minus(timeService.getHourOfDay() * TimeService.HOUR);
      double[] prices = new double[24];
      TreeMap<Integer, Double> hrPrices = new TreeMap<>();
      double lastPrice = 0.0;
      for (int hr = 0; hr < prices.length; hr++) {
        prices[hr] =
                tariff.getUsageCharge(midnight.plus(hr * TimeService.HOUR), 1.0, 1.0);
        if (hr == 0) {
          lastPrice = prices[0];
          hrPrices.put(0, prices[0]);
        }
        else if (prices[hr] != lastPrice) {
          lastPrice = prices[hr];
          hrPrices.put(hr, prices[hr]);
        }
      }
      String delim = "";
      for (int hr: hrPrices.keySet()) {
        output.format("%s%d:%.3f", delim, hr, hrPrices.get(hr));
        delim = " ";
      }      
      output.print("]");
    }
  }

  // -----------------------------------
  // catch SimStart to start things off
  public void handleMessage (SimStart start)
  {
    System.out.println("SimStart");
    started = true;
    firstLine();
  }

  // -----------------------------------
  // use SimEnd to print the last line
  public void handleMessage (SimEnd end)
  {
    System.out.println("SimEnd");
    report();
  }

  // -----------------------------------
  // catch TariffSpecification messages
  public void handleMessage (TariffSpecification spec)
  {
    long tid = spec.getId();
    if (tariffData.containsKey(tid)) {
      System.out.println("Duplicate tariff " + tid);
      return;
    }
    Tariff tariff = tariffRepo.findTariffById(tid);
    if (null != tariff) {
      System.out.println("Tariff found " + tid);
    }
    else {
      tariff = new Tariff(spec);
      tariff.init();
      tariffRepo.addTariff(tariff);
    }

    if (spec.hasRegulationRate()) {
      output.format("---Tariff %s has regulation rates\n", spec.getId());
    }
    
    TariffData data = new TariffData(tariff);
    Broker broker = spec.getBroker();
    if (!tariffs.containsKey(broker)) {
      log.error("Tariff map does not contain broker {}", broker.getUsername());
    }
    else {
      tariffs.get(broker).add(data);
    }
    tariffData.put(tid, data);
    newTariffs.add(tariff);
  }

  // -----------------------------------
  // catch TariffTransaction messages
  public void handleMessage (TariffTransaction tx)
  {
    //Broker broker = tx.getBroker();
    long tid = tx.getTariffSpec().getId();
    TariffData td = tariffData.get(tid);
    double amount = tx.getCharge();
    if (tx.getTxType() == Type.PUBLISH) {
      td.addFees(amount);
      return;
    }
    if (tx.getTxType() == Type.REVOKE) {
      td.addFees(amount);
      revokes.add(tariffRepo.findTariffById(tid));
      return;
    }
    // separate state from produce/consume tx
    activeTariffs.add(tid);
    if (tx.getTxType() == Type.PRODUCE || tx.getTxType() == Type.CONSUME) {
      td.addEnergy(tx.getKWh(), amount, tx.isRegulation());
      return;
    }
    if (tx.getTxType() == Type.SIGNUP) {
      td.subscriptionChange(tx.getCustomerCount());
      td.addStaticEarnings(amount);
      return;
    }
    if (tx.getTxType() == Type.WITHDRAW) {
      td.subscriptionChange(-tx.getCustomerCount());
      td.addStaticEarnings(amount);
      return;
    }
    // PERIODIC, REFUND
    td.addStaticEarnings(amount);
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  private int skip = 1; // Skip the first one
  public void handleMessage (TimeslotUpdate tu)
  {
    if (started) {
      if (skip > 0)
        skip -= 1;
      else {
        dumpNewTariffs();
        if (timeslot % summaryInterval == 0) {
          //System.out.println("Summarize ts " + timeslot);
          summarizeTimeslot(timeslot);
          for (TariffData data: tariffData.values()) {
            data.clear();
          }
        }
        timeslot = tu.getFirstEnabled() -
                Competition.currentCompetition().getDeactivateTimeslotsAhead();
      }
    }
  }

  class TariffData
  {
    Tariff tariff;
    double fees = 0.0;
    double totalFees = 0.0;
    double energy = 0.0;
    double totalEnergy = 0.0;
    double energyEarnings = 0.0;
    double totalEnergyEarnings = 0.0;
    double balanceEnergy = 0.0;
    double totalBalanceEnergy = 0.0;
    double balanceEarnings = 0.0;
    double totalBalanceEarnings = 0.0;
    int subChange = 0;
    int subscribers = 0;
    double staticEarnings = 0.0;
    double totalStaticEarnings = 0.0;

    TariffData (Tariff tariff)
    {
      super();
      this.tariff = tariff;  
    }

    void addFees (double value)
    {
      fees += value;
      totalFees += value;
    }

    void addEnergy (double kwh, double money, boolean balancing)
    {
      if (!balancing) {
        energy += kwh;
        totalEnergy += kwh;
        energyEarnings += money;
        totalEnergyEarnings += money;
      }
      else {
        balanceEnergy += kwh;
        totalBalanceEnergy += kwh;
        balanceEarnings += money;
        totalBalanceEarnings += money;
      }
    }

    void addStaticEarnings (double value)
    {
      staticEarnings += value;
      totalStaticEarnings += value;
    }

    void subscriptionChange (int count)
    {
      subChange += count;
      subscribers += count;
    }

    void clear ()
    {
      fees = 0.0;
      energy = 0.0;
      energyEarnings = 0.0;
      balanceEnergy = 0.0;
      balanceEarnings = 0.0;
      subChange = 0;
      staticEarnings = 0.0;
    }

    void printSummary (int timeslot)
    {
      output.format("%s %d:", tariff.getBroker().getUsername(), tariff.getId());
      //if (!tariff.isActive())
      //  output.print(" (inactive)");
      if (fees != 0.0)
        output.format(" fees=%.3f", fees);
      output.format(" energy=%.3f earnings=%.3f", energy, energyEarnings);
      if (balanceEnergy != 0.0 || balanceEarnings != 0.0)
        output.format(" regulation=%.3f reg earnings=%.3f",
                      balanceEnergy, balanceEarnings);
      if (subChange > 0)
        output.format(" signups=%d", subChange);
      else if (subChange < 0)
        output.format(" withdrawals=%d", -subChange);
      output.format(" subscribers=%d", subscribers);
      if (staticEarnings != 0.0)
        output.format(" cust fees=%.3f", staticEarnings);
      output.println();
    }

    void printFinalSummary ()
    {
      output.format("%d %s", tariff.getId(), tariff.getPowerType().toString());
      output.format(" fees=%.3f", totalFees);
      output.format(" energy=%.3f, earnings=%.3f",
                    totalEnergy, totalEnergyEarnings);
      output.format(" regulation=%.3f, reg earnings=%.3f",
                    totalBalanceEnergy, totalBalanceEarnings);
      output.format(" subscribers=%d", subscribers);
      output.format(" cust fees=%.3f", totalStaticEarnings);
      output.println();
    }
  }
}
