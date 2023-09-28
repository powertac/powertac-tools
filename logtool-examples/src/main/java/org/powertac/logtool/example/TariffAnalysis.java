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
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.IntStream;

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
 * Output depends on whether the --narrative option or the --profile option
 * is specified. In narrative mode, the output is a sequence of events, including
 * tariff publication, subscription changes, and daily summaries of per-tariff earnings.
 * 
 * With the --profile option, output is tariffs in a form directly readable by python.
 * Each published tariff is represented as a dict of the form
 *  {broker:b,ts:ts,tariffId:id,powerType:pt,minDuration:md,signup:p,withdraw:w,
 *   periodic:p,tiered:tf,variable:tf,rate:rrr,upReg:u,downReg:d}
 * where the rate is a list of one element for fixed price, or 24 or 168 elements for
 * a TOU rate. The rate field is blank for a variable-rate tariff.
 * 
 * Without an option, output is in three parts. Part 1 is a list of
 * tariffs in the order they appear in the log. For each tariff, fields are:
 *   intro TS, Broker, ID, PowerType, minDuration, signup, withdraw, periodic pmt,
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
 * NOTE: Numeric data is formatted using the US locale in order to avoid confusion over
 * the meaning of the comma character when used in other locales.
 * 
 * Usage: TariffAnalysis [options] state-log-file output-data-file
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
  private HashMap<Broker, List<TariffData>> brokerTariff;
  private TreeMap<Integer, HashMap<Broker, List<TariffActivity>>> subscriptionActivity; 
  private HashMap<Long, TariffData> tariffData;
  private List<Tariff> newTariffs;
  private List<Tariff> revokes;
  private Set<Long> activeTariffs;
  private Set<Long> activeSubs;
  
  // TariffData indexed by Timeslot and by tariffID
  //private HashMap<Integer, HashMap<Tariff, TariffData>> tariffData;

  private boolean narrative = false;
  private boolean profile = false;
  private boolean started = false;
  //private boolean firstTx = false;
  private int timeslot = 360;
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
    else if (args.length == 3 && args[0].equals("--profile")) {
      offset = 1;
      profile = true;
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
    brokerTariff = new HashMap<>();
    tariffData = new HashMap<>();
    subscriptionActivity = new TreeMap<>();
    newTariffs = new ArrayList<>();
    revokes = new ArrayList<>();
    activeTariffs = new TreeSet<>();
    activeSubs = new TreeSet<>();
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
    if (brokers.isEmpty()) {
      brokers = brokerRepo.findRetailBrokers();
      for (Broker broker: brokers) {
        if (!brokerTariff.containsKey(broker))
        brokerTariff.put(broker, new ArrayList<>());
      }
      Broker db = brokerRepo.findByUsername("default broker");
      if (null == db) {
        log.error("Could not find default broker");
      }
      else if (!brokerTariff.containsKey(db)) {
        brokerTariff.put(db, new ArrayList<>());
      }
    }    
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    if (narrative) {
      output.println("-------------\nGame totals\n------------");
      for (Broker broker: brokers) {
        output.println(broker.getUsername());
        List<TariffData> dataList = brokerTariff.get(broker);
        for (TariffData data: dataList) {
          data.printFinalSummary();
        }
      }
    }
    else if (!profile) {
      output.println("------------\nSubscription changes\n------------");
      // broker name header
      output.print("                ");
      for (Broker broker: brokers) {
        output.format("%-42s", broker.getUsername());
      }
      output.println();
      // iterate through timeslots
      for (int ts: subscriptionActivity.keySet()) {
        HashMap<Broker, List<TariffActivity>> tsActivity =
                subscriptionActivity.get(ts);
        // count rows needed
        int rows = 0;
        for (Broker broker: brokers) {
          if (tsActivity.containsKey(broker)) {
            rows = Math.max(rows, tsActivity.get(broker).size());
          }
        }
        if (rows == 0)
          continue;
        IntStream.range(0, rows).forEach
        (r -> {
          if (r == 0)
            output.format("%4d", ts);
          else
            output.print("    ");
          for (Broker broker: brokers) {
            if (!tsActivity.containsKey(broker)) {
              output.format("%42s", " ");
              continue;
            }
            List<TariffActivity> acts = tsActivity.get(broker);
            if (acts.size() <= r) {
              output.format("%42s", "");
              continue;
            }
            TariffActivity ta = acts.get(r);
            // [subscription count, tariff-payment, rate-payment, regulation-payment]
            output.format("  %9d [%6d %10.3f %10.3f]",
                          ta.getTariffid(),
                          ta.getSubCount(), ta.getTariffPmt(),
                          ta.getRatePmt());
          }
          output.println();
        });
        output.println();
      }
    }
    output.close();
  }

  // Dump collected data to output. Format depends on perBroker setting.
  private void narrativeSummary (int timeslot)
  {
    if (narrative) {
      output.format("--- Tariff summary ts %d, %s:\n", timeslot,
                    timeService.getCurrentDateTime().toString("MM-dd-hh"));
      for (Broker broker: brokers) {
        List<TariffData> dataList = brokerTariff.get(broker);
        if (dataList.isEmpty())
          log.error("Empty data list for {}", broker.getUsername());
        for (TariffData data: dataList) {
          if (activeTariffs.contains(data.tariff.getId())) {
            data.printSummary(timeslot);
          }
        }
      }
      activeTariffs.clear();
    }
    else {
      // not narrative
    }
  }

  // Dumps all the tariffs published in the current timeslot. We do this
  // all at once because Rates are not always in the TariffSpecification
  // instances when they show up in the data stream
  private void dumpNewTariffs ()
  {
    if (narrative) {
      if (newTariffs.size() > 0) {
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
          output.println();
        }
        revokes.clear();
      }
    }
    else if (profile) {
      if (newTariffs.size() > 0) {
        for (Tariff tariff: newTariffs) {
          dumpTariffProfile(tariff);
        }
        newTariffs.clear();
      }
    }
    else {
      recordSubscriptionChanges();
      if (newTariffs.size() == 0 && revokes.size() == 0)
        return;
      // not narrative mode, get here every ts
      output.format("--- ts %d\n", timeslot);
      if (newTariffs.size() > 0) {
        for (Tariff tariff: newTariffs) {
          output.print("offer ");
          dumpTariff(tariff);
        }
        newTariffs.clear();
      }
      if (revokes.size() > 0) {
        for (Tariff tariff: revokes) {
          output.format("kill %d %s %d %s",
                        timeslot, tariff.getBroker().getUsername(),
                        tariff.getSpecId(), tariff.getPowerType().toString());
          Tariff sup = tariff.getIsSupersededBy();
          if (null != sup) {
            output.format(" replaced by %d", sup.getSpecId());
          }
          output.println();
        }
        revokes.clear();
      }
    }
  }

  private void recordSubscriptionChanges ()
  {
    if (!subscriptionActivity.containsKey(timeslot))
      subscriptionActivity.put(timeslot, new HashMap<>());
    HashMap<Broker, List<TariffActivity>> timeslotActivity =
            subscriptionActivity.get(timeslot);
    for (long tid: activeSubs) {
      // find the TariffData for this tariff and make a TariffActivity snapshot of it
      TariffData td = tariffData.get(tid);
      TariffActivity ta = new TariffActivity(td);
      Broker broker = td.getBroker();
      if (!timeslotActivity.containsKey(broker))
        timeslotActivity.put(broker, new ArrayList<>());
      List<TariffActivity> activities = timeslotActivity.get(broker);
      activities.add(ta);
    }
    activeSubs.clear();
  }

  //   Broker, ID, PowerType, intro TS, minDuration, signup, withdraw, periodic pmt,
  //       rate-info, up-reg price, down-reg price.
  // where rate-info is a single number for a fixed rate, an array for daily TOU
  // rate, a dict for a tiered rate.
  private void dumpTariff (Tariff tariff)
  {
    //if (tariff.getId() == 501428079) {
    //  System.out.println("rr tariff");
    //}
    output.format("%s %d %s:",
                  tariff.getBroker().getUsername(), tariff.getId(),
                  tariff.getPowerType().toString());
    if (tariff.getMinDuration() > 0) {
      output.format(" minDur=%dh", tariff.getMinDuration()/3600000);
    }
    if (tariff.getSignupPayment() != 0.0) {
      output.format(" sgnup=%s", df.format(tariff.getSignupPayment()));
    }
    if (tariff.getEarlyWithdrawPayment() != 0.0) {
      output.format(" wthdrw=%s", df.format(tariff.getEarlyWithdrawPayment()));
    }
    if (tariff.getPeriodicPayment() != 0.0) {
      output.format(" pp=%s", df.format(tariff.getPeriodicPayment()));
    }

    List<Rate> rates = tariff.getTariffSpecification().getRates();
    List<RegulationRate> rrs = tariff.getTariffSpecification().getRegulationRates();
    if (rates.size() == 1) {
      // Simple tariff, single rate
      output.format(" fxp=%s", df.format(rates.get(0).getValue()));
    }
    else {
      dumpComplexRates(tariff);
    }
    if (rrs.size() > 0) {
      RegulationRate rr = rrs.get(0);
      output.format(" upreg=%s dwnreg=%s",
                    df.format(rr.getUpRegulationPayment()),
                    df.format(rr.getDownRegulationPayment()));
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
    if (tariff.isVariableRate()) {
      output.format(" Variable: mean=%s, realized=%s",
                    df.format(tariff.getMeanConsumptionPrice()),
                    df.format(tariff.getRealizedPrice()));
      return;
    }
    if (tariff.isWeekly()) {
      output.print(" tou=[");
      //start at midnight next Monday
      Instant start = mondayMidnight();
      TreeMap<Integer, TreeMap<Integer, Double>> dayPrices = new TreeMap<>();
      int lastDay = 0;
      for (int d = 0; d < 7; d++) {
        TreeMap<Integer, Double> dp =
                gatherHourlyPrices(tariff, start.plus(d * TimeService.DAY));
        if (d == 0) {
          dayPrices.put(d, dp);
        }
        else if (!dp.equals(dayPrices.get(lastDay))) {
          // prices change on day d
          dayPrices.put(d, dp);
          lastDay = d;
        }
      }
      String delimiter = "";
      for (int d : dayPrices.keySet()) {
        output.format("%s%s", delimiter, DayOfWeek.of(d + 1));
        printHourlyPrices(dayPrices.get(d));
        delimiter = " ";
      }
      output.print("]");
      return;
    }
    if (tariff.isTimeOfUse()) {
      output.print(" tou=");
      Instant now = timeService.getCurrentTime();
      Instant midnight = now.minus(timeService.getHourOfDay() * TimeService.HOUR);
      TreeMap<Integer, Double> hrPrices = gatherHourlyPrices(tariff, midnight);
      printHourlyPrices(hrPrices);
    }
  }

  private Instant mondayMidnight ()
  {
    Instant now = timeService.getCurrentTime();
    Instant midnight = now.minus(timeService.getHourOfDay() * TimeService.HOUR);
    int day = midnight.toDateTime().getDayOfWeek();
    Instant start = midnight.plus((7 - day) * TimeService.DAY);
    return start;
  }

  private void dumpTariffProfile (Tariff tariff)
  {
    //{broker:b,ts:ts,tariffId:id,powerType:pt,minDuration:md,signup:p,withdraw:w,
    // periodic:p,tiered:tf,variable:tf,rate:rrr,upReg:u,downReg:d}
    output.format("{'broker':'%s','ts':%d,'tariffId':%d,'powerType':'%s',",
                  tariff.getBroker().getUsername(), timeslot, tariff.getId(),
                  tariff.getPowerType().toString());
    output.format("'minDuration':%d,'signup':%s,'withdraw':%s,'periodic':%s,",
                  tariff.getMinDuration(),
                  df.format(tariff.getSignupPayment()),
                  df.format(tariff.getEarlyWithdrawPayment()),
                  df.format(tariff.getPeriodicPayment()));
    output.format("'tiered':%s,", tariff.isTiered()?"True":"False");
    output.format("'variable':%s,", tariff.isVariableRate()?"True":"False");
    double[] prices = getRateArray(tariff);
    output.print("'rate':");
    String delim = "[";
    for (double price: prices) {
      output.format("%s%s", delim, df.format(price));
      delim = ",";
    }
    output.print("],");
    double upreg = 0.0;
    double downreg = 0.0;
    if (tariff.getTariffSpecification().hasRegulationRate()) {
      RegulationRate rr = tariff.getTariffSpecification().getRegulationRates().get(0);
      upreg = rr.getUpRegulationPayment();
      downreg = rr.getDownRegulationPayment();
    }
    output.format("'upReg':%s,'downReg':%s}\n",
                  df.format(upreg), df.format(downreg));
  }

  private double[] getRateArray (Tariff tariff)
  {
    double[] prices;
    Instant start = mondayMidnight();
    if (tariff.isTimeOfUse()) {
      int hrs = 24;
      if (tariff.isWeekly())
        hrs = 168;
      prices = new double[hrs];
      for (int hr = 0; hr < prices.length; hr++) {
        prices[hr] =
                tariff.getUsageCharge(start.plus(hr * TimeService.HOUR), 1.0);
      }
    }
    else {
      prices = new double[1];
      prices[0] = tariff.getTariffSpecification().getRates().get(0).getValue();
    }
    return prices;
  }

  private TreeMap<Integer, Double> gatherHourlyPrices (Tariff tariff,
                                                       Instant midnight)
  {
    double[] prices = new double[24];
    TreeMap<Integer, Double> hrPrices = new TreeMap<>();
    double lastPrice = 0.0;
    for (int hr = 0; hr < prices.length; hr++) {
      prices[hr] =
              tariff.getUsageCharge(midnight.plus(hr * TimeService.HOUR), 1.0);
      if (hr == 0) {
        lastPrice = prices[0];
        hrPrices.put(0, prices[0]);
      }
      else if (prices[hr] != lastPrice) {
        lastPrice = prices[hr];
        hrPrices.put(hr, prices[hr]);
      }
    }
    return hrPrices;
  }

  private void printHourlyPrices (TreeMap<Integer, Double> hrPrices)
  {
    output.print("[");
    String delim = "";
    for (int hr: hrPrices.keySet()) {
      output.format("%s%d:%s", delim, hr, df.format(hrPrices.get(hr)));
      delim = " ";
    }      
    output.print("]");
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
    }

    //if (spec.hasRegulationRate()) {
    //  output.format("---Tariff %s has regulation rates\n", spec.getId());
    //}
    
    TariffData data = new TariffData(tariff);
    Broker broker = spec.getBroker();
    if (!brokerTariff.containsKey(broker)) {
      brokerTariff.put(broker, new ArrayList<>());
    }
    brokerTariff.get(broker).add(data);
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
      activeSubs.add(tid);
      return;
    }
    if (tx.getTxType() == Type.WITHDRAW) {
      td.subscriptionChange(-tx.getCustomerCount());
      td.addStaticEarnings(amount);
      activeSubs.add(tid);
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
          narrativeSummary(timeslot);
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

    Broker getBroker ()
    {
      return tariff.getBroker();
    }

    long getTariffId ()
    {
      return tariff.getSpecId();
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

    double getRateEarnings ()
    {
      return totalEnergyEarnings;
    }

    double getRegulationEarnings ()
    {
      return totalBalanceEarnings;
    }

    void addStaticEarnings (double value)
    {
      staticEarnings += value;
      totalStaticEarnings += value;
    }

    double getStaticEarnings ()
    {
      return totalStaticEarnings + totalFees;
    }

    void subscriptionChange (int count)
    {
      subChange += count;
      subscribers += count;
    }

    int getSubscribers ()
    {
      return subscribers;
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
        output.format(" fees=%s", df.format(fees));
      output.format(" energy=%s earnings=%s",
                    df.format(energy), df.format(energyEarnings));
      if (balanceEnergy != 0.0 || balanceEarnings != 0.0)
        output.format(" regulation=%s reg earnings=%s",
                      df.format(balanceEnergy), df.format(balanceEarnings));
      if (subChange > 0)
        output.format(" signups=%d", subChange);
      else if (subChange < 0)
        output.format(" withdrawals=%d", -subChange);
      output.format(" subscribers=%d", subscribers);
      if (staticEarnings != 0.0)
        output.format(" cust fees=%s", df.format(staticEarnings));
      output.println();
    }

    void printFinalSummary ()
    {
      output.format("%d %s", tariff.getId(), tariff.getPowerType().toString());
      output.format(" fees=%s", df.format(totalFees));
      output.format(" energy=%s, earnings=%s",
                    df.format(totalEnergy), df.format(totalEnergyEarnings));
      output.format(" regulation=%s, reg earnings=%s",
                    df.format(totalBalanceEnergy), df.format(totalBalanceEarnings));
      output.format(" subscribers=%d", subscribers);
      output.format(" cust fees=%s", df.format(totalStaticEarnings));
      output.println();
    }
  }

  class TariffActivity
  {
    int ts;
    long tariffId;
    int subCount;
    double tariffPmt;
    double ratePmt;
    double regPmt;

    TariffActivity (long tid, int subs, double tp, double rp, double rgp)
    {
      super();
      ts = timeslot;
      tariffId = tid;
      subCount = subs;
      tariffPmt = tp;
      ratePmt = rp;
      regPmt = rgp;
    }

    TariffActivity (TariffData td)
    {
      super();
      ts = timeslot;
      tariffId = td.getTariffId();
      subCount = td.getSubscribers();
      tariffPmt = td.getStaticEarnings();
      ratePmt = td.getRateEarnings();
      regPmt = td.getRegulationEarnings();
    }

    double getTimeslot ()
    {
      return ts;
    }

    long getTariffid ()
    {
      return tariffId;
    }

    int getSubCount ()
    {
      return subCount;
    }

    double getTariffPmt ()
    {
      return tariffPmt;
    }

    double getRatePmt ()
    {
      return ratePmt;
    }

    double getRegPmt ()
    {
      return regPmt;
    }
  }
}
