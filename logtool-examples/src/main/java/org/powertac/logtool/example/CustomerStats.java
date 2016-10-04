/*
 * Copyright (c) 2016 by the original author
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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.CustomerInfo;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherReport;
import org.powertac.common.enumerations.PowerType;
import org.powertac.common.repo.CustomerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Counts customer instances of various types. Note that it's not enough
 * to catch the creation of the CustomerInfo instances in the state log,
 * because the PowerType is set in a separate operation. Therefore, we just
 * let them accumulate in the CustomerRepo and count them at the end.
 * 
 * @author John Collins
 */
public class CustomerStats
extends LogtoolContext
implements Analyzer
{
  static private Logger log = Logger.getLogger(CustomerStats.class.getName());

  private DomainObjectReader dor;
  private CustomerRepo customerRepo;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "customers.txt";

  Map<PowerType, Integer> customerByType;

  /**
   * Default constructor
   */
  public CustomerStats ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new CustomerStats().cli(args);
  }
  
  /**
   * Takes two args, input filename and output filename
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

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#setup()
   */
  @Override
  public void setup ()
  {
    dor = (DomainObjectReader) getBean("reader");
    customerRepo = (CustomerRepo)getBean("customerRepo");

    customerByType = new HashMap<>();
    //dor.registerNewObjectListener(new CustomerInfoHandler(),
    //                              CustomerInfo.class);
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      log.error("Cannot open file " + dataFilename);
    }
  }

  /* (non-Javadoc)
   * @see org.powertac.logtool.ifc.Analyzer#report()
   */
  @Override
  public void report ()
  {
    for (CustomerInfo info: customerRepo.list()) {
      PowerType pt = info.getPowerType();
      Integer count = customerByType.get(pt);
      int cc = info.getPopulation() + (null == count?0:count);
      customerByType.put(pt, cc);
    }
    for (PowerType pt: customerByType.keySet()) {
      data.format("%s: %d\n", pt.toString(), customerByType.get(pt));
    }
    data.close();
    return;
  }
}
