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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;

import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Extracts info about the competition and brokers, records in a summary
 * file having two kinds of records:
 * A competition record looks like
 *   competition, game-id, game-length, nbrokers
 * A broker record looks like 
 *   broker, broker-name, broker-id
 * 
 * TODO - this does not work with newer logs in which the logfile name does
 * not begin with 'game-'.
 * 
 * @author John Collins
 */
public class GameBrokerInfo
extends LogtoolContext
implements Analyzer
{
  //static private Logger log = Logger.getLogger(GameBrokerInfo.class.getName());

  // data collectors for current timeslot
  private int timeslotCount = 0;
  private Competition competition;
  private HashMap<String, Broker> brokers;

  // data output file
  private PrintWriter data = null;
  private String dataFilename = "data.txt";

  /**
   * Constructor does nothing. Call setup() before reading a file to
   * get this to work.
   */
  public GameBrokerInfo ()
  {
    super();
  }
  
  /**
   * Main method just creates an instance and passes command-line args to
   * its inherited cli() method.
   */
  public static void main (String[] args)
  {
    new GameBrokerInfo().cli(args);
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

  /**
   * Creates data structures, opens output file. It would be nice to dump
   * the broker names at this point, but they are not known until we hit the
   * first timeslotUpdate while reading the file.
   */
  @Override
  public void setup ()
  {
    brokers = new HashMap<String, Broker>();
    try {
      data = new PrintWriter(new File(dataFilename));
    }
    catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void report ()
  {
    // competition name is of the form "game-nnn" but we want the nnn
    String gameName = competition.getName();
    String[] elements = gameName.split("-");
    if (elements.length != 2) {
      // should be two elements: "game" and nnn
      System.out.println("game name does not start with game-");
    }
    else {
      int game = Integer.parseInt(elements[1]);
      data.print(String.format("%s, %d, %d, %d\n",
                               "competition", game,
                               timeslotCount - 1,
                               brokers.size()));
      for (String brokerName: brokers.keySet()) {
        Broker broker = brokers.get(brokerName);
        data.print(String.format("%s, %s, %d\n",
                                 "broker",
                                 broker.getUsername(),
                                 broker.getId()));
      }
    }
    data.close();
  }

  // -----------------------------------
  // catch TimeslotUpdate events
  public void handleMessage (TimeslotUpdate msg)
  {
    timeslotCount += 1;
    data.print("-- timeslot " + timeslotCount);
    data.flush();
  }

  // -----------------------------------
  // catch the Competition instance
  public void handleMessage (Competition comp)
  {
    competition = comp;
  }

  // -----------------------------------
  public void handleMessage (Broker broker)
  {
    if (!(broker.isWholesale() ||
        broker.getUsername().equals("default broker")))
      brokers.put(broker.getUsername(), broker);
  }
}
