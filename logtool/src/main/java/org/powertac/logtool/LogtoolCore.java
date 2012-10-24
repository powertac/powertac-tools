/*
 * Copyright (c) 2012 by the original author
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
package org.powertac.logtool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.powertac.logtool.common.DomainObjectReader;

/**
 * Reads a state log file, re-creates and updates objects, calls
 * listeners.
 * @author John Collins
 */
public class LogtoolCore
{
  //static private Logger log = Logger.getLogger(LogtoolCore.class.getName());

  /**
   * Default constructor
   */
  public LogtoolCore ()
  {
    super();
  }
  
  /**
   * Processes a command line. For now, it's just the name of a
   * state-log file from the simulation server.
   */
  public void processCmdLine (String[] args)
  {
    if (args.length == 0) {
      System.out.println("Usage: Logtool file");
      return;
    }
    File input = new File(args[0]);
    if (!input.canRead()) {
      System.out.println("Cannot read file " + args[0]);
    }
    DomainObjectReader dor = new DomainObjectReader();
    try {
      BufferedReader in =
              new BufferedReader(new FileReader(input));
      while (true) {
        String line = in.readLine();
        if (null == line)
          break;
        dor.readObject(line);
      }
    }
    catch (FileNotFoundException e) {
      System.out.println("Cannot open file " + args[0]);
    }
    catch (IOException e) {
      System.out.println("error reading from file " + args[0]);
    }
  }
}
