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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.MissingDomainObject;
import org.powertac.logtool.ifc.Analyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Reads a state log file, re-creates and updates objects, calls
 * listeners.
 * @author John Collins
 */
@Service
public class LogtoolCore
{
  static private Logger log = Logger.getLogger(LogtoolCore.class.getName());

  @Autowired
  private DomainObjectReader reader;

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
    if (args.length < 2) {
      System.out.println("Usage: Logtool file analyzer");
      return;
    }
    readStateLog(args[0], args[1]);
  }

  /**
   * Reads the given state-log file using the DomainObjectReader
   */
  public void readStateLog (String filename, String analyzer)
  {
    File input = new File(filename);
    String line = null;
    if (!input.canRead()) {
      System.out.println("Cannot read file " + filename);
    }
    
    Analyzer tool;
    try {
      Class<?> toolClass = Class.forName(analyzer);
      tool = (Analyzer)toolClass.newInstance();
    }
    catch (ClassNotFoundException e1) {
      System.out.println("Cannot find analyzer class " + analyzer);
      return;
    }
    catch (Exception ex) {
      System.out.println("Exception creating analyzer " + ex.toString());
      return;
    }
    
    try {
      tool.setup();
      DomainObjectReader dor = getReader();
      BufferedReader in =
              new BufferedReader(new FileReader(input));
      while (true) {
        line = in.readLine();
        if (null == line)
          break;
        dor.readObject(line);
      }
      tool.report();
    }
    catch (FileNotFoundException e) {
      System.out.println("Cannot open file " + filename);
    }
    catch (IOException e) {
      System.out.println("error reading from file " + filename);
    }
    catch (MissingDomainObject e) {
      System.out.println("MDO on " + line);
    }
  }
  
  /**
   * Returns the singleton DomainObjectReader, 
   * which is needed to set callbacks.
   */
  public DomainObjectReader getReader ()
  {
    if (null == reader) {
      log.warn("Reader is not autowired");
      reader = new DomainObjectReader();
    }
    return reader;
  }
}
