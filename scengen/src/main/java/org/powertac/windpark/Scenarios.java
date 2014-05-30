/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS,  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.powertac.windpark;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * This class represents wind speed forecast error scenarios.
 * 
 * @author spande00 (Shashank Pande)
 * 
 */
@XStreamAlias("WindForecastErrorScenarios")
public class Scenarios
{
  @XStreamImplicit
  private SortedSet<Scenario> scenarioSet = new TreeSet<Scenario>();
    

  protected Scenarios ()
  {

  }

  public Scenarios (Collection<Scenario> scenarios)
  {
    scenarioSet.addAll(scenarios);
  }

  public boolean addScenario (Scenario sco)
  {
    sco.createValueList();
    return scenarioSet.add(sco);
  }

  public boolean addScenarios (Collection<Scenario> scenarioCollection)
  {
    return scenarioSet.addAll(scenarioCollection);
  }

  public Set<Scenario> getScenarios ()
  {
    return Collections.unmodifiableSortedSet(scenarioSet);
  }
  
  public static XStream getConfiguredXStream() {
    XStream xstream = new XStream();
    xstream.alias("Scenario", Scenario.class);
    xstream.alias("Scenarios", Scenarios.class);
    xstream.alias("Value", Scenario.ScenarioValue.class);
    xstream.addImplicitCollection(Scenarios.class, "scenarioSet");
    xstream.addImplicitCollection(Scenario.class, "values");
    xstream.useAttributeFor(Scenario.class, "scenarioNumber");
    xstream.aliasField("id", Scenario.class, "scenarioNumber");
    xstream.useAttributeFor(Scenario.class, "probability");
    xstream.useAttributeFor(Scenario.ScenarioValue.class, "hour");
    xstream.useAttributeFor(Scenario.ScenarioValue.class, "value"); 
    xstream.aliasField("error", Scenario.ScenarioValue.class, "value");
    return xstream;
  }
  
	public static Scenarios getScenarios(String errorScenarioDataFile) {
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(errorScenarioDataFile);
		} catch (FileNotFoundException ex) {
			System.out.println("File Not Found");
		}
		XStream xstream = getConfiguredXStream();
		Scenarios wferrorScenarios = (Scenarios) xstream.fromXML(inputStream);
		for (Scenario scn : wferrorScenarios.getScenarios()) {
			scn.createValueList();
		}
		return wferrorScenarios;
	}
	
	public boolean writeToXML(String fileName) {
		XStream xstream = Scenarios.getConfiguredXStream();
		String xmlStr = xstream.toXML(this);		
		try {
			FileWriter fw = new FileWriter(fileName);
			fw.write(xmlStr);
			fw.write("\n");
			fw.close();
		} catch (IOException ex) {
			System.out.println(ex);
			return false;
		}		
		
		return true;
	}

}

