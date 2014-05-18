package org.powertac.windpark;

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
public class WindSpeedForecastErrorScenarios {
	private static String errorScenarioDataFile = "/home/shashank/Downloads/WindSpeedForecastErrorScenariosMinneapolis.xml";
	@XStreamImplicit
	private SortedSet<Scenario> windSpeedForecastErrorScenarios = new TreeSet<Scenario>();

	public WindSpeedForecastErrorScenarios() {

	}

	public WindSpeedForecastErrorScenarios(Collection<Scenario> scenarios) {
		windSpeedForecastErrorScenarios.addAll(scenarios);
	}

	public boolean addScenario(Scenario sco) {
		return windSpeedForecastErrorScenarios.add(sco);
	}

	public boolean addScenarios(Collection<Scenario> scenarioCollection) {
		return windSpeedForecastErrorScenarios.addAll(scenarioCollection);
	}

	public Set<Scenario> getScenarios() {
		return Collections
				.unmodifiableSortedSet(windSpeedForecastErrorScenarios);
	}

	public  static XStream getConfiguredXStream() {
		XStream xstream = new XStream();
		xstream.alias("Scenario", Scenario.class);
		xstream.alias("WindForecastErrorScenarios",
				WindSpeedForecastErrorScenarios.class);
		xstream.alias("Value", Scenario.ScenarioValue.class);
		xstream.addImplicitCollection(WindSpeedForecastErrorScenarios.class,
				"windSpeedForecastErrorScenarios");
		xstream.addImplicitCollection(Scenario.class, "values");
		xstream.useAttributeFor(Scenario.class, "scenarioNumber");
		xstream.aliasField("id", Scenario.class, "scenarioNumber");
		xstream.useAttributeFor(Scenario.class, "probability");
		xstream.useAttributeFor(Scenario.ScenarioValue.class, "hour");
		xstream.useAttributeFor(Scenario.ScenarioValue.class, "value");
		xstream.aliasField("error", Scenario.ScenarioValue.class, "value");
		return xstream;
	}

	public WindSpeedForecastErrorScenarios getWindForecastErrorScenarios() {
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(errorScenarioDataFile);
		} catch (FileNotFoundException ex) {
			System.out.println(String.format("File not found %s",
					errorScenarioDataFile));
		}
		XStream xstream = getConfiguredXStream();
		WindSpeedForecastErrorScenarios wferrorScenarios = (WindSpeedForecastErrorScenarios) xstream
				.fromXML(inputStream);
		for (Scenario scn : wferrorScenarios.getScenarios()) {
			scn.createValueList();
		}
		return wferrorScenarios;
	} // getWindForecastErrorScenarios()

} //class WindSpeedForecastErrorScenarios
