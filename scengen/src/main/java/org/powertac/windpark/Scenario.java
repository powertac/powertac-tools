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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * This class represents a scenario. A scenario can be a wind speed error
 * scenario, or a wind power production scenario etc.
 * 
 * @author spande00 (Shashank Pande)
 * 
 */
@XStreamAlias("Scenario")
public class Scenario implements Comparable<Scenario> {
	@XStreamAlias("Value")
	public static class ScenarioValue implements Comparable<ScenarioValue> {
		@XStreamAlias("hour")
		@XStreamAsAttribute
		private int hour = 0;
		@XStreamAlias("error")
		@XStreamAsAttribute
		private double value = 0;

		public ScenarioValue(int hr, double val) {
			this.hour = hr;
			this.value = val;
		}

		public int getHour() {
			return this.hour;
		}

		public double getValue() {
			return this.value;
		}

		public int compareTo(ScenarioValue sv) {
			if (this.hour < sv.getHour()) {
				return -1;
			} else if (this.hour > sv.getHour()) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	@XStreamAlias("number")
	@XStreamAsAttribute
	private int scenarioNumber = 0;
	@XStreamAlias("probability")
	@XStreamAsAttribute
	private double probability = 0.0;
	@XStreamImplicit
	private SortedSet<ScenarioValue> values = new TreeSet<ScenarioValue>();
	// a list representation is needed for performance reason
	private List<ScenarioValue> valueList = new ArrayList<ScenarioValue>();

	public Scenario(final int number, final double prob) {
		this.scenarioNumber = number;
		this.probability = prob;
	}

	public int getScenarioNumber() {
		return this.scenarioNumber;
	}

	public double getProbability() {
		return this.probability;
	}

	public Set<ScenarioValue> getValues() {
		return Collections.unmodifiableSortedSet(this.values);
	}

	public void addValue(ScenarioValue sv) {
		if ((sv != null) && (sv.getHour() > 0)) {
			values.add(sv);
		}
	}

	public void createValueList() {
		if (!values.isEmpty() && valueList.isEmpty()) {
			valueList.addAll(values);
		}
	}

	public List<ScenarioValue> getValueList() {
		return Collections.unmodifiableList(valueList);
	}

	public int compareTo(Scenario o) {
		if (this.scenarioNumber < o.getScenarioNumber()) {
			return -1;
		} else if (this.scenarioNumber > o.getScenarioNumber()) {
			return 1;
		}
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Scenario) {
			Scenario obj = (Scenario) o;
			return (this.scenarioNumber == obj.getScenarioNumber());
		}
		return false;
	}

}
