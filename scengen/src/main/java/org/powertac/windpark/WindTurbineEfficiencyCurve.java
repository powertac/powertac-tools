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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents the windfarm efficiency curve. It stores the efficiency
 * curve data and provides API to retrieve efficiency at given wind speed.
 * 
 * @author Shashank Pande
 * 
 */

public class WindTurbineEfficiencyCurve {

	private static class WindSpeedband {
		private double fromWindSpeed = 0;
		private double toWindSpeed = 0;

		public WindSpeedband(double fromSpeed, double toSpeed) {
			this.fromWindSpeed = fromSpeed;
			this.toWindSpeed = toSpeed;
		}

		public boolean isWithinSpeedband(double windSpeed) {
			if ((windSpeed >= fromWindSpeed) && (windSpeed < toWindSpeed)) {
				return true;
			} else {
				return false;
			}
		}
	} // static class WindSpeedband

	/** Configured values to be read as List of Strings */
	private List<String> cfgWindSpeedbands = Arrays.asList("4-5","5-6","6-7","7-8",
			                                 "8-9","9-10","10-11","11-12","12-13","13-14");
	private List<String> cfgSlope = Arrays.asList("0.112704918","0.048960386","0.022516468",
			"0.01184951","0.012746067","0.007222986","-0.029581606","-0.068315931","-0.068956675","-0.055775751");
	private List<String> cfgYIntercept = Arrays.asList("-0.215582134","0.103140528","0.261804034",
			"0.33647274","0.329300284","0.379008009","0.747053936","1.173131512","1.180820432","1.009468425");

	/** This map should be populated from configured values */
	private List<WindSpeedband> windSpeedbands = new ArrayList<WindSpeedband>();
	private List<Double> slope = new ArrayList<Double>();
	private List<Double> yIntercept = new ArrayList<Double>();

	/**
	 * Constructor
	 */
	public WindTurbineEfficiencyCurve() {
		initialize();
	} // WindFarmEfficiencyCurve()

	private void initialize() {

		// write code here to populate the mapWindSpeedToEfficiency
		for (int i = 0; i < cfgWindSpeedbands.size(); i++) {
			String from_to = cfgWindSpeedbands.get(i);
			String[] fromtoarray = from_to.split("-");
			WindSpeedband wspb = new WindSpeedband(
					Double.valueOf(fromtoarray[0]),
					Double.valueOf(fromtoarray[1]));
			this.windSpeedbands.add(wspb);

			this.slope.add(Double.valueOf(cfgSlope.get(i)));
			this.yIntercept.add(Double.valueOf(cfgYIntercept.get(i)));

		}
	}

	/**
	 * get efficiency for given wind speed in m/sec
	 * 
	 * @param windSpeed
	 *            wind speed in m/sec
	 * @return efficiency
	 */
	public double getEfficiency(double windSpeed) {
		int indexInList = -1;
		for (int i = 0; i < windSpeedbands.size(); i++) {
			if (windSpeedbands.get(i).isWithinSpeedband(windSpeed)) {
				indexInList = i;
				break;
			}
		}
		if (indexInList > -1) {
			double m = slope.get(indexInList);
			double b = yIntercept.get(indexInList);
			return (m * windSpeed + b);
		} else {
			return 0;
		}
	} // get efficiency

} // class WindFarmEfficiencyCurve

