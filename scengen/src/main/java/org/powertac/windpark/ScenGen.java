package org.powertac.windpark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.powertac.windpark.Scenario.ScenarioValue;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Scenario Generator This utility generates wind speed forecast error scenarios
 * from ARMA(1,1) model.
 */
public class ScenGen {
	
	private static final String errorScenarioFile = "/home/shashank/Downloads/WindSpeedForecastErrorScenMasonCity.xml";
	private static final String wsForecastFile = "/home/shashank/Downloads/minneapolis/minneapolis1.xml";
	private static final String wpScenarioFile = "/home/shashank/Downloads/WindPowerScenarios.xml";
	
	private int numberOfScenarios;
	private double alpha;
	private double beta;
	private double sigmaz;

	Scenarios windSpeedForecastErrorScenarios;
	
	public ScenGen(int num, double a, double b, double s) {
		this.numberOfScenarios = num;
		this.alpha = a;
		this.beta = b;
		this.sigmaz = s;
		windSpeedForecastErrorScenarios = new Scenarios();
	}
	
	public void generate() {
		
		//get a random number generator with zero mean and sigmaz standard deviation
		RandomNumberGenerator randomGen = new RandomNumberGenerator(0.0,this.sigmaz);
		
		float probability = (float)1/this.numberOfScenarios;
		
		for (int s = 0; s < this.numberOfScenarios; s++) {
	
			double xk1 = 0; //previous value of x
			double xk = 0; //current value of x
			double zk = 0; //current value of z
			double zk1 = 0; //previous value of z
			// create instance of scenario
			Scenario scen = new Scenario(s+1, probability);
			for (int h = 0; h < 24; h++) {
				//save previous values
				xk1 = xk;
				zk1 = zk;
				// get fresh zk
				zk = randomGen.nextGaussian();
				xk = (this.alpha * xk1) + zk + (this.beta * zk1);
				// create scenario value instance
				Scenario.ScenarioValue sv = new Scenario.ScenarioValue(h+1, xk);
				scen.addValue(sv);
			} // for each hour
			
			//add scenario to collection
			this.windSpeedForecastErrorScenarios.addScenario(scen);
			
		} //for each scenario
		
	}
	
	/**
	 * entry point for Scenario Generator Application
	 * @param args
	 */
	public static void main(String[] args) {
		// Read ARMA Series Parameters
		// TODO: read from XML file
		OptionParser optParser = new OptionParser();
		
		OptionSpec<Integer> optScen = optParser.accepts("num").withRequiredArg().ofType(Integer.class);
		OptionSpec<Double> optA = optParser.accepts("alpha").withRequiredArg().ofType(Double.class);
		OptionSpec<Double> optB = optParser.accepts("beta").withRequiredArg().ofType(Double.class);
		OptionSpec<Double> optS = optParser.accepts("sigma").withRequiredArg().ofType(Double.class);
		
		OptionSet optSet = optParser.parse(args);
		double a = 0; 
		double b = 0; 
		double s = 0; 
		int scenNum = 10000; //default scenario number
		if (optSet.hasArgument(optScen)) {
			scenNum = optSet.valueOf(optScen);
		}
		if (optSet.hasArgument(optA) && optSet.hasArgument(optB) && optSet.hasArgument(optS)) {
			a = optSet.valueOf(optA);
			b = optSet.valueOf(optB);
			s = optSet.valueOf(optS);
			if (Math.abs(a) < 0.0000000001 || Math.abs(b) < 0.0000000001 || Math.abs(s) < 0.0000000001) {
				System.out.println("Invalid Arguments");
				return;
			}
			ScenGen scenGenerator = new ScenGen(scenNum, a, b, s);
			scenGenerator.generate();
			scenGenerator.windSpeedForecastErrorScenarios.writeToXML(errorScenarioFile);
		}
		
		//read wind speed forecast error scenario file
		Scenarios errorScen = Scenarios.getScenarios(errorScenarioFile);
		
		//read wind speed forecast (just one file supported for now)
		WsData windSpeedForecastData = WsData.getWsData(wsForecastFile);
		
		//build lead hour to wind speed forecast map
		Map<Integer, Double> mapLeadHourToWindSpeed = new HashMap<Integer, Double>();
		Map<Integer, Double> mapLeadHourToTemp = new HashMap<Integer, Double>();
		for (int i = 0; i < 24; i++) {
			double wspeed = windSpeedForecastData.getForecastWindSpeed(i+1);
			double temp   = windSpeedForecastData.getForecastTemperature(i+1);
			if (wspeed < -9999.0) {
				wspeed = 0;
				temp = 0;
			}
			mapLeadHourToWindSpeed.put(i+1, wspeed);
			mapLeadHourToTemp.put(i+1, temp);
		}
		
		Scenarios windSpeedScenarios = new Scenarios();
		
		//for each error scenario, generate wind speed scenario
		for (Scenario es : errorScen.getScenarios()) {
			double p = es.getProbability();
			int sn = es.getScenarioNumber();
			List<ScenarioValue> svs = es.getValueList();
			Scenario windSpeedForecastScenario = new Scenario(sn, p);
			for (ScenarioValue sv : svs) {
				int hr = sv.getHour();
				double err = sv.getValue();
				// get wind speed forecast value for this lead hour
				double windSpeedValue = mapLeadHourToWindSpeed.get(hr) + err;
				ScenarioValue wsScenVal = new ScenarioValue(hr,windSpeedValue);
				windSpeedForecastScenario.addValue(wsScenVal);
			} //for each hour in the scenario
			windSpeedScenarios.addScenario(windSpeedForecastScenario);
		} //for each scenario
		
		//for each wind speed scenario compute wind power output scenario
		Scenarios powerOutputScenarios = new Scenarios();
		WindPark wpark = new WindPark(); //create a wind park instance t oget output
		for (Scenario wsp: windSpeedScenarios.getScenarios()) {
			double p = wsp.getProbability();
			int sn = wsp.getScenarioNumber();
			List<ScenarioValue> svs = wsp.getValueList();
			Scenario powerOutputScenario = new Scenario(sn, p);
			for (ScenarioValue sv: svs) {
				int hr = sv.getHour();
				double wspForecast = sv.getValue();
				double temperature = mapLeadHourToTemp.get(hr);
				double windParkOutput = wpark.getPowerOutput(temperature, wspForecast);
				ScenarioValue wpScenVal = new ScenarioValue(hr, windParkOutput);
				powerOutputScenario.addValue(wpScenVal);
			}
			powerOutputScenarios.addScenario(powerOutputScenario);
		}
		
		//write wind power output scenarios to a file
		powerOutputScenarios.writeToXML(wpScenarioFile);
		
		System.out.println("======= Program Completed ============");
		
		return;

	} // main
	
	
} //class ScenGen
