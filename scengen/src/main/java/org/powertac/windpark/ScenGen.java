package org.powertac.windpark;

import java.io.FileWriter;
import java.io.IOException;

import com.thoughtworks.xstream.XStream;

/**
 * Scenario Generator This utility generates wind speed forecast error scenarios
 * from ARMA(1,1) model.
 */
public class ScenGen {
	
	private static final String scenarioFile = "/tmp/wsrmse/WindSpeedScenarios.xml";
	
	private int numberOfScenarios;
	private double alpha;
	private double beta;
	private double sigmaz;

	WindSpeedForecastErrorScenarios windSpeedForecastErrorScenarios;
	
	public ScenGen(int num, double a, double b, double s) {
		this.numberOfScenarios = num;
		this.alpha = a;
		this.beta = b;
		this.sigmaz = s;
		windSpeedForecastErrorScenarios = new WindSpeedForecastErrorScenarios();
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
	
	public void writeToXML() {
		XStream xstream = WindSpeedForecastErrorScenarios.getConfiguredXStream();
		String xmlStr = xstream.toXML(this.windSpeedForecastErrorScenarios);
		String fileName = scenarioFile;
		
		try {
			FileWriter fw = new FileWriter(fileName);
			fw.write(xmlStr);
			fw.write("\n");
			fw.close();
		} catch (IOException ex) {
			System.out.println(ex);
		}
		
	} //writeToXML()
	
	/**
	 * entry point for Scenario Generator Application
	 * @param args
	 */
	public static void main(String[] args) {
		// Read ARMA Series Parameters
		// TODO: read from XML file
		
		ScenGen scenGenerator = new ScenGen(5000, 0.3, 0.2, 0.67);
		
		scenGenerator.generate();
		
		scenGenerator.writeToXML();
		
		System.out.println("======= Program Completed ============");
		
		return;

	} // main
} //class ScenGen
