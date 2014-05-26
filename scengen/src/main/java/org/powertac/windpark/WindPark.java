package org.powertac.windpark;

/** class WindPark: represents a wind park
 * 
 * @author Shashank Pande
 *
 */
public class WindPark {
	private  int numOfTurbines = 100; //number of turbines
	private double turbineCapacity = 1.5; //MW
	private double cutInSpeed = 4; //meters per second
	private double cutOutSpeed = 25; //meters per second
	private double maxPowerOutputSpeed = 14; //meters per second
	private double sweepAreaOfTurbine = 2391.2; // square meters
	private double airPressure = 100978.449; //Newtons per meter square (N/m^2)
	private WindTurbineEfficiencyCurve effCurve = new WindTurbineEfficiencyCurve();
	
	public WindPark() {}

	public double getPowerOutput(double tempInCentigrade, double windSpeed) {
		double airDensity = WindPark.getDryAirDensity(airPressure, tempInCentigrade);
		if (windSpeed < cutInSpeed) {
			return 0;
		} else if ((windSpeed >= maxPowerOutputSpeed)
				&& (windSpeed < cutOutSpeed)) {
			return (this.turbineCapacity * this.numOfTurbines);
		} else if (windSpeed > this.cutOutSpeed) {
			return 0;
		} else {
			double powerOutput = 0;
			double efficiency = effCurve.getEfficiency(windSpeed);
			powerOutput = 0.5 * efficiency * sweepAreaOfTurbine * airDensity
					* Math.pow(windSpeed, 3) * this.numOfTurbines;
			return powerOutput / 1000000; // convert Watts to MW
		}
	} //getPowerOutput()
	
	public static double getDryAirDensity(double airPressure, double tempInCentigrade) {
		double T = tempInCentigrade + 273.15; // temp in deg Kelvin
		double R = 287.05; // Specific gas constant for dry air J/kg.K

		double airDensity = airPressure / (R * T);
		return airDensity;
	}
}
