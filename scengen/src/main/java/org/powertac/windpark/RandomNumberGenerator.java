package org.powertac.windpark;

import java.util.Random;

public class RandomNumberGenerator {

	private double second;
	private boolean secondValid = false;
	private Random javaRandom = new Random();
	private double mean = 0.0; //default mean
	private double std = 1.0; // default standard deviation

	public RandomNumberGenerator() {
	}

	public RandomNumberGenerator(double mean, double std) {
		this.mean = mean;
		this.std = std;
	}

	double nextGaussian() {
		double v1, v2, y1, y2, x1, x2, w;

		if (secondValid) {
			secondValid = false;
			return second;
		}

		do {
			v1 = 2 * javaRandom.nextDouble() - 1;
			v2 = 2 * javaRandom.nextDouble() - 1;
			w = v1 * v1 + v2 * v2;
		} while (w > 1);

		y1 = v1 * Math.sqrt(-2 * Math.log(w) / w);
		y2 = v2 * Math.sqrt(-2 * Math.log(w) / w);
		x1 = mean + y1 * std;
		x2 = mean + y2 * std;
		second = x2;
		secondValid = true;
		return x1;
	}
} //class RandomNumberGenerator
