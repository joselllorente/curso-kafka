package es.joseluisllorente.kafka.vehicles.utils;

import java.util.Random;

public class Utils {
	
	public static double generateRandom (int rangeMin, int rangeMax) {
		Random r = new Random();
		double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();
		
		return randomValue;
	}
}
