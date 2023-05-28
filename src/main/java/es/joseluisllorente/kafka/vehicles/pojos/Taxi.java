package es.joseluisllorente.kafka.vehicles.pojos;

import es.joseluisllorente.kafka.vehicles.GeographicPoint;

public class Taxi extends Vehicle {

	int maxCapacity;
	
	public Taxi(int id, GeographicPoint geoPoint) {
		super(id, geoPoint);
	}

	public Taxi(int id) {
		super(id);
	}
	
	public Taxi(int id, int maxCapacity) {
		super(id);
		this.maxCapacity = maxCapacity;
	}	
}
