package es.joseluisllorente.kafka.vehicles.pojos;

import es.joseluisllorente.kafka.vehicles.GeographicPoint;

public class Truck extends Vehicle {

	Double tonnage;
	
	public Truck(int id, GeographicPoint geoPoint) {
		super(id, geoPoint);
	}

	public Truck(int id) {
		super(id);
	}
	
	public Truck(int id, Double tonnage) {
		super(id);
		this.tonnage = tonnage;
	}

	public Double getTonnage() {
		return tonnage;
	}

	public void setTonnage(Double tonnage) {
		this.tonnage = tonnage;
	}
	
	
}
