package es.joseluisllorente.kafka.vehicles.pojos;

import es.joseluisllorente.kafka.vehicles.GeographicPoint;

public class Vehicle extends Thread {
	
	int id;
	GeographicPoint geoPoint;
	long period =  10000 + (long) (Math.random() * (50000 - 10000));
	
	public Vehicle(int id) {
		super();
		this.id = id;
		//System.out.println("Vehiculo con id "+id + " creado");
	}
	
	public Vehicle(int id, GeographicPoint geoPoint) {
		super();
		this.id = id;
		this.geoPoint = geoPoint;
	}
	
	public GeographicPoint getGeoPoint() {
		return geoPoint;
	}
	public void setGeoPoint(GeographicPoint geoPoint) {
		this.geoPoint = geoPoint;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+ "[id=" + id + ", geoPoint=" + geoPoint + "]";
	}
}
