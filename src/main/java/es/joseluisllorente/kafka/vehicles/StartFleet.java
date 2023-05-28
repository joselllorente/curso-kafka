package es.joseluisllorente.kafka.vehicles;

import es.joseluisllorente.kafka.vehicles.pojos.Vehicle;
import es.joseluisllorente.kafka.vehicles.utils.Utils;

public class StartFleet extends Thread {

	private Vehicle vehicle;
	private String server;
	long period =  10000 + (long) (Math.random() * (50000 - 10000));
	
	public StartFleet(Vehicle vehicle) {
		super();
		this.vehicle = vehicle;
	}
	
	public StartFleet(Vehicle vehicle, String server) {
		super();
		this.vehicle = vehicle;
		this.server = server;
	}
	
	public void run() {
		try {
			for (int i = 0; i < 1000; i++) {
				vehicle.setGeoPoint(new GeographicPoint(Utils.generateRandom(-90, 90), Utils.generateRandom(-90, 90)));
				//System.out.println("sfid: "+vehicle.getId());
				FleetProducer fp = new FleetProducer (vehicle.getClass().getSimpleName(), false, server);
				fp.sendKafkaEvent(vehicle);
				sleep(period);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
