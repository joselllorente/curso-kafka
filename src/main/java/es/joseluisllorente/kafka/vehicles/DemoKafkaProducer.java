package es.joseluisllorente.kafka.vehicles;

import es.joseluisllorente.kafka.vehicles.pojos.Taxi;
import es.joseluisllorente.kafka.vehicles.pojos.Truck;
import es.joseluisllorente.kafka.vehicles.pojos.Vehicle;

public class DemoKafkaProducer {

	public static final String TOPIC = "testTopic";
    public static void main(String[] args) {
        boolean isAsync = false;
        String server="localhost";
        Vehicle [] fleet = {new Taxi(1, 5), new Taxi(2,7), new Taxi(3,4),
        		new Truck(1, 1000d), new Truck(2,5000d), new Truck(3,500d), new Truck(4,500d),
        		new Truck(5, 1000d), new Truck(6,5000d), new Truck(7,500d), new Truck(8,500d)};
        
        if(args.length>0) {
        	server = args[0];
        }
        
        for (Vehicle vehicle : fleet) {
        	StartFleet sf = new StartFleet(vehicle,server);
        	sf.start();
        	try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    }

}
