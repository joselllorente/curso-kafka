package es.joseluisllorente.kafka.vehicles;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import es.joseluisllorente.kafka.vehicles.pojos.Vehicle;

public class FleetProducer  {
	private final KafkaProducer<String,String> producer;
    private final String topic;
    private final Boolean isAsync;
    public static String kafka_server = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";
    
    public FleetProducer(String topic, Boolean isAsync) {
    	System.out.println("Conectando al servidor "+kafka_server+":"+KAFKA_SERVER_PORT);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka_server + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }
    
    public FleetProducer(String topic, Boolean isAsync, String server) {
    	kafka_server=server;
    	
    	System.out.println("Conectando al servidor "+kafka_server+":"+KAFKA_SERVER_PORT);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafka_server + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        producer = new KafkaProducer<String,String>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }
    
    public void sendKafkaEvent (Vehicle vehicle) {
    		String className = vehicle.getClass().getSimpleName();
            String messageStr = className+vehicle.getId()+ " en [" + vehicle.getGeoPoint().getLongitude() + "," + vehicle.getGeoPoint().getLatitude() + "]";
            String id = className.concat(String.valueOf(vehicle.getId()));
            String topic = "topic".concat(className);
            
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord(topic, id, messageStr), new DemoCallBack(startTime, id, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord(topic, id, messageStr)).get();
                    System.out.println("Sent message: (" + id + ", " + messageStr + ") to topic "+topic);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    // handle the exception
                }
            }
    }
}



class DemoCallBack implements Callback {
    private final long startTime;
    private final String id;
    private final String message;
    public DemoCallBack(long startTime, String id, String message) {
        this.startTime = startTime;
        this.id = id;
        this.message = message;
    }
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + id + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
