package es.joseluisllorente.kafka.simple;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSimpleProducerExercise {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> data;
            String key = "impar2";
            String msg = String.format("El numero es %d ", i);
            
            if (i % 2 == 0) {
            	key = "par2";            	
            }

            System.out.println("Clave- "+key+ " valor "+ msg);
            
            data = new ProducerRecord<String, String>("topic-ejercicio", key, msg);
            producer.send(data);
            Thread.sleep(1000L);
        }
        System.out.println("Termina");
        producer.close();

	}

}
