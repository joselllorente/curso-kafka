package es.joseluisllorente.kafka.simple;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


/*
 * kafka-topics --bootstrap-server localhost:9092 --create -topic topic-par --partitions 3 --replication-factor 1
 * kafka-topics --bootstrap-server localhost:9092 --create -topic topic-impar --partitions 3 --replication-factor 1
 */
public class KafkaSimpleProducer {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> data;
            String topic = Topics.TOPIC_IMPAR;
            String msg = String.format("%d es impar", i);
            if (i % 2 == 0) {
            	topic = Topics.TOPIC_PAR;
            	msg = String.format("%d es par", i);
            } 
            
            //data = new ProducerRecord<String, String>(topic, 0, Integer.toString(i), msg);
            System.out.println("Enviando al topic "+topic+" el mensaje "+msg); 
            data = new ProducerRecord<String, String>(topic,msg);
            producer.send(data);
            Thread.sleep(1000L);
        }
        System.out.println("Termina");
        producer.close();
	}
}
