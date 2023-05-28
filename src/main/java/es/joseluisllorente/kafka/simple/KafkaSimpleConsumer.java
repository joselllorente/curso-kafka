package es.joseluisllorente.kafka.simple;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaSimpleConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
        
        consumer.subscribe(Arrays.asList(Topics.TOPIC_IMPAR,Topics.TOPIC_PAR));
        
        int counter = 0;
        while (counter <= 1000) {
        	//Especifica que el consumidor esperará como máximo 100 milisegundos para recibir mensajes antes de retornar
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> particiones = recs.partitions();
//            for (TopicPartition topicPartition : particiones) {
//            	System.out.printf("Topic %s Particion: %s", topicPartition.topic(),topicPartition.partition());
//            	System.out.println();
//			}

            
            if (recs.count() != 0) {
            	for (ConsumerRecord<String, String> rec : recs) {
                    //System.out.printf("Recieved %s: %s", rec.key(), rec.value());
            		System.out.printf("Topic %s Particion: %s", rec.topic(),rec.partition());
            		System.out.printf("Recibido %s", rec.value());
            		
            		System.out.println();
                }
            }
            counter++;
        }

	}

}
