package es.joseluisllorente.kafka.advanced.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.BrokerConfigHandler;

/**
 * Producer Kafka
 *
 */
public class ProducerCallBackKeys 
{
    public static void main( String[] args )
    {
    	Logger logger = LoggerFactory.getLogger(ProducerCallBackKeys.class);
    	
    	String bootstrapServers ="localhost:9092";
    	String topicName="topic-test";
        //Properties
    	//https://kafka.apache.org/documentation/#producerconfigs
    	Properties kafkaProps = new Properties();
    	//kafkaProps.setProperty("bootstrap.servers", bootstrapServers );
    	kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
    	
    	//Le permite como tiene que realizar la serialización a bytes ya que kafka convertirá todo a bytes
    	//Como enviamos Strings
    	kafkaProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	kafkaProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	
    	//Creamos el productor
    	KafkaProducer<String, String> producer = new KafkaProducer <String,String>(kafkaProps);
    	
    	
    	for (int i =0; i<=10 ; i++) {
	    	//Creamos los registros que se enviaran
    		
    		String valor = "Enviando datos "+i;
    		String clave = "key_"+i;
    		logger.info("Enviando datos a "+bootstrapServers + " topic: "+ topicName + " clave: "+clave);
	    	ProducerRecord <String,String> record = new ProducerRecord<String, String>(topicName, clave, valor);
	    	
	    	try {
	    		//Enviamos los registros de manera SINCRONA!!! con el get() del final
	    		producer.send(record, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						//Cada vez que un registro se envia satisfactoriamente o se envia una excepción
						if (exception==null) {
							logger.info("Recibida metainformación \n" + 
								"Topic: " + metadata.topic()+ "\n" +
								"Partition: " + metadata.partition()+ "\n" +
								"Offset: " + metadata.offset()+ "\n" +
								"Timestamp: " + metadata.timestamp()+ "\n" );
							
						}else {
							logger.error("Registro no enviado " + exception.getMessage());
						}
						
					}
				}).get();//NO HACER ESTO EN PRODUCCION!!!!
	    	}catch (Exception e) {
				System.err.println("Error"+e.getMessage());
			}
    	}
//    	producer.flush();
    	producer.close();
    	logger.info("Enviados2");
    }
}
