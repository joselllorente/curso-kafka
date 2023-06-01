package es.joseluisllorente.kafka.streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import scala.collection.parallel.ParIterableLike.Foreach;



/*
 * kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic input-topic-csv
 * 
 * kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic output-topic-json
 */
public class StreamsStarterApp {

    public static void main(String[] args) {
    	
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-csv-tojson");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, "D://kafka-streams");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> csvToJson = builder.stream("input-topic-csv");
        
        csvToJson.mapValues( data -> {return transformData(data);} ).mapValues(data -> data.toUpperCase()).to("output-topic-json");
        //csvToJson.mapValues( data -> data.toUpperCase() ).to("output-topic-json");

        
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        //streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
    
    public static String transformData (String csv) {
    	System.out.println("Data received "+ csv);
    	String[] csvSplit = csv.split(",");
    	String transform = "{";
    	for (int i = 0; i < csvSplit.length; i++) {
    		transform+="field"+i+":"+csvSplit[i];
    		if (i!=csvSplit.length-1) {
    			transform+=",";
    		}
		}
    	transform += "}";
    	System.out.println("Data transform "+ transform);
		return transform;
    }

}
/*
 * First name, Surname, Gender, Date of Birth\\nTom, Cruise, Male, 01-01-1966\\nShahrukh, Khan, Male, 02-02-1955
*/
