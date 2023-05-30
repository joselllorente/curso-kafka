package es.joseluisllorente.kafka.advanced.topics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.joseluisllorente.kafka.advanced.beans.Topic;

public class TopicUtils {
	static Logger logger = LoggerFactory.getLogger(TopicUtils.class);
	
	public static AdminClient createAdminClient(String url, String port) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, url.concat(":").concat(port));
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient(String bootstrapserver) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient(List<String> bootstrapservers) {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static AdminClient createAdminClient() {
		List<String> bootStrapServers = new ArrayList<String>();
		bootStrapServers.add("localhost:9092");
		//bootStrapServers.add("localhost:9093");
		//bootStrapServers.add("localhost:9094");
		
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		AdminClient admin = AdminClient.create(config);
		
		return admin;
	}
	
	public static void deleteAllTopics(AdminClient admin) {
		logger.info("Deleting all topics");
		List<String> topicNames = getTopicsName(admin);
		topicNames.stream().filter(topicName->!topicName.startsWith("_")).forEach(topicName->deleteTopic(admin,topicName));
		//deleteTopic(admin,topicNames);
	}
	
	public static void deleteTopic(AdminClient admin, List<String> topicsToDelete) {
		logger.info("Deleting topics "+topicsToDelete);
		try {
			admin.deleteTopics(topicsToDelete);
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void deleteTopic(AdminClient admin, String topicToDelete) {
		logger.info("Deleting topic "+topicToDelete);
		List<String> topicsToDelete = new ArrayList<String>();
		topicsToDelete.add(topicToDelete);
		try {
			admin.deleteTopics(topicsToDelete);
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void createTopic (AdminClient admin, String topicName) {  
		createTopic(admin,topicName,1,1);
	}
	
	public static void createTopic (AdminClient admin, String topicName, int noOfPartitions, int noOfReplication) {
		Topic topic = new Topic(topicName,noOfPartitions,noOfReplication);
		createTopic(admin, topic);
	}
	
	public static void createTopic (AdminClient admin, Topic topic) {
		logger.info("Creating topic "+topic);
		NewTopic newTopic = new NewTopic(topic.getName(), topic.getNumPartitions(), (short) topic.getRepFactor());
		try {
			admin.createTopics(Collections.singleton(newTopic));
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	
	/**
	 * 
	 * @param admin
	 * @param topicName
	 * @param noOfPartitions
	 * @param noOfReplication
	 * @return
	 */
	public static boolean topicExist (AdminClient admin, String topicName) {
        return TopicUtils.getTopicsName(admin).contains(topicName);
	}
	
	public static void listTopics (AdminClient admin) {
		// listing
		logger.info("-- listing --");
		try {
			admin.listTopics().names().get().forEach(System.out::println);
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static void listTopics () {
		// listing
		logger.info("-- listing --");
		try {
			createAdminClient().listTopics().names().get().forEach(System.out::println);
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
	}
	
	public static List<String> getTopicsName (AdminClient admin) {
		// listing
		List<String> topics = new ArrayList<String>();
		logger.info("-- getting Topics names --");
		try {
			
			admin.listTopics().names().get().forEach((name)->topics.add(name));
		}catch (InterruptedException e) {
			logger.error("Error" + e.getMessage());
		}catch (ExecutionException e) {
			logger.error("Error" + e.getMessage());
		}
		return topics;
	}
}
