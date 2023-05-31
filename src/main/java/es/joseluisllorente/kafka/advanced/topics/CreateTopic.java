package es.joseluisllorente.kafka.advanced.topics;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.joseluisllorente.kafka.advanced.beans.Topic;

public class CreateTopic {
	static Logger logger = LoggerFactory.getLogger(CreateTopic.class);
	
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		
		args = new String[]{"topic-test;3;1"};
		AdminClient admin = TopicUtils.createAdminClient();
		if(args.length==0) {
			logger.info("Creating default topic \"topic-test\",3,1");
			//TopicUtils.createTopic(admin, new Topic("topic-test",3,1));
			String topicName="topic-test";
	    	if ( !TopicUtils.topicExist(admin, topicName)){
	    		TopicUtils.createTopic(admin, topicName,3,1);
	    	}
	    	
		}else {
			for (String topic : args) {
				if (topic.contains(";")) {
					String[] topicData = topic.split(";");
					logger.info("Creating topic with data "+Arrays.toString(topicData));
					TopicUtils.createTopic(admin, new Topic(topicData[0], Integer.parseInt(topicData[1]) ,Integer.parseInt(topicData[2])) );
				}else {
					logger.info("Creating topic with name "+args[0]);
					TopicUtils.createTopic(admin, new Topic(args[0]));
				}
			}
			
		}
		Thread.sleep(1000);
		logger.info("Creating topics finished");
	}
}