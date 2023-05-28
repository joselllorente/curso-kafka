package es.joseluisllorente.kafka.simple;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;

import kafka.common.KafkaException;

public class KafkaTopicList {
	public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true); // Incluye los topics internos

            ListTopicsResult topicsResult = adminClient.listTopics(options);
            KafkaFuture<Collection<TopicListing>> topicsFuture = topicsResult.listings();

            Collection<TopicListing> topicListings = topicsFuture.get();
            for (TopicListing topicListing : topicListings) {
                System.out.println("Topic: " + topicListing.name());
            }
        } catch (InterruptedException | ExecutionException | KafkaException e) {
            e.printStackTrace();
        }
    }
}
