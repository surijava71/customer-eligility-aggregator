package com.healthfirst.amazonws.kafka.aggregator.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Producer {
	
	private static final Logger log = LogManager.getLogger(Producer.class);
	String bootstrapServers = "b-1.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098,"
			+ "b-2.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098,"
			+ "b-3.ods-kafka.v06nbd.c12.kafka.us-east-1.amazonaws.com:9098";
	String keystoreLocation = "/home/hadoop/eligibility/kafka.client.keystore.jks";
	String truststoreLocation = "/home/hadoop/eligibility/cacerts";
	String keystorePassword = "kafka123";
	String truststorePassword = "kafka123";
	KafkaProducer<String, String> producer = null;
	
	public Producer() {
		log.info("In Producer - Constructor Start..");
		
		/*
		 * Configure the Properties
		 */
		Properties config = new Properties();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
		config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
		config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keystorePassword);
		config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
		
		/*
		 * Create the Producer
		 */
		producer = new KafkaProducer<String, String>(config);
		
		log.info("In Producer - Complete Constructor..");
		
	}

	public void publish(String topicName, String valueString) throws InterruptedException {
		log.info("In Producer - publish Start..");
		/**
		 * Create producer record
		 */
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, valueString);
		
		log.info("In Producer - publish: before send");
		log.info("In Producer - publish: topic name: " + topicName);
		log.info("In Producer - publish: value string: " + valueString);
		
		/*
		 * Send record to topic
		 */
		producer.send(producerRecord);
		/*
		 * Close the producer to flush the record
		 */
		producer.close();
		
		log.info("In Producer - publish: after send");		
	}
}