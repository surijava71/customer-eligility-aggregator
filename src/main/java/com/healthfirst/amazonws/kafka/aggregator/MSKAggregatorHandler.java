package com.healthfirst.amazonws.kafka.aggregator;

import java.util.Base64;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.healthfirst.amazonws.kafka.aggregator.producer.Producer;
import com.healthfirst.amazonws.kafka.aggregator.util.AggregateUtils;

public class MSKAggregatorHandler implements RequestHandler<KafkaEvent, String> {

	private static final Logger log = LogManager.getLogger(MSKAggregatorHandler.class);
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	String customer_eligibility_topic = "ods-customer-eligibility";
	Producer producer = null;
	AggregateUtils aggregateUtils = null;

	public MSKAggregatorHandler() {
		aggregateUtils = new AggregateUtils();
		producer = new Producer();
	}

	@Override
	public String handleRequest(KafkaEvent kafkaEvent, Context context) {

		String response = "200 SUCCESS";
		/**
		 * topic name from the event is the event key
		 */
		String eventKey = null;
		List<KafkaEventRecord> kafkaEventRecords = null;

		log.info("In Aggrerator handleRequest - Kafka Event: \n", kafkaEvent.toString());
		log.info("In Aggrerator handleRequest - Kafka Event Json: {} \n", gson.toJson(kafkaEvent));

		if (kafkaEvent.getRecords().keySet() != null)
			eventKey = kafkaEvent.getRecords().keySet().stream().findFirst().get();
		if (eventKey == null)
			return "400 Event Key is NULL - Stream doesn't have topic name to read events";

		log.info("In Aggrerator handleRequest - Source Topic :: " + eventKey);

		/*
		 * Read the records for the topic
		 */
		kafkaEventRecords = kafkaEvent.getRecords().get(eventKey);

		for (KafkaEventRecord kafkaEventRecord : kafkaEventRecords) {
			byte[] decodedBytes = Base64.getDecoder().decode(kafkaEventRecord.getValue());
			String decodedString = new String(decodedBytes);

			log.info("In Aggrerator handleRequest - Decoded Event Message: \n", decodedString);

			JsonObject json = null;

			try {
				json = new Gson().fromJson(decodedString, JsonObject.class);
				log.info("In Aggrerator handleRequest - Json Message : {} \n", json);
			} catch (Exception e) {
				log.error(e.getMessage());
				return "400 Message formate Error";
			}

			String aggregatedJson = null;
			try {
				/**
				 * Add or update record to Dynamo DB and return the aggregated JSON string
				 */
				aggregatedJson = aggregateUtils.aggregate(json);
				log.info("In Aggrerator handleRequest - Aggregated Json Message : {} \n", json);
				if (aggregatedJson != null) {
					producer.publish(customer_eligibility_topic, aggregatedJson);
				} else {
					log.error("Aggregated Json have issues, Review the aggregation logic and update");
				}
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e.getMessage());
			}
		}
		return response;
	}
}