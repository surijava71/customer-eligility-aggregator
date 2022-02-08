package com.healthfirst.amazonws.kafka.aggregator.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;

public class DynamoDBUtils {

	private static final Logger log = LogManager.getLogger(DynamoDBUtils.class);
	
	private DynamoDB dynamoDb;
	private String TABLE_NAME = "ods-ddb-eligibility";
	private Regions REGION = Regions.US_EAST_1;
	private String PRIMARY_KEY = "hf_customer_master_id_cd#hf_member_num_cd";
	private String SORT_KEY = "KAFKA_PROCESS_NAME#customer_eligibility_span_fk#trigger_timestamp#provider_pk";

	public DynamoDBUtils() {
		log.info("Initialize Dynamo DB Client");
		AmazonDynamoDBClient client = new AmazonDynamoDBClient();
		client.setRegion(Region.getRegion(REGION));
		this.dynamoDb = new DynamoDB(client);
	}

	public Item getItem(String primaryKey, String sortKey) {
		log.info("Get Item Start...");
		Table table = dynamoDb.getTable(TABLE_NAME);
		log.info("Dynamo DB Table Name: " + table.getTableName());
		log.info("Primary Key Value: " + primaryKey);
		log.info("Sort Key Value: " + sortKey);
		//Item item = table.getItem(PRIMARY_KEY, primaryKey);
		
		Item item = table.getItem(PRIMARY_KEY, primaryKey, SORT_KEY, sortKey);
		if(item != null)
			log.info("Item: " + item.toString());
		else
			log.info("Item not found");
		log.info("Get Item End...");
		return item;
	}

	public PutItemOutcome updateItem(Item item) {
		log.info("Persist to dynamo db Start...");
		Table table = dynamoDb.getTable(TABLE_NAME);
		log.info("Dynamo DB Table Name: " + table.getTableName());
		
		PutItemOutcome putItemOutcome = table.putItem(new PutItemSpec().withItem(item));
		log.info("Persist to dynamo db End...");
		return putItemOutcome;
	}
}