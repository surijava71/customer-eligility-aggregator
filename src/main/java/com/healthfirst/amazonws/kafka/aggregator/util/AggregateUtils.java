package com.healthfirst.amazonws.kafka.aggregator.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.gson.JsonObject;

public class AggregateUtils {

	private static final Logger log = LogManager.getLogger(AggregateUtils.class);
	private String PRIMARY_KEY = "hf_customer_master_id_cd#hf_member_num_cd";
	private String SORT_KEY = "KAFKA_PROCESS_NAME#customer_eligibility_span_fk#trigger_timestamp#provider_pk";
	
	public String aggregate(JsonObject json) {
		DynamoDBUtils dynamoDBUtils = new DynamoDBUtils();
		
		String custid = json.get("hf_customer_master_id_cd").getAsString();
		String memid = json.get("hf_member_num_cd").getAsString();
		String processame = json.get("KAFKA_PROCESS_NAME").getAsString(); 
		String custEligibility_span_fk = json.get("customer_eligibility_span_pk").getAsString();
		String triggerTimeStamp = json.get("trigger_timestamp").getAsString();
		String providerPK = "providerTestPK";

		String primaryKey = custid  + "#" + memid;
		String sortKey = processame + "#" + custEligibility_span_fk + "#" + triggerTimeStamp + "#" + providerPK;
		 
		Item item = null;
		item = dynamoDBUtils.getItem(primaryKey, sortKey);
		
		if(item == null) {
			/**
			 * If the record doesn't exist in table, create new one to start with...
			 */
			item = new Item();
		}

		log.info("Primary Key item : " + item.toString());
		
		List<Item> itemList = new ArrayList<Item>();
		itemList.add(item);
		json.keySet().stream().forEach(key -> {
			log.info("Item key: " + key);
			Item listItem = itemList.get(0);
			listItem = listItem.withString(key, json.get(key).getAsString());
			itemList.set(0, listItem);
		});

		/*
		 * Added below lines to set PK and SK
		 */
		Item updatedItem = itemList.get(0);
		updatedItem = updatedItem.withString(PRIMARY_KEY, primaryKey);
		updatedItem = updatedItem.withString(SORT_KEY, sortKey);
		itemList.set(0, updatedItem);
		/*
		 * End of PK and SK setting
		 */
		
		dynamoDBUtils.updateItem(itemList.get(0));
		return itemList.get(0).toJSON();
	}

}