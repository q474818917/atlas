package com.dwarf.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * http://120.25.163.237:9200/twitter/tweet/_mapping
 * @author jiyu
 *
 */
public class HandleMapping {
	
	public static void main(String[] args) {
		new HandleMapping().putMapping();
	}
	
	public void putMapping(){
		CreateIndexRequestBuilder createRequestBuilder = ESAction.getInstance().client.admin().indices().prepareCreate("twitter");
		
		try {
			XContentBuilder mappingBuilder = jsonBuilder().startObject()
					.startObject("tweet")
			        	.startObject("properties")
			        		.startObject("name").field("type", "string").field("store", "yes").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
			        		.startObject("text").field("type", "string").field("store", "yes").field("copy_to", "name").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
			        		.startObject("sex").field("type", "boolean").field("store", "yes").endObject()
			        		.startObject("age").field("type", "integer").field("store", "yes").field("copy_to", "name").endObject()
			        		.startObject("birtyday").field("type", "date").field("store", "yes").endObject()
			        		/*.startObject("manager")
			        			.startObject("properties")
			        				.startObject("age").field("type", "integer").endObject()
			        				.startObject("name")
			        					.startObject("properties")
			        						.startObject("first").field("type", "string").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
			        						.startObject("last").field("type", "string").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
			        					.endObject()
			        				.endObject()
			        			.endObject()
			        		.endObject()*/
			        	.endObject()
			        .endObject()
			.endObject();
			createRequestBuilder.addMapping("tweet", mappingBuilder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		CreateIndexResponse createIndexResponse = createRequestBuilder.execute().actionGet();
		System.out.println(createIndexResponse.isAcknowledged());
		
	}
	
	public void updateMapping(){
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject().
					startObject("tweet").
						startObject("properties").
							startObject("text").field("type", "string").field("store", "yes").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject().
						endObject().
					endObject().
			endObject();
		    PutMappingRequest mapping = Requests.putMappingRequest("twitter").type("tweet").source(builder);
		    ESAction.getInstance().client.admin().indices().putMapping(mapping).actionGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
