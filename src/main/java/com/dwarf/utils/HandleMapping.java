package com.dwarf.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class HandleMapping {
	
	public static void main(String[] args) {
		new HandleMapping().updateMapping();
	}
	
	public void putMapping(){
		CreateIndexRequestBuilder createRequestBuilder = ESAction.getInstance().client.admin().indices().prepareCreate("twitter2");
		
		try {
			XContentBuilder mappingBuilder = jsonBuilder().startObject().startObject("tweet2")
			        .startObject("_ttl").field("enabled", "true").field("default", "1s").endObject().endObject()
			        .endObject();
			createRequestBuilder.addMapping("tweet2", mappingBuilder);
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
							startObject("intro").field("type", "integer").endObject().
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
