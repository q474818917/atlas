package com.dwarf.utils;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;

public class HandleIndex {

	public static void main(String[] args) {
		new HandleIndex().deleteIndex("twitter");
	}
	
	public void deleteIndex(String index){
		DeleteIndexResponse deleteIndexResponse = ESAction.getInstance().client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
		System.out.println(deleteIndexResponse.isAcknowledged());
	}

}
