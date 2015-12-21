package com.dwarf.core;

import java.util.Map;


public interface ElasticsearchOperations {
	
	boolean createIndex(String indexName);
	
	boolean createIndex(String indexName, Object settings);
	
	<T> boolean putMapping(Class<T> clazz, Object mapping);
	
	boolean putMapping(String indexName, String type, Object mapping);
	
	Map getMapping(String indexName, String type);
	
	Map getSetting(String indexName);
	
	String delete(String indexName, String type, String id);
	
	boolean deleteIndex(String indexName);
	
	boolean indexExists(String indexName);
	
}
