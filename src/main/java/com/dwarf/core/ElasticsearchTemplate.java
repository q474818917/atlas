package com.dwarf.core;

import static org.elasticsearch.client.Requests.indicesExistsRequest;

import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dwarf.exception.ElasticsearchException;

public class ElasticsearchTemplate implements ElasticsearchOperations {
	
	private static Logger logger = LoggerFactory.getLogger(ElasticsearchTemplate.class);
	private Client client;
	
	public ElasticsearchTemplate(Client client) {
		this.client = client;
	}

	@Override
	public boolean createIndex(String indexName) {
		logger.info("ElasticsearchTemplate createIndex param is {}", indexName);
		return client.admin().indices()
				.create(Requests.createIndexRequest(indexName))
				.actionGet().isAcknowledged();
	}

	@Override
	public boolean createIndex(String indexName, Object settings) {
		logger.info("ElasticsearchTemplate createIndex param indexName is {}, settings is {}", indexName, settings);
		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
		if (settings instanceof String) {
			createIndexRequestBuilder.setSettings(String.valueOf(settings));
		} else if (settings instanceof Map) {
			createIndexRequestBuilder.setSettings((Map) settings);
		} else if (settings instanceof XContentBuilder) {
			createIndexRequestBuilder.setSettings((XContentBuilder) settings);
		}
		return createIndexRequestBuilder.execute().actionGet().isAcknowledged();
	}

	@Override
	public boolean putMapping(String indexName, String type, Object mapping) {
		logger.info("ElasticsearchTemplate putMapping param indexName is {}, type is {}, mapping is {}", indexName, type, mapping);
		PutMappingRequestBuilder requestBuilder = client.admin().indices()
				.preparePutMapping(indexName).setType(type);
		if (mapping instanceof String) {
			requestBuilder.setSource(String.valueOf(mapping));
		} else if (mapping instanceof Map) {
			requestBuilder.setSource((Map) mapping);
		} else if (mapping instanceof XContentBuilder) {
			requestBuilder.setSource((XContentBuilder) mapping);
		}
		return requestBuilder.execute().actionGet().isAcknowledged();
	}

	@Override
	public Map getMapping(String indexName, String type) {
		logger.info("ElasticsearchTemplate getMapping param indexName is {}, type is {}", indexName, type);
		Map mappings = null;
		try {
			mappings = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName).types(type))
					.actionGet().getMappings().get(indexName).get(type).getSourceAsMap();
		} catch (Exception e) {
			throw new ElasticsearchException("Error while getting mapping for indexName : " + indexName + " type : " + type + " " + e.getMessage());
		}
		return mappings;
	}

	@Override
	public Map getSetting(String indexName) {
		logger.info("Elasticsearch getSetting param indexName is {}", indexName);
		return client.admin().indices().getSettings(new GetSettingsRequest())
				.actionGet().getIndexToSettings().get(indexName).getAsMap();
	}

	@Override
	public String delete(String indexName, String type, String id) {
		logger.info("Elasticsearch delete param indexName is {}, type is {}, id is {}", indexName, type, id);
		return client.prepareDelete(indexName, type, id).execute().actionGet().getId();
	}

	@Override
	public boolean deleteIndex(String indexName) {
		logger.info("Elasticsearch deleteIndex param indexName is {}", indexName);
		if (indexExists(indexName)) {
			return client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet().isAcknowledged();
		}
		return false;
	}

	@Override
	public boolean indexExists(String indexName) {
		logger.info("Elasticsearch indexExists param indexName is {}", indexName);
		return client.admin().indices().exists(indicesExistsRequest(indexName)).actionGet().isExists();
	}

}
