package com.dwarf.core;

import static org.elasticsearch.client.Requests.indicesExistsRequest;
import static org.elasticsearch.index.VersionType.EXTERNAL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.util.Assert;

import com.dwarf.annotations.Document;
import com.dwarf.annotations.Mapping;
import com.dwarf.core.convert.ElasticsearchConverter;
import com.dwarf.core.convert.MappingElasticsearchConverter;
import com.dwarf.core.mapping.ElasticsearchPersistentEntity;
import com.dwarf.core.mapping.SimpleElasticsearchMappingContext;
import com.dwarf.exception.ElasticsearchException;

public class ElasticsearchTemplate implements ElasticsearchOperations {
	
	private static Logger logger = LoggerFactory.getLogger(ElasticsearchTemplate.class);
	private Client client;
	private ElasticsearchConverter elasticsearchConverter;
	private ResultsMapper resultsMapper;
	
	public ElasticsearchTemplate(Client client) {
		this.client = client;
	}
	
	public ElasticsearchTemplate(String clusterName, String server, String port){
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", clusterName).build();
		
		TransportClient tsClient = TransportClient.builder().settings(settings).build();
		for(int i = 0; i < server.split(",").length; i++){
			try {
				tsClient.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(server.split(",")[i]), Integer.parseInt(port.split(",")[i])));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		this.client = tsClient;
		
		this.elasticsearchConverter = (elasticsearchConverter == null) ? new MappingElasticsearchConverter(
				new SimpleElasticsearchMappingContext()) : elasticsearchConverter;
		
		this.resultsMapper = (resultsMapper == null) ? new DefaultResultMapper(this.elasticsearchConverter.getMappingContext()) : resultsMapper;
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
	
	private IndexRequestBuilder prepareIndex(IndexQuery query) {
		try {
			String indexName = StringUtils.isBlank(query.getIndexName()) ? retrieveIndexNameFromPersistentEntity(query.getObject()
					.getClass())[0] : query.getIndexName();
			String type = StringUtils.isBlank(query.getType()) ? retrieveTypeFromPersistentEntity(query.getObject().getClass())[0]
					: query.getType();

			IndexRequestBuilder indexRequestBuilder = null;

			if (query.getObject() != null) {
				String entityId = null;
				if (isDocument(query.getObject().getClass())) {
					entityId = getPersistentEntityId(query.getObject());
				}
				// If we have a query id and a document id, do not ask ES to generate one.
				if (query.getId() != null && entityId != null) {
					indexRequestBuilder = client.prepareIndex(indexName, type, query.getId());
				} else {
					indexRequestBuilder = client.prepareIndex(indexName, type);
				}
				indexRequestBuilder.setSource(resultsMapper.getEntityMapper().mapToString(query.getObject()));
			} else if (query.getSource() != null) {
				indexRequestBuilder = client.prepareIndex(indexName, type, query.getId()).setSource(query.getSource());
			} else {
				throw new ElasticsearchException("object or source is null, failed to index the document [id: " + query.getId() + "]");
			}
			if (query.getVersion() != null) {
				indexRequestBuilder.setVersion(query.getVersion());
				indexRequestBuilder.setVersionType(EXTERNAL);
			}

			if (query.getParentId() != null) {
				indexRequestBuilder.setParent(query.getParentId());
			}

			return indexRequestBuilder;
		} catch (IOException e) {
			throw new ElasticsearchException("failed to index the document [id: " + query.getId() + "]", e);
		}
	}
	
	private String[] retrieveIndexNameFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[]{getPersistentEntityFor(clazz).getIndexName()};
		}
		return null;
	}

	private String[] retrieveTypeFromPersistentEntity(Class clazz) {
		if (clazz != null) {
			return new String[]{getPersistentEntityFor(clazz).getIndexType()};
		}
		return null;
	}
	
	private ElasticsearchPersistentEntity getPersistentEntityFor(Class clazz) {
		Assert.isTrue(clazz.isAnnotationPresent(Document.class), "Unable to identify index name. " + clazz.getSimpleName()
				+ " is not a Document. Make sure the document class is annotated with @Document(indexName=\"foo\")");
		return elasticsearchConverter.getMappingContext().getPersistentEntity(clazz);
	}
	
	private boolean isDocument(Class clazz) {
		return clazz.isAnnotationPresent(Document.class);
	}
	
	private String getPersistentEntityId(Object entity) {
		PersistentProperty idProperty = getPersistentEntityFor(entity.getClass()).getIdProperty();
		if (idProperty != null) {
			Method getter = idProperty.getGetter();
			if (getter != null) {
				try {
					Object id = getter.invoke(entity);
					if (id != null) {
						return String.valueOf(id);
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
		}
		return null;
	}

	public static String readFileFromClasspath(String url) {
		StringBuilder stringBuilder = new StringBuilder();

		BufferedReader bufferedReader = null;

		try {
			ClassPathResource classPathResource = new ClassPathResource(url);
			InputStreamReader inputStreamReader = new InputStreamReader(classPathResource.getInputStream());
			bufferedReader = new BufferedReader(inputStreamReader);
			String line;

			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line);
			}
		} catch (Exception e) {
			logger.debug(String.format("Failed to load file from url: %s: %s", url, e.getMessage()));
			return null;
		} finally {
			if (bufferedReader != null)
				try {
					bufferedReader.close();
				} catch (IOException e) {
					logger.debug(String.format("Unable to close buffered reader.. %s", e.getMessage()));
				}
		}

		return stringBuilder.toString();
	}

	@Override
	public <T> boolean putMapping(Class<T> clazz, Object mapping) {
		return putMapping(getPersistentEntityFor(clazz).getIndexName(), getPersistentEntityFor(clazz).getIndexType(), mapping);
	}

}
