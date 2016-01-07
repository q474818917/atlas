package com.dwarf.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jiyu
 *
 */
public class ESAction {
	
	private static Logger logger = LoggerFactory.getLogger(ESAction.class);
	
	private static ESAction _ESAction;
	
	private static Properties prop = new Properties();
	
	static {
		InputStream in = ESAction.class.getClassLoader().getResourceAsStream("server.properties");
		try {
			prop.load(in);
			logger.info("server.properties loading succeed");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Client client;
	
	private ESAction(){
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", prop.getProperty("es.clusterName")).build();
		
		TransportClient tsClient = TransportClient.builder().settings(settings).build();
		for(int i = 0; i < prop.getProperty("es.server").split(",").length; i++){
			try {
				tsClient.addTransportAddress(new InetSocketTransportAddress(
						InetAddress.getByName(prop.getProperty("es.server").split(",")[i]), Integer.parseInt(prop.getProperty("es.port").split(",")[i])));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		
		client = tsClient;
	}
	
	public static ESAction getInstance(){
		if(_ESAction == null){
			_ESAction = new ESAction();
		}
		return _ESAction;
	}
	
	
	//QueryBuilders.matchQuery(field, text);
	
	public static void main(String[] args) throws IOException {
		/*ESAction.getInstance().update("twitter", "tweet", "1");
		try {
			Thread.sleep(14400);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		
		
		/*SearchResponse response = ESAction.getInstance().client.prepareSearch("index")
		        .setTypes("fulltext")
		        .setQuery(QueryBuilders.matchQuery("content", "中国"))                 
		        .execute()
		        .actionGet();
		
		SearchHits searchHits = response.getHits();
		for(SearchHit hit : searchHits.getHits()){
			System.out.println(hit.getSourceAsString());
		}*/
		
		/*BulkRequestBuilder bulkRequest = ESAction.getInstance().client.prepareBulk();
		long t1 = System.currentTimeMillis();
		for(int i = 0; i < 1000; i ++){
			bulkRequest.add(client.prepareIndex("twitter", "tweet", i + "")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("name", "kimchy" + i)
			                    .endObject()
			                  )
			        );
		}
		BulkResponse bulkResponse = bulkRequest.get();
		long t2 = System.currentTimeMillis();
		System.out.println("spend time is " + (t2 -t1) + "->" + bulkResponse.hasFailures());*/
		
		ESAction.getInstance().add("twitter", "tweet", "1");
	}
	
	public Object get(String index, String type, String idx){
		GetResponse response = client.prepareGet(index, type, idx).get();
		System.out.println(response.getSource());
		return response;
	}
	
	public boolean add(String index, String type, String idx){
		try {
			IndexResponse response = ESAction.getInstance().client.prepareIndex(index, type, idx)
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("name", "王振兴")
			                        .field("text", "ing")
			                        .field("age", 27)
			                        .field("birthday", new Date())
			                        .field("sex", false)
			                        //.field("manager.age", 30)
			                        //.field("manager.name.first", "王")
			                        //.field("manager.name.last", "尼玛")
			                    .endObject()
			                  )
			        .get();
			System.out.println(response.isCreated());
			return response.isCreated();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public boolean delete(String index, String type, String idx){
		DeleteResponse response = client.prepareDelete(index, type, idx).get();
		System.out.println(response.isFound());
		return !response.isFound();
	}
	
	public boolean update(String index, String type, String idx){
		try{
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(index);
			updateRequest.type(type);
			updateRequest.id(idx);
			updateRequest.doc(jsonBuilder()
			        .startObject()
			            .field("user", "wangzx")
			        .endObject());
			UpdateResponse response = client.update(updateRequest).get();
			System.out.println(response.getShardInfo().getSuccessful());
		}catch(IOException e){
			e.printStackTrace();
		}catch(ExecutionException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		return false;
	}
	
}
