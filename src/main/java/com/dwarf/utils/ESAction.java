package com.dwarf.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	public static void main(String[] args) throws IOException {
		
		IndexResponse response = ESAction.getInstance().client.prepareIndex("twitter", "tweet", "1")
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("user", "kimchy")
		                        .field("postDate", new Date())
		                        .field("message", "trying out Elasticsearch")
		                    .endObject()
		                  )
		        .get();
		
		try {
			Thread.sleep(14400);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
