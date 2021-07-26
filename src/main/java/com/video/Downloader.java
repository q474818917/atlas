package com.video;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLDecoder;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Downloader {
	
	private static Logger logger = LoggerFactory.getLogger(Downloader.class);

	private String url;
	private String title;
	private long totalSize;
	private String ext;
	
	public Downloader(String url, String title, long totalSize, String ext) {
		this.url = url;
		this.title = title;
		this.totalSize = totalSize;
		this.ext = ext;
	}
	
	public void execute() {
		String url = URLDecoder.decode(this.url);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		HttpGet httpGet = new HttpGet(url);
		try {
			CloseableHttpResponse response = httpclient.execute(httpGet);
			if(response.getStatusLine().getStatusCode() == 200) {
				logger.info("正在进入下载，此时网络正常");
				HttpEntity httpEntity = response.getEntity();
				byte[] b = EntityUtils.toByteArray(httpEntity);
				FileOutputStream fileOut = new FileOutputStream(title + "." + ext); // 保存的文件名
				fileOut.write(b);
				fileOut.flush();
				fileOut.close();
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
