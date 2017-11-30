package com.dwarf;

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

public class App {
	
	public static void main(String[] args) {
		String url = URLDecoder.decode("https://r1---sn-ogueln7k.googlevideo.com/videoplayback?expire=1511957729&initcwndbps=1362500&ipbits=0&key=yt6&lmt=1508527464741267&dur=216.409&itag=140&gir=yes&sparams=clen%2Cdur%2Cei%2Cgir%2Cid%2Cinitcwndbps%2Cip%2Cipbits%2Citag%2Ckeepalive%2Clmt%2Cmime%2Cmm%2Cmn%2Cms%2Cmv%2Cpl%2Crequiressl%2Csource%2Cexpire&mime=audio%2Fmp4&keepalive=yes&id=o-AFIH8RhwpKBhTvsYsz8WwPXtS2vDwM2oE4gV2vmqY6uP&mn=sn-ogueln7k&mm=31&ip=47.74.9.109&pl=21&mv=m&mt=1511936016&ms=au&ei=gVAeWqHrINLlqAH-urDwBw&signature=830214D6D5698DC0893A2E67D9EAD2DFE21E92FE.4A90CFE583E5EA73DADFC893C9BB942065A39390&source=youtube&clen=3438162&requiressl=yes&ratebypass=yes");
		CloseableHttpClient httpclient = HttpClients.createDefault();
		
		HttpGet httpGet = new HttpGet(url);
		try {
			CloseableHttpResponse response = httpclient.execute(httpGet);
			HttpEntity httpEntity = response.getEntity();
			System.out.println(httpEntity);
			byte[] b = EntityUtils.toByteArray(httpEntity);
			FileOutputStream fileOut = new FileOutputStream("2.MP4"); // 保存的文件名
			fileOut.write(b);
			fileOut.flush();
			fileOut.close();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
	
}
