package com.dmai;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.dmai.bean.User;
import com.dmai.utils.DemaiUtils;
import com.dmai.utils.DruidUtils;
import com.dwarf.utils.ESAction;

public class UserController {
	
	public static void main(String args[]){
		new UserController().termsAggregation();
	}
	
	private boolean addMapping(){
		CreateIndexRequestBuilder createRequestBuilder = ESAction.getInstance().client.admin().indices().prepareCreate("dmai");
		
		try {
			XContentBuilder mappingBuilder = jsonBuilder().startObject()
					.startObject("user")
			        	.startObject("properties")
			        		.startObject("id").field("type", "long").field("store", "yes").endObject()
			        		.startObject("flag").field("type", "integer").field("store", "yes").endObject()
			        		.startObject("cids").field("type", "long").field("store", "yes").endObject()
			        		.startObject("rank").field("type", "long").field("store", "yes").endObject()
			        		.startObject("app").field("type", "long").field("store", "yes").endObject()
			        		.startObject("sequence").field("type", "long").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("time").field("type", "long").field("store", "yes").endObject()
			        		.startObject("profession").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("month").field("type", "integer").field("store", "yes").endObject()
			        		.startObject("day").field("type", "integer").field("store", "yes").endObject()
			        		.startObject("year").field("type", "integer").field("store", "yes").endObject()
			        		.startObject("tag").field("type", "string").field("store", "yes").endObject()
			        		.startObject("name").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("text").field("type", "string").field("store", "yes").field("analyzer", "ik_max_word").field("search_analyzer", "ik_max_word").endObject()
			        		.startObject("sex").field("type", "string").field("store", "yes").endObject()
			        		.startObject("age").field("type", "string").field("store", "yes").endObject()
			        		.startObject("birtyday").field("type", "date").field("store", "yes").endObject()
			        		.startObject("constellation").field("type", "string").field("store", "yes").endObject()
			        		.startObject("zodiac").field("type", "string").field("store", "yes").endObject()
			        		.startObject("personmark").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("company").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("post").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("mytag").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("my_goodat_key").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("intro").field("type", "string").field("store", "yes").endObject()
			        		.startObject("sign").field("type", "string").field("store", "yes").endObject()
			        		.startObject("mobile").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("home").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("area").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("morearea").field("type", "string").field("store", "yes").endObject()
			        		.startObject("integrity").field("type", "integer").field("store", "yes").endObject()
			        		.startObject("content").field("type", "string").field("store", "yes").endObject()
			        		.startObject("travelgone").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("travelwantgo").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("bookdone").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("bookdoing").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("bookwantdo").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("moviewantdo").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("moviedone").field("type", "string").field("store", "yes").field("copy_to", Arrays.asList("text", "content")).endObject()
			        		.startObject("worktrack").field("type", "string").field("store", "yes").endObject()
			        		.startObject("education").field("type", "string").field("store", "yes").field("copy_to", "text").endObject()
			        		.startObject("friends").field("type", "long").field("store", "yes").endObject()
			        		.startObject("focus").field("type", "string").field("store", "yes").endObject()
			        		.startObject("flag").field("type", "integer").field("store", "yes").endObject()
			        		
			        	.endObject()
			        .endObject()
			.endObject();
			createRequestBuilder.addMapping("user", mappingBuilder);
		} catch (IOException e) {
			e.printStackTrace();
		}
		CreateIndexResponse createIndexResponse = createRequestBuilder.execute().actionGet();
		return createIndexResponse.isAcknowledged();
	}
	
	private boolean deleteIndexName(String index){
		DeleteIndexResponse deleteIndexResponse = ESAction.getInstance().client.admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
		return deleteIndexResponse.isAcknowledged();
	}
	
	private boolean bulkIndex(int pageNum){
		
		BulkRequestBuilder bulkRequest = ESAction.getInstance().client.prepareBulk();
		
		StringBuilder builder = new StringBuilder();
		builder.append("select b.*,r.*,")
			.append("(select resourcelevel from user.statinfo where tkey=b.tkey) as resourcelevel,")
			.append("(select requte from user.statinfo where tkey=b.tkey) as requte,")
			.append("(select reposts from user.statinfo where tkey=b.tkey) as reposts,")
			.append("(select group_concat(comment SEPARATOR '|||') from `user`.badgehandle h, `user`.badge n where h.uid= b.tkey and n.tkey = h.type) as badge,")
			.append("(select count(comment) from `user`.badgehandle where uid= b.tkey) as badgecount")
			.append(" from `user`.basic b, `user`.realdata r where b.tkey = r.tkey AND b.source not in (8000,8001,8002,8003,8004) limit " + ((pageNum-1)*10000) + ", 10000");
		
		Connection conn = DruidUtils.getInstance().getConnection();
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(builder.toString());
			ResultSet resultSet = pstmt.executeQuery();
			while(resultSet.next()){
				try {
					long id = resultSet.getLong("tkey");
					int flag = resultSet.getInt("flag");
					List<Long> cids = this.getUserCids(resultSet.getLong("tkey"));
					long rank = resultSet.getLong("rank");
					long app = resultSet.getLong("app");
					long sequence = resultSet.getLong("sequence");
					long time = resultSet.getLong("time");
					List<String> profession = new ArrayList<>();
					String name = resultSet.getString("name");
					
					if(StringUtils.isNotEmpty(resultSet.getString("profession"))){
						profession = this.convertIrrWord(resultSet.getString("profession"));
					}
					int month = resultSet.getInt("month");
					int year = resultSet.getInt("year");
					int day = resultSet.getInt("day");
					String tag = resultSet.getString("tag");
					
					String zodiac = "";
					String constellation = "";
					Date birthday = null;
					if(year != 0){
						String date = year + "-" + month + "-" + day;
						SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
						try {
							birthday = myFormatter.parse(date);
							zodiac = (DemaiUtils.getZodica(myFormatter.parse(date)));
							constellation = (DemaiUtils.getConstellation(myFormatter.parse(date)));
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					List<String> personmark = new ArrayList<>();
					if(StringUtils.isNotEmpty(resultSet.getString("personmark"))){
						String[] arrs = resultSet.getString("personmark").split(" ");
						List<String> strList = new ArrayList<>();
						for(String s : arrs){
							strList.add(s);
						}
						personmark = strList;
					}
					
					String company = resultSet.getString("company");
					String post = resultSet.getString("post");
					List<String> mytag = new ArrayList<>();
					if(StringUtils.isNotEmpty(resultSet.getString("mytag"))){
						mytag = DemaiUtils.convertIrrWord(resultSet.getString("mytag"));
					}
					List<String> my_goodat_key = new ArrayList<>();
					if(StringUtils.isNotEmpty(resultSet.getString("my_goodat_key"))){
						String[] arrs = resultSet.getString("my_goodat_key").split(" ");
						List<String> strList = new ArrayList<>();
						for(String s : arrs){
							strList.add(s);
						}
						my_goodat_key = strList;
					}
					
					String intro = resultSet.getString("intro");
					String mobile = resultSet.getString("mobile");
					String area = this.convertArea(resultSet.getString("area"));
					String home = resultSet.getString("home");
					String morearea = resultSet.getString("morearea");
					int integrity = resultSet.getInt("integrity");
					String sex = "";
					if(resultSet.getInt("sex") > 0){
						if(resultSet.getInt("sex") > 1){
							sex = "男";
						}else{
							sex = "女";
						}
					}
					List<String> worktrack = (this.getWorkTrack(resultSet.getLong("tkey")));
					String education = (this.getEducation(resultSet.getLong("tkey")));
					
					Map<String, List<String>> map = this.getSuperJson(resultSet.getLong("tkey"));
					List<String> traveltaggo = new ArrayList<>();
					List<String> traveltagwantgo = new ArrayList<>();
					List<String> booktagreading = new ArrayList<>();
					List<String> booktagread = new ArrayList<>();
					List<String> booktagunread = new ArrayList<>();
					List<String> videotagsee = new ArrayList<>();
					List<String> videotagwantsee = new ArrayList<>();
					List<String> musicfavorite = new ArrayList<>();
					if(map != null){
						traveltaggo = map.get("traveltaggo") == null ? new ArrayList<>() : map.get("traveltaggo");
						traveltagwantgo = map.get("traveltagwantgo")  == null ? new ArrayList<>() : map.get("traveltagwantgo");
						booktagreading = map.get("booktagreading")  == null ? new ArrayList<>() : map.get("booktagreading");
						booktagread = map.get("booktagread")  == null ? new ArrayList<>() : map.get("booktagread");
						booktagunread = map.get("booktagunread") == null ? new ArrayList<>() : map.get("booktagunread");
						videotagsee = map.get("videotagsee") == null ? new ArrayList<>() : map.get("videotagsee");
						videotagwantsee = map.get("videotagwantsee") == null ? new ArrayList<>() : map.get("videotagwantsee");
						musicfavorite = map.get("musicfavorite") == null ? new ArrayList<>() : map.get("musicfavorite");
					}
					List<Long> friendsList = this.getFollow(resultSet.getLong("tkey"), 2, 0);
					
					bulkRequest.add(ESAction.getInstance().client.prepareIndex("dmai", "user", id + "")
					        .setSource(jsonBuilder()
					                    .startObject()
					                    	.field("id", id)
					                    	.field("flag", flag)
					                    	.field("cids", cids)
					                    	.field("rank", rank)
					                    	.field("app", app)
					                    	.field("sequence", sequence)
					                    	.field("time", time)
					                        .field("name", name)
					                        .field("profession", profession)
					                        .field("month", month)
					                        .field("day", day)
					                        .field("year", year)
					                        .field("birthday", birthday)
					                        .field("zodiac", zodiac)
					                        .field("constellation", constellation)
					                        .field("personmark", personmark)
					                        .field("company", company)
					                        .field("post", post)
					                        .field("mytag", mytag)
					                        .field("my_goodat_key", my_goodat_key)
					                        .field("intro", intro)
					                        .field("mobile", mobile)
					                        .field("area", area)
					                        .field("home", home)
					                        .field("integrity", integrity)
					                        .field("morearea", morearea)
					                        .field("sex", sex)
					                        .field("worktrack", worktrack)
					                        .field("education", education)
					                        .field("travelgone", traveltaggo)
					                        .field("travelwantgo", traveltagwantgo)
					                        .field("bookdoing", booktagreading)
					                        .field("bookdone", booktagread)
					                        .field("bookwantdo", booktagunread)
					                        .field("moviedone", videotagsee)
					                        .field("moviewantdo", videotagwantsee)
					                        .field("friends", friendsList)
					                    .endObject()
					                  )
					        );
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
		
		BulkResponse bulkResponse = bulkRequest.get();
		return bulkResponse.hasFailures();
	}
	
	public void doSearch(){
		SearchResponse response = ESAction.getInstance().client.prepareSearch("dmai")
	        .setTypes("user")
	        .setQuery(QueryBuilders.matchQuery("name", "李子"))                
	        //.setPostFilter(QueryBuilders.rangeQuery("age").from(25).to(27))    
	        //.setFrom(0).setSize(60).setExplain(true)
	        .execute()
	        .actionGet();
			
		SearchHits searchHits = response.getHits();
		for(SearchHit hit : searchHits.getHits()){
			User user = JSON.parseObject(hit.getSourceAsString(), User.class);
		}
	}
	
	public void findBySearch(){
		SearchResponse response = ESAction.getInstance().client.prepareSearch("dmai")
		        .setTypes("user")
		        .setQuery(QueryBuilders.queryStringQuery("(id:1343 OR id:1381^2)"))                
		        .execute()
		        .actionGet();
				
			SearchHits searchHits = response.getHits();
			for(SearchHit hit : searchHits.getHits()){
				Map<String, Object> sourceMap = hit.getSource();
				String sourceJson = JSON.toJSONString(sourceMap);
				User user = JSON.parseObject(sourceJson, User.class);
				System.out.println(user);
			}
		
	}
	
	public void termsAggregation(){
		SearchResponse response = ESAction.getInstance().client.prepareSearch("dmai")
		        .setTypes("user")
		        .addAggregation(AggregationBuilders.terms("aggs").field("app"))
		        .execute()
		        .actionGet();
		Terms genders = response.getAggregations().get("aggs");

		for (Terms.Bucket entry : genders.getBuckets()) {
		    System.out.println(entry.getKey());      
		    System.out.println(entry.getDocCount()); 
		}
	}
	
	private List<Long> getUserCids(long uid) {
		List<Long> list = new ArrayList<>();
		Connection conn = DruidUtils.getInstance().getConnection();
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement("select cid from circle.members where uid=" + uid);
			ResultSet resultSet = pstmt.executeQuery();
			while(resultSet.next()){
				list.add(resultSet.getLong("cid"));
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	private List<String> convertIrrWord(String word){
		word = word.trim();
		List<String> strList = new ArrayList<>();
		if(word.contains(" ")){
			String[] arrs = word.split(" ");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}else if(word.contains("、")){
			String[] arrs = word.split("、");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}else if(word.contains(",")){
			String[] arrs = word.split(",");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}else if(word.contains("，")){
			String[] arrs = word.split("，");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}else if(word.contains("。")){
			String[] arrs = word.split("。");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}else if(word.contains(".")){
			String[] arrs = word.split(".");
			for(String s : arrs){
				strList.add(s.trim());
			}
		}
		return strList;
	}
	
	private String convertArea(String area){
		if(StringUtils.isNotEmpty(area)){
			if(area.contains(" ")){
				String[] str = area.split(" ");
				if(str.length > 0){
					return area.split(" ")[0];
				}
			}else if(area.contains(",")){
				String[] str = area.split(",");
				if(str.length > 0){
					return area.split(",")[0];
				}
			}else if(area.contains("，")){
				String[] str = area.split("，");
				if(str.length > 0){
					return area.split("，")[0];
				}
			}else if(area.contains("、")){
				String[] str = area.split("、");
				if(str.length > 0){
					return area.split("、")[0];
				}
			}
			
		}
		return area;
	}
	
	private List<String> getWorkTrack(long uid) {
		Connection conn = DruidUtils.getInstance().getConnection();
		PreparedStatement pstmt;
		List<String> list = new ArrayList<>();
		try {
			pstmt = conn.prepareStatement("select * from user.demai_extension where type = 1 and uid=" + uid);
			ResultSet resultSet = pstmt.executeQuery();
			
			StringBuilder worktrackStr = new StringBuilder("");
			while(resultSet.next()){
				String temp = resultSet.getString("agency");
				if(temp.split("\\|").length > 1){
					list.add(temp.split("\\|")[0]);
				}else{
					list.add(temp);
				}
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	private String getEducation(long uid) {
		Connection conn = DruidUtils.getInstance().getConnection();
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement("select * from user.demai_extension where type = 2 and uid=" + uid);
			ResultSet resultSet = pstmt.executeQuery();
			StringBuilder educationStr = new StringBuilder("");
			while(resultSet.next()){
				String temp = resultSet.getString("agency");
				if(temp.split("\\|").length > 1){
					educationStr.append(temp.split("\\|")[0]);
				}else{
					educationStr.append(temp);
				}
				educationStr.append("|||");
			}
			conn.close();
			String education = educationStr.toString();
			if(education.endsWith("|||")){
				education = education.substring(0, education.length() -3);
			}
			return education;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private Map<String, List<String>> getSuperJson(long uid) {
		Connection conn = DruidUtils.getInstance().getConnection();
		Map<String, List<String>> map = new HashMap<>();
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement("select * from user.demai_extension where type = 6 and uid=" + uid);
			ResultSet resultSet = pstmt.executeQuery();
			
			while(resultSet.next()){
				Map<String, JSONArray> object = (Map<String, JSONArray>) JSON.parse(resultSet.getString("agency"));
				if(object.get("traveltaggo") != null){//
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("traveltaggo");
					for(int i = 0; i < array.size(); i++){
						//list.add(array.getString(i));
						list.addAll(DemaiUtils.convertIrrWord(array.getString(i)));
					}
					map.put("traveltaggo", list);
				}else if(object.get("traveltagwantgo") != null){
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("traveltagwantgo");
					for(int i = 0; i < array.size(); i++){
						list.add(array.getString(i));
					}
					map.put("traveltagwantgo", list);
				}else if(object.get("booktagread") != null){//
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("booktagread");
					for(int i = 0; i < array.size(); i++){
						list.addAll(DemaiUtils.convertIrrWord(array.getString(i)));
					}
					map.put("booktagread", list);
				}else if(object.get("booktagreading") != null){
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("booktagreading");
					for(int i = 0; i < array.size(); i++){
						list.add(array.getString(i));
					}
					map.put("booktagreading", list);
				}else if(object.get("booktagunread") != null){
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("booktagunread");
					for(int i = 0; i < array.size(); i++){
						list.add(array.getString(i));
					}
					map.put("booktagunread", list);
				}else if(object.get("videotagwantsee") != null){//
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("videotagwantsee");
					for(int i = 0; i < array.size(); i++){
						list.addAll(DemaiUtils.convertIrrWord(array.getString(i)));
					}
					map.put("videotagwantsee", list);
				}else if(object.get("videotagsee") != null){
					List<String> list = new ArrayList<>();
					JSONArray array = object.get("videotagsee");
					for(int i = 0; i < array.size(); i++){
						list.add(array.getString(i));
					}
					map.put("videotagsee", list);
				}else if(object.get("musicfavorite") != null){
					List<String> list = new ArrayList<>();
					Map<String, JSONArray> subObject = (Map<String, JSONArray>) object.get("musicfavorite");
					if(subObject.get("musicfavorite") != null){
						for(int i = 0; i < subObject.get("musicfavorite").size(); i++){
							list.add(subObject.get("musicfavorite").getString(i));
						}
					}
					map.put("musicfavorite", list);
				}
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	private List<Long> getFollow(long uid, int type1, int type2) {
		Connection conn = DruidUtils.getInstance().getConnection();
		StringBuilder builderStr = new StringBuilder("select target from relation.follows where ");
		if(type2 == 0 ){
			builderStr.append("uid =").append(uid).append(" AND type=").append(type1);
		}else{
			builderStr.append("uid =").append(uid).append(" AND (type=").append(type1).append(" OR type=").append(type2).append(")");
		}
		List<Long> list = new ArrayList<>();
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(builderStr.toString());
			ResultSet resultSet = pstmt.executeQuery();
			
			while(resultSet.next()){
				list.add(resultSet.getLong("target"));
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return list;
	}
}
