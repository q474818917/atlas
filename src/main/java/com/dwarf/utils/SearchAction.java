package com.dwarf.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * http://120.25.163.237:9200/twitter/tweet/_search
 * @author jiyu
 *
 */
public class SearchAction {

	public static void main(String[] args) {
		SearchResponse response = ESAction.getInstance().client.prepareSearch("twitter")
        .setTypes("tweet")
        //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
        .setQuery(QueryBuilders.matchQuery("name", "振兴"))                
        .setPostFilter(QueryBuilders.rangeQuery("age").from(25).to(27))    
        .setFrom(0).setSize(60).setExplain(true)
        .execute()
        .actionGet();
		
		SearchHits searchHits = response.getHits();
		for(SearchHit hit : searchHits.getHits()){
			System.out.println(hit.getSourceAsString());
		}
	}

}
