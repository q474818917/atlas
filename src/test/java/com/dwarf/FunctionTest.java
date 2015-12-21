package com.dwarf;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dwarf.core.ElasticsearchTemplate;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "file:src/main/resources/applicationContext*.xml" })
public class FunctionTest {
	
	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;
	
	@Test
	public void test(){
	}
	
	
}
