package com.dwarf.utils;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

public class ESAction {

	public static void main(String[] args) {
		Node node = nodeBuilder().clusterName("clusterES").node();
		Client client = node.client();
		try {
			Thread.sleep(14400);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		node.close();
	}

}
