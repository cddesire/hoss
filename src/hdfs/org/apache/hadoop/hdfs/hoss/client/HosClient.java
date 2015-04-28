package org.apache.hadoop.hdfs.hoss.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;


public class HosClient {
	
	private static final Log LOG = LogFactory.getLog(HosClient.class);
	
	private static ClientProtocol client = null;
	
	static {
		try {
			Configuration conf = new Configuration();
			client = (ClientProtocol)RPC.waitForProxy(ClientProtocol.class,
			        ClientProtocol.versionID,NameNode.getAddress(conf), conf);
		} catch (IOException e) {
			LOG.error("hos meta data client proxy creat error");
		}
	}
	public static ClientProtocol client() {
		return client;
	}
	
	public static void closeRPC() {
		RPC.stopProxy(client);
	}


}
